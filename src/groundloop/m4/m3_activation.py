"""Fail-closed activation of one published M3 run as the first M4 base.

The M3 and M4 implementations historically had separate executable demos:
M3 atomically published real static artifacts, while M4 tests bootstrapped a
synthetic state.  This module is the deliberately narrow bridge between those
two already-frozen contracts.

Activation is supported only for a dedicated schema containing exactly one
published M3 run and no earlier M4 history.  Neural products remain empirical;
the bridge only validates and composes their immutable records before the
existing exact M4 publication bootstrap.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlsplit

from psycopg import Connection, sql

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction.artifacts import (
    claim_extraction_prompt_artifact,
)
from groundloop.ai.contracts import (
    ChunkDraft,
    ModelArtifact,
    ModelTask,
    PipelineRunManifest,
    PipelineRunStatus,
    PromptArtifact,
    QueryKind,
    RetrievalCandidate,
    VerificationResult,
    stable_digest,
)
from groundloop.ai.embeddings.common import EMBEDDING_DIMENSION, input_sha256
from groundloop.ai.generation.artifacts import (
    generation_prompt_artifact,
    qwen_model_artifact,
)
from groundloop.ai.manifest import canonical_manifest_json, manifest_from_dict
from groundloop.ai.persistence import lock_m3_m4_lifecycle
from groundloop.ai.pipeline import claim_version_id
from groundloop.ai.retrieval.retriever import RETRIEVAL_METHOD_VERSION
from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    DecisionPolicy,
)
from groundloop.errors import (
    ArtifactConflictError,
    EventConflictError,
    InvalidEventError,
    ValidationError,
)
from groundloop.m4.admission.fresh import PostgresExactFreshFrontierRetriever
from groundloop.m4.admission.lexical import (
    LexicalRegistrySnapshot,
    LexicalV1Config,
    LexicalV1Policy,
    load_frozen_lexical_v1,
)
from groundloop.m4.admission.manifest import build_candidate_policy_manifest
from groundloop.m4.admission.postgres_common import PostgresAdmissionServerIdentity
from groundloop.m4.admission.postgres_lexical import (
    PostgresLexicalSearchBackend,
    PostgresSimpleLexemeAnalyzer,
)
from groundloop.m4.admission.postgres_vector import (
    ExactPgvectorConfig,
    PostgresExactReverseVectorIndex,
)
from groundloop.m4.admission.service import PostgresHybridAdmissionPort
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    M4Application,
)
from groundloop.m4.artifacts import PostgresM4ArtifactRegistry
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    VectorIndexKind,
    sha256_text,
    stable_m4_digest,
)
from groundloop.m4.execution import M4ExecutionIdentity
from groundloop.m4.models.contracts import (
    CHUNK_ROLE_TEMPLATE,
    CLAIM_ROLE_TEMPLATE,
    EmbeddingRole,
    RoleEmbeddingProvenance,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput, M4BgeRoleAdapter
from groundloop.m4.models.ports import (
    AdmissionEmbeddingArtifacts,
    M4AdmissionEmbeddingService,
    M4VerificationApplicationPort,
)
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m4.pipeline import (
    M4ExecutionMode,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    PostgresM4VerificationExecutionWriter,
    PostgresPairInputResolver,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.m4.runtime import RuntimeEpochState
from groundloop.postgres import MismatchCounts, read_mismatch_counts, read_oracle_states

ACTIVATION_SCHEMA_VERSION = "groundloop-m3-m4-activation-v1"
EXACT_FRESH_FRONTIER_RETRIEVER_ID = "m4-postgres-exact-fresh-frontier-v1"

# Every relation read or written by this adapter must resolve in the selected
# current schema.  This prevents a missing local relation from falling through
# to a later search_path schema.
_REQUIRED_RELATIONS = (
    "groundloop_pipeline_run",
    "groundloop_epoch",
    "groundloop_document",
    "groundloop_document_version",
    "groundloop_chunk_version",
    "groundloop_chunk_provenance",
    "groundloop_chunk_embedding",
    "groundloop_question",
    "groundloop_answer_version",
    "groundloop_claim",
    "groundloop_answer_citation",
    "groundloop_model_artifact",
    "groundloop_prompt_artifact",
    "groundloop_chunker_artifact",
    "groundloop_generation_execution",
    "groundloop_claim_extraction_execution",
    "groundloop_retrieval_candidate",
    "groundloop_verification_execution",
    "groundloop_decision_policy",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_claim_certificate",
    "groundloop_pipeline_artifact_use",
    "groundloop_component_timing",
    "groundloop_status_delta",
    "groundloop_candidate_policy",
    "groundloop_m4_update",
    "groundloop_m4_publication_head",
    "groundloop_published_observation_currency",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_m4_claim_registry_snapshot",
    "groundloop_m4_claim_registry_member",
    "groundloop_m4_claim_admission_index",
    "groundloop_m4_role_embedding_artifact",
)

# Before first activation all M4 runtime/history relations must be empty.  On
# replay only the small activation-owned subset below may contain rows.
_M4_RELATIONS = (
    "groundloop_candidate_policy",
    "groundloop_claim_embedding",
    "groundloop_m4_update",
    "groundloop_semantic_job",
    "groundloop_semantic_job_dependency",
    "groundloop_semantic_job_attempt",
    "groundloop_discovery_scope",
    "groundloop_impact_channel_hit",
    "groundloop_admitted_pair",
    "groundloop_candidate_frontier",
    "groundloop_pair_judgment",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_object_evaluation",
    "groundloop_working_transition",
    "groundloop_impact_evaluation_run",
    "groundloop_m4_publication_head",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
    "groundloop_m4_working_claim_state",
    "groundloop_m4_working_answer_state",
    "groundloop_m4_claim_admission_index",
    "groundloop_m4_verification_execution",
    "groundloop_m4_role_embedding_artifact",
    "groundloop_m4_structural_deactivation",
    "groundloop_m4_document_metadata_overlay",
    "groundloop_m4_claim_registry_member",
    "groundloop_m4_discovery_result",
    "groundloop_m4_claim_registry_snapshot",
    "groundloop_m4_execution_accounting",
    "groundloop_m4_evaluation_default",
    "groundloop_m4_event_audit_run",
    "groundloop_m4_evaluation_epoch_counter",
    "groundloop_m4_evaluation_override_counter",
    "groundloop_m4_evaluation_counter_transition",
)

_ACTIVATION_RELATIONS = frozenset(
    {
        "groundloop_candidate_policy",
        "groundloop_m4_publication_head",
        "groundloop_published_observation_currency",
        "groundloop_published_claim_state",
        "groundloop_published_answer_state",
        "groundloop_m4_claim_registry_snapshot",
        "groundloop_m4_claim_registry_member",
        "groundloop_m4_claim_admission_index",
        "groundloop_m4_role_embedding_artifact",
    }
)


@dataclass(frozen=True, slots=True)
class _M3DocumentVersion:
    document_id: str
    document_version_id: str
    content_hash: str
    source_uri: str


def _m3_source_uri_sort_key(source_uri: str) -> str:
    """Recover the Unicode path ordering used by M3 for ordinary file inputs."""
    parsed = urlsplit(source_uri)
    if (
        parsed.scheme != "file"
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or not parsed.path
    ):
        raise ArtifactConflictError("M3 document source URI is not canonical")
    try:
        decoded_path = unquote(parsed.path, encoding="utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ArtifactConflictError("M3 document source URI is not UTF-8") from error
    return unicodedata.normalize("NFC", decoded_path)


@dataclass(frozen=True, slots=True)
class PublishedM3ActivationSource:
    """Complete, validated M3 state that may become the first M4 base."""

    schema_name: str
    manifest: PipelineRunManifest
    epoch_id: int
    answer_version_id: str
    claims: tuple[ClaimEmbeddingInput, ...]
    chunks: tuple[ChunkDraft, ...]
    observation_ids: tuple[str, ...]
    decision_policy: DecisionPolicy

    @property
    def run_id(self) -> str:
        return self.manifest.run_id

    @property
    def claim_ids(self) -> tuple[str, ...]:
        return tuple(item.claim_id for item in self.claims)

    @property
    def chunk_ids(self) -> tuple[str, ...]:
        return tuple(item.chunk_version_id for item in self.chunks)

    @property
    def registry_snapshot_id(self) -> str:
        digest = stable_m4_digest(
            "m4-m3-run-claim-registry-v1",
            self.run_id,
            self.answer_version_id,
            *self.claim_ids,
        )
        return f"m4-m3-registry-{digest}"


@dataclass(frozen=True, slots=True)
class M3M4ActivationReceipt:
    """Machine-readable result of fresh activation or exact replay."""

    schema_version: str
    run_id: str
    base_epoch_id: int
    answer_version_id: str
    claim_registry_snapshot_id: str
    candidate_policy_id: str
    candidate_policy_hash: str
    claim_count: int
    chunk_count: int
    observation_count: int
    role_artifact_count: int
    claim_index_count: int
    claim_ids: tuple[str, ...]
    chunk_ids: tuple[str, ...]
    embedding_model_artifact_id: str
    verifier_model_artifact_id: str
    verifier_prompt_artifact_id: str
    calibration_version: str
    calibration_temperature: float
    decision_policy_version: str
    verifier_execution_spec_hash: str
    vector_method_version: str
    vector_index_kind: str
    vector_index_build_config_hash: str
    vector_search_config_hash: str
    lexical_method_version: str
    lexical_config_hash: str
    created_role_artifact_count: int
    reused_role_artifact_count: int
    activation_embedding_request_count: int
    baseline_python_mismatch_count: int
    baseline_sql_claim_mismatch_count: int
    baseline_sql_answer_mismatch_count: int
    global_closure_valid: bool
    replayed: bool

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], asdict(self))

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class M3M4ActivationInspection:
    """Read-only inspection result, including a validation-only replay."""

    source: PublishedM3ActivationSource
    replay_receipt: M3M4ActivationReceipt | None


@dataclass(slots=True)
class PersistingM4AdmissionEmbeddingService:
    """Persist every role artifact returned by the production embedding port."""

    delegate: M4AdmissionEmbeddingService
    registry: PostgresM4ArtifactRegistry
    inserted_role_artifacts: int = 0
    reused_role_artifacts: int = 0

    @property
    def request_count(self) -> int:
        return self.delegate.request_count

    def embed_for_admission(
        self,
        *,
        claims: Sequence[ClaimEmbeddingInput] = (),
        chunks: Sequence[ChunkDraft] = (),
    ) -> AdmissionEmbeddingArtifacts:
        result = self.delegate.embed_for_admission(claims=claims, chunks=chunks)
        for artifact in result.claims:
            if self.registry.register_role_embedding(artifact):
                self.inserted_role_artifacts += 1
            else:
                self.reused_role_artifacts += 1
        for chunk_artifact in result.chunks:
            if self.registry.register_role_embedding(chunk_artifact):
                self.inserted_role_artifacts += 1
            else:
                self.reused_role_artifacts += 1
        return result


@dataclass(frozen=True, slots=True)
class M3M4RuntimeComposition:
    """Production M4 objects bound to one historically validated M3 base."""

    activation_receipt: M3M4ActivationReceipt
    source: PublishedM3ActivationSource
    application: M4Application
    ports: PostgresM4ApplicationPorts
    embeddings: PersistingM4AdmissionEmbeddingService
    verifier: M4VerificationApplicationPort
    identity: M4ExecutionIdentity
    candidate_policy: CandidatePolicyManifest


@dataclass(frozen=True, slots=True)
class _RuntimeActivationAncestry:
    source: PublishedM3ActivationSource
    receipt: M3M4ActivationReceipt


def _require_autocommit(connection: Connection[Any]) -> None:
    if not connection.autocommit:
        raise ValidationError(
            "M3-to-M4 activation requires an autocommit connection; "
            "atomic writes use explicit transaction blocks"
        )


def _text(value: object) -> str:
    return str(value).strip()


def _current_schema(connection: Connection[Any]) -> str:
    row = connection.execute("SELECT current_schema()").fetchone()
    if row is None or row[0] is None or not str(row[0]).strip():
        raise ValidationError("M3-to-M4 activation requires one current schema")
    return str(row[0])


def _existing_relations(
    connection: Connection[Any], schema_name: str
) -> frozenset[str]:
    rows = connection.execute(
        """
        SELECT class.relname
        FROM pg_class AS class
        JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = %s
          AND class.relkind IN ('r', 'p')
        """,
        (schema_name,),
    ).fetchall()
    return frozenset(str(row[0]) for row in rows)


def _require_local_relations(
    connection: Connection[Any], schema_name: str
) -> frozenset[str]:
    existing = _existing_relations(connection, schema_name)
    missing = sorted((set(_REQUIRED_RELATIONS) | set(_M4_RELATIONS)) - existing)
    if missing:
        raise ValidationError(
            "selected schema lacks required M3/M4 relations: " + ", ".join(missing)
        )
    return existing


def _count_table(connection: Connection[Any], schema_name: str, relation: str) -> int:
    row = connection.execute(
        sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(schema_name), sql.Identifier(relation)
        )
    ).fetchone()
    assert row is not None
    return int(row[0])


def _m4_counts(
    connection: Connection[Any], schema_name: str, existing: frozenset[str]
) -> dict[str, int]:
    return {
        relation: _count_table(connection, schema_name, relation)
        for relation in _M4_RELATIONS
        if relation in existing
    }


def _require_empty_m4(
    connection: Connection[Any], schema_name: str, existing: frozenset[str]
) -> None:
    nonempty = {
        relation: count
        for relation, count in _m4_counts(connection, schema_name, existing).items()
        if count
    }
    if nonempty:
        rendered = ", ".join(
            f"{relation}={count}" for relation, count in sorted(nonempty.items())
        )
        raise EventConflictError(
            "first M3-to-M4 activation requires empty M4 state: " + rendered
        )


def _require_only_activation_m4(
    connection: Connection[Any], schema_name: str, existing: frozenset[str]
) -> None:
    unexpected = {
        relation: count
        for relation, count in _m4_counts(connection, schema_name, existing).items()
        if count and relation not in _ACTIVATION_RELATIONS
    }
    if unexpected:
        rendered = ", ".join(
            f"{relation}={count}" for relation, count in sorted(unexpected.items())
        )
        raise EventConflictError(
            "M4 activation replay found dynamic or partial history: " + rendered
        )


def _manifest(value: object) -> tuple[PipelineRunManifest, dict[str, object]]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError("stored M3 manifest must be a JSON object")
    raw = cast(dict[str, object], value)
    try:
        parsed = manifest_from_dict(cast(dict[str, Any], raw))
    except (KeyError, TypeError, ValueError) as error:
        raise ValidationError("stored M3 manifest is malformed") from error
    if json.loads(canonical_manifest_json(parsed)) != raw:
        raise ValidationError("stored M3 manifest is not canonical")
    return parsed, raw


def _m3_observation_id(claim_id: str, result: VerificationResult) -> str:
    return "observation-" + stable_digest(
        "m3-observation-v1",
        claim_id,
        result.chunk_version_id,
        "direct_verification",
        result.model_artifact_id,
        result.prompt_artifact_id,
        result.calibration_version,
        result.input_hash,
    )


def _load_run_manifest(
    connection: Connection[Any], run_id: str, *, lock_run: bool
) -> tuple[PipelineRunManifest, int, str]:
    if not run_id.strip():
        raise ValidationError("M3 run_id must be non-empty")
    counts = connection.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE status = 'published')
        FROM groundloop_pipeline_run
        """
    ).fetchone()
    if counts is None or (int(counts[0]), int(counts[1])) != (1, 1):
        raise InvalidEventError(
            "activation requires exactly one pipeline row and one published M3 run"
        )
    statement = """
        SELECT schema_version, status, config_hash, input_hash, corpus_hash,
               question_id, answer_version_id, semantic_epoch_id, manifest,
               failure_code, completed_at IS NOT NULL
        FROM groundloop_pipeline_run
        WHERE run_id = %s
    """
    if lock_run:
        statement += " FOR UPDATE"
    row = connection.execute(
        statement,
        (run_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError(f"unknown published M3 run: {run_id}")
    parsed, _raw = _manifest(row[8])
    if str(row[0]) != "m3-v1" or str(row[1]) != PipelineRunStatus.PUBLISHED.value:
        raise InvalidEventError("selected M3 run is not a published m3-v1 run")
    if row[6] is None or row[7] is None or row[9] is not None or not bool(row[10]):
        raise InvalidEventError("published M3 run has incomplete terminal columns")
    answer_id = str(row[6])
    epoch_id = int(row[7])
    relational = (
        str(row[0]),
        run_id,
        str(row[1]),
        _text(row[2]),
        _text(row[3]),
        _text(row[4]),
        str(row[5]),
        answer_id,
        epoch_id,
    )
    manifested = (
        parsed.schema_version,
        parsed.run_id,
        parsed.status.value,
        parsed.config_hash,
        parsed.input_hash,
        parsed.corpus_hash,
        parsed.question_id,
        parsed.answer_version_id,
        parsed.semantic_epoch_id,
    )
    if relational != manifested:
        raise ArtifactConflictError("M3 run columns differ from its manifest")
    if parsed.confirmed_as_of_epoch != epoch_id:
        raise ArtifactConflictError("M3 manifest confirmation epoch drift")
    return parsed, epoch_id, answer_id


def _validate_epoch(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    epoch_id: int,
    *,
    pristine: bool,
) -> None:
    statement = """
        SELECT epoch_id, event_id, payload_hash, revision, structural_status,
               semantic_status, evaluation_state, publication_mode,
               sealed_at IS NOT NULL
        FROM groundloop_epoch
    """
    parameters: tuple[object, ...] = ()
    if pristine:
        statement += " ORDER BY epoch_id"
    else:
        statement += " WHERE epoch_id = %s"
        parameters = (epoch_id,)
    rows = connection.execute(statement, parameters).fetchall()
    expected = (
        epoch_id,
        f"m3-run:{manifest.run_id}",
        manifest.input_hash,
        0,
        "committed",
        "sealed",
        "complete",
        "provisional",
        True,
    )
    normalized = tuple(
        (
            int(row[0]),
            str(row[1]),
            _text(row[2]),
            int(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
            str(row[7]),
            bool(row[8]),
        )
        for row in rows
    )
    if normalized != (expected,):
        raise ArtifactConflictError("M3 activation source epoch identity drift")


def _load_policy(
    connection: Connection[Any], manifest: PipelineRunManifest, epoch_id: int
) -> DecisionPolicy:
    rows = connection.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version, valid_from_epoch, valid_to_epoch
        FROM groundloop_decision_policy
        ORDER BY policy_version
        """
    ).fetchall()
    if len(rows) != 1:
        raise ArtifactConflictError("M3 activation requires one decision policy")
    row = rows[0]
    if (
        str(row[0]) != manifest.decision_policy_version
        or int(row[4]) != epoch_id
        or row[5] is not None
    ):
        raise ArtifactConflictError("M3 decision-policy interval drift")
    return DecisionPolicy(
        policy_version=str(row[0]),
        support_threshold=float(row[1]),
        refute_threshold=float(row[2]),
        tie_rule_version=str(row[3]),
    )


def _validate_models_and_prompts(
    connection: Connection[Any], manifest: PipelineRunManifest
) -> tuple[dict[str, ModelArtifact], dict[str, PromptArtifact]]:
    model_rows = connection.execute(
        """
        SELECT model_artifact_id, task, provider, model_id,
               immutable_revision, tokenizer_revision, license_id,
               config_hash, artifact_sha256, metadata
        FROM groundloop_model_artifact
        ORDER BY model_artifact_id
        """
    ).fetchall()
    model_ids = tuple(str(row[0]) for row in model_rows)
    if model_ids != tuple(sorted(manifest.model_artifact_ids)):
        raise ArtifactConflictError("M3 model registry differs from manifest")
    if len(set(model_ids)) != len(model_ids):
        raise ArtifactConflictError("M3 model registry contains duplicate IDs")
    if any(row[9] != {} for row in model_rows):
        raise ArtifactConflictError("M3 model registry contains unbound metadata")
    try:
        models = {
            str(row[0]): ModelArtifact(
                artifact_id=str(row[0]),
                task=ModelTask(str(row[1])),
                provider=str(row[2]),
                model_id=str(row[3]),
                immutable_revision=str(row[4]),
                tokenizer_revision=str(row[5]),
                license_id=str(row[6]),
                config_hash=_text(row[7]),
                artifact_sha256=None if row[8] is None else _text(row[8]),
            )
            for row in model_rows
        }
    except (TypeError, ValueError, ValidationError) as error:
        raise ArtifactConflictError("M3 model registry payload is invalid") from error

    prompt_rows = connection.execute(
        """
        SELECT prompt_artifact_id, task, version, template, template_hash,
               decoding_config_hash
        FROM groundloop_prompt_artifact
        ORDER BY prompt_artifact_id
        """
    ).fetchall()
    prompt_ids = tuple(str(row[0]) for row in prompt_rows)
    if prompt_ids != tuple(sorted(manifest.prompt_artifact_ids)):
        raise ArtifactConflictError("M3 prompt registry differs from manifest")
    try:
        prompts = {
            str(row[0]): PromptArtifact(
                artifact_id=str(row[0]),
                task=ModelTask(str(row[1])),
                version=str(row[2]),
                template=str(row[3]),
                template_hash=_text(row[4]),
                decoding_config_hash=_text(row[5]),
            )
            for row in prompt_rows
        }
    except (TypeError, ValueError, ValidationError) as error:
        raise ArtifactConflictError("M3 prompt registry payload is invalid") from error
    for expected_model in (
        qwen_model_artifact(ModelTask.GENERATION),
        qwen_model_artifact(ModelTask.CLAIM_EXTRACTION),
    ):
        if models.get(expected_model.artifact_id) != expected_model:
            raise ArtifactConflictError("M3 frozen static model artifact drift")
    for expected_prompt in (
        generation_prompt_artifact(),
        claim_extraction_prompt_artifact(),
    ):
        if prompts.get(expected_prompt.artifact_id) != expected_prompt:
            raise ArtifactConflictError("M3 frozen static prompt artifact drift")
    return models, prompts


def _prompt_version_stamp(prompt: PromptArtifact) -> str:
    return f"{prompt.version}:{prompt.template_hash}"


def _validate_answer_and_claims(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    epoch_id: int,
    answer_id: str,
    models: Mapping[str, ModelArtifact],
    prompts: Mapping[str, PromptArtifact],
) -> tuple[ClaimEmbeddingInput, ...]:
    if manifest.answer is None or manifest.extraction is None:
        raise ArtifactConflictError("published M3 manifest lacks answer provenance")

    question_rows = connection.execute(
        "SELECT question_id, text, created_epoch FROM groundloop_question"
    ).fetchall()
    normalized_questions = tuple(
        (str(row[0]), str(row[1]), int(row[2])) for row in question_rows
    )
    if len(normalized_questions) != 1:
        raise ArtifactConflictError("M3 question universe differs from selected run")
    question_id, question_text, question_epoch = normalized_questions[0]
    normalized_question_text = " ".join(question_text.split())
    if (
        question_id != manifest.question_id
        or question_epoch != epoch_id
        or question_text != normalized_question_text
        or question_id
        != "question-" + stable_digest("m3-question-v1", normalized_question_text)
    ):
        raise ArtifactConflictError("M3 question identity/provenance drift")

    answer_rows = connection.execute(
        """
        SELECT answer.answer_version_id, answer.question_id, answer.text,
               answer.generator_model_id, answer.generator_model_version,
               answer.prompt_version, answer.created_epoch,
               execution.run_id, execution.model_artifact_id,
               execution.prompt_artifact_id,
               execution.decoding_config_hash, execution.input_hash,
               execution.raw_output_hash, execution.repair_count
        FROM groundloop_answer_version AS answer
        JOIN groundloop_generation_execution AS execution
          USING (answer_version_id)
        """
    ).fetchall()
    if len(answer_rows) != 1:
        raise ArtifactConflictError("M3 activation requires one generated answer")
    answer = answer_rows[0]
    generation_model_id = str(answer[8])
    generation_prompt_id = str(answer[9])
    model = models.get(generation_model_id)
    prompt = prompts.get(generation_prompt_id)
    if model is None or model.task is not ModelTask.GENERATION:
        raise ArtifactConflictError("M3 generation model provenance drift")
    if prompt is None or prompt.task is not ModelTask.GENERATION:
        raise ArtifactConflictError("M3 generation prompt provenance drift")
    expected_answer = (
        answer_id,
        manifest.question_id,
        manifest.answer.text,
        model.model_id,
        model.immutable_revision,
        _prompt_version_stamp(prompt),
        epoch_id,
        manifest.run_id,
        model.artifact_id,
        prompt.artifact_id,
        prompt.decoding_config_hash,
        manifest.answer.input_hash,
        manifest.answer.raw_output_hash,
        manifest.answer.repair_count,
    )
    actual_answer = (
        str(answer[0]),
        str(answer[1]),
        str(answer[2]),
        str(answer[3]),
        str(answer[4]),
        str(answer[5]),
        int(answer[6]),
        str(answer[7]),
        str(answer[8]),
        str(answer[9]),
        _text(answer[10]),
        _text(answer[11]),
        _text(answer[12]),
        int(answer[13]),
    )
    if actual_answer != expected_answer:
        raise ArtifactConflictError("M3 answer/execution provenance drift")

    citations = tuple(
        (int(row[0]), str(row[1]))
        for row in connection.execute(
            """
            SELECT citation_ordinal, chunk_version_id
            FROM groundloop_answer_citation
            WHERE answer_version_id = %s
            ORDER BY citation_ordinal
            """,
            (answer_id,),
        ).fetchall()
    )
    expected_citations = tuple(
        (ordinal, chunk_id)
        for ordinal, chunk_id in enumerate(
            manifest.answer.cited_chunk_version_ids, start=1
        )
    )
    if citations != expected_citations:
        raise ArtifactConflictError("M3 answer citation closure drift")

    local_to_global = {
        claim.local_claim_id: claim_version_id(answer_id, claim)
        for claim in manifest.claims
    }
    rows = connection.execute(
        """
        SELECT claim.claim_id, claim.answer_version_id, claim.text,
               claim.required, claim.extractor_model_id,
               claim.extractor_model_version, claim.extractor_prompt_version,
               execution.run_id, execution.model_artifact_id,
               execution.prompt_artifact_id, execution.input_hash,
               execution.raw_output_hash, execution.repair_count
        FROM groundloop_claim AS claim
        JOIN groundloop_claim_extraction_execution AS execution USING (claim_id)
        ORDER BY claim.claim_id
        """
    ).fetchall()
    by_global = {local_to_global[item.local_claim_id]: item for item in manifest.claims}
    all_claim_ids = tuple(
        str(row[0])
        for row in connection.execute(
            "SELECT claim_id FROM groundloop_claim ORDER BY claim_id"
        ).fetchall()
    )
    if tuple(str(row[0]) for row in rows) != tuple(
        sorted(by_global)
    ) or all_claim_ids != tuple(sorted(by_global)):
        raise ArtifactConflictError("M3 claim universe differs from manifest")
    claims: list[ClaimEmbeddingInput] = []
    for row in rows:
        global_id = str(row[0])
        atomic = by_global[global_id]
        extraction_model_id = str(row[8])
        extraction_prompt_id = str(row[9])
        extraction_model = models.get(extraction_model_id)
        extraction_prompt = prompts.get(extraction_prompt_id)
        if (
            extraction_model is None
            or extraction_model.task is not ModelTask.CLAIM_EXTRACTION
        ):
            raise ArtifactConflictError("M3 extraction model provenance drift")
        if (
            extraction_prompt is None
            or extraction_prompt.task is not ModelTask.CLAIM_EXTRACTION
        ):
            raise ArtifactConflictError("M3 extraction prompt provenance drift")
        expected = (
            global_id,
            answer_id,
            atomic.text,
            atomic.required,
            extraction_model.model_id,
            extraction_model.immutable_revision,
            _prompt_version_stamp(extraction_prompt),
            manifest.run_id,
            extraction_model.artifact_id,
            extraction_prompt.artifact_id,
            manifest.extraction.input_hash,
            manifest.extraction.raw_output_hash,
            manifest.extraction.repair_count,
        )
        actual = (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            bool(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
            str(row[7]),
            str(row[8]),
            str(row[9]),
            _text(row[10]),
            _text(row[11]),
            int(row[12]),
        )
        if actual != expected:
            raise ArtifactConflictError("M3 claim/extraction provenance drift")
        claims.append(ClaimEmbeddingInput(global_id, atomic.text))
    return tuple(claims)


def _candidate_tuple(candidate: RetrievalCandidate) -> tuple[object, ...]:
    claim_id = candidate.query_id if candidate.query_kind is QueryKind.CLAIM else None
    return (
        candidate.candidate_id,
        candidate.query_kind.value,
        candidate.query_id,
        claim_id,
        candidate.chunk_version_id,
        candidate.embedding_model_artifact_id,
        candidate.method_version,
        candidate.score,
        candidate.rank,
    )


def _validate_candidates(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    models: Mapping[str, ModelArtifact],
) -> dict[str, RetrievalCandidate]:
    rows = connection.execute(
        """
        SELECT candidate_id, query_kind, query_id, claim_id,
               chunk_version_id, embedding_model_artifact_id,
               method_version, score, rank, run_id
        FROM groundloop_retrieval_candidate
        ORDER BY candidate_id
        """
    ).fetchall()
    actual = tuple(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            None if row[3] is None else str(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
            float(row[7]),
            int(row[8]),
        )
        for row in rows
    )
    expected = tuple(
        sorted(_candidate_tuple(item) for item in manifest.retrieval_candidates)
    )
    if actual != expected or any(str(row[9]) != manifest.run_id for row in rows):
        raise ArtifactConflictError("M3 retrieval candidate closure drift")

    candidates = manifest.retrieval_candidates
    embedding_model_ids = tuple(
        sorted(
            artifact_id
            for artifact_id, artifact in models.items()
            if artifact.task is ModelTask.EMBEDDING
        )
    )
    if len(embedding_model_ids) != 1:
        raise ArtifactConflictError(
            "M3 retrieval requires exactly one embedding model artifact"
        )
    embedding_model_id = embedding_model_ids[0]
    answer_id = manifest.answer_version_id
    if answer_id is None or manifest.answer is None:
        raise ArtifactConflictError("published M3 manifest lacks answer provenance")
    global_claim_ids = {claim_version_id(answer_id, claim) for claim in manifest.claims}
    question_candidates = tuple(
        item for item in candidates if item.query_kind is QueryKind.QUESTION
    )
    if not question_candidates:
        raise ArtifactConflictError("M3 question retrieval has no evidence")
    if any(item.query_id != manifest.question_id for item in question_candidates):
        raise ArtifactConflictError("M3 question retrieval query identity drift")
    claim_query_ids = {
        item.query_id for item in candidates if item.query_kind is QueryKind.CLAIM
    }
    if claim_query_ids != global_claim_ids:
        raise ArtifactConflictError("M3 claim retrieval query identity drift")
    if any(
        item.embedding_model_artifact_id != embedding_model_id
        or item.method_version != RETRIEVAL_METHOD_VERSION
        or item.chunk_version_id not in manifest.chunk_version_ids
        for item in candidates
    ):
        raise ArtifactConflictError("M3 retrieval method/model/chunk provenance drift")
    if len({item.candidate_id for item in candidates}) != len(candidates):
        raise ArtifactConflictError("M3 retrieval candidate identity is not unique")

    grouped: dict[tuple[QueryKind, str], list[RetrievalCandidate]] = {}
    for item in candidates:
        grouped.setdefault((item.query_kind, item.query_id), []).append(item)
    for group in grouped.values():
        ranked = sorted(group, key=lambda item: item.rank)
        if tuple(item.rank for item in ranked) != tuple(range(1, len(ranked) + 1)):
            raise ArtifactConflictError("M3 retrieval ranks are not contiguous")
        if len({item.chunk_version_id for item in ranked}) != len(ranked):
            raise ArtifactConflictError("M3 retrieval repeats evidence in one query")
        if ranked != sorted(
            ranked, key=lambda item: (-item.score, item.chunk_version_id)
        ):
            raise ArtifactConflictError("M3 retrieval rank ordering drift")

    question_evidence = {item.chunk_version_id for item in question_candidates}
    if not set(manifest.answer.cited_chunk_version_ids) <= question_evidence:
        raise ArtifactConflictError(
            "M3 answer citations escape question retrieval evidence"
        )
    return {item.candidate_id: item for item in candidates}


def _validate_verifications(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    epoch_id: int,
    answer_id: str,
    candidates: Mapping[str, RetrievalCandidate],
    models: Mapping[str, ModelArtifact],
    prompts: Mapping[str, PromptArtifact],
    *,
    validate_current_currency: bool,
) -> tuple[str, ...]:
    local_to_global = {
        claim.local_claim_id: claim_version_id(answer_id, claim)
        for claim in manifest.claims
    }
    by_candidate = {item.candidate_id: item for item in manifest.verifications}
    claim_candidate_ids = {
        item.candidate_id
        for item in manifest.retrieval_candidates
        if item.query_kind is QueryKind.CLAIM
    }
    if set(by_candidate) != claim_candidate_ids:
        raise ArtifactConflictError(
            "M3 claim candidates and verifications are not an exact bijection"
        )
    if not by_candidate:
        raise ArtifactConflictError("M3 activation requires verifier observations")
    if len(by_candidate) != len(manifest.verifications):
        raise ArtifactConflictError("M3 manifest reuses one verification candidate")
    verifier_identities = {
        (
            item.model_artifact_id,
            item.prompt_artifact_id,
            item.calibration_version,
            item.temperature,
        )
        for item in manifest.verifications
    }
    if len(verifier_identities) != 1:
        raise ArtifactConflictError("M3 verifier execution identity is ambiguous")
    rows = connection.execute(
        """
        SELECT execution.observation_id, execution.run_id,
               execution.candidate_id, execution.model_artifact_id,
               execution.prompt_artifact_id, execution.calibration_version,
               execution.temperature, execution.raw_logits,
               execution.raw_output_hash, execution.reused_from_observation_id,
               observation.subject_kind, observation.subject_id,
               observation.chunk_version_id, observation.task_type,
               observation.support_score, observation.refute_score,
               observation.neutral_score, observation.model_id,
               observation.model_version, observation.prompt_version,
               observation.input_hash, observation.produced_epoch,
               observation.raw_output_hash
        FROM groundloop_verification_execution AS execution
        JOIN groundloop_semantic_observation AS observation USING (observation_id)
        ORDER BY execution.observation_id
        """
    ).fetchall()
    if len(rows) != len(by_candidate):
        raise ArtifactConflictError("M3 verification execution closure drift")
    observation_ids: list[str] = []
    expected_currency: list[tuple[object, ...]] = []
    for row in rows:
        candidate_id = str(row[2])
        result = by_candidate.get(candidate_id)
        candidate = candidates.get(candidate_id)
        if result is None or candidate is None:
            raise ArtifactConflictError("M3 verification references unknown candidate")
        global_id = local_to_global.get(result.claim_id)
        if (
            global_id is None
            or candidate.query_kind is not QueryKind.CLAIM
            or candidate.query_id != global_id
            or candidate.chunk_version_id != result.chunk_version_id
        ):
            raise ArtifactConflictError("M3 verification claim mapping drift")
        model = models.get(result.model_artifact_id)
        prompt = prompts.get(result.prompt_artifact_id)
        if model is None or model.task is not ModelTask.VERIFICATION:
            raise ArtifactConflictError("M3 verifier model provenance drift")
        if prompt is None or prompt.task is not ModelTask.VERIFICATION:
            raise ArtifactConflictError("M3 verifier prompt provenance drift")
        observation_id = _m3_observation_id(global_id, result)
        logits = tuple(float(value) for value in (result.raw_logits or (0.0,) * 3))
        actual = (
            str(row[0]),
            str(row[1]),
            candidate_id,
            str(row[3]),
            str(row[4]),
            str(row[5]),
            float(row[6]),
            tuple(float(value) for value in row[7]),
            _text(row[8]),
            row[9],
            str(row[10]),
            str(row[11]),
            str(row[12]),
            str(row[13]),
            float(row[14]),
            float(row[15]),
            float(row[16]),
            str(row[17]),
            str(row[18]),
            str(row[19]),
            str(row[20]),
            int(row[21]),
            _text(row[22]),
        )
        expected = (
            observation_id,
            manifest.run_id,
            candidate_id,
            result.model_artifact_id,
            result.prompt_artifact_id,
            result.calibration_version,
            result.temperature,
            logits,
            result.raw_output_hash,
            None,
            "claim",
            global_id,
            result.chunk_version_id,
            "direct_verification",
            result.scores.support,
            result.scores.refute,
            result.scores.neutral,
            model.model_id,
            model.immutable_revision,
            _prompt_version_stamp(prompt),
            result.input_hash,
            epoch_id,
            result.raw_output_hash,
        )
        if actual != expected:
            raise ArtifactConflictError("M3 verification/observation provenance drift")
        observation_ids.append(observation_id)
        expected_currency.append(
            (
                "claim",
                global_id,
                result.chunk_version_id,
                "direct_verification",
                observation_id,
                0,
            )
        )

    all_observation_ids = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT observation_id
            FROM groundloop_semantic_observation
            ORDER BY observation_id
            """
        ).fetchall()
    )
    if validate_current_currency and all_observation_ids != tuple(
        sorted(observation_ids)
    ):
        raise ArtifactConflictError("M3 semantic observation closure drift")

    if validate_current_currency:
        currency = tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                int(row[5]),
            )
            for row in connection.execute(
                """
                SELECT subject_kind, subject_id, chunk_version_id, task_type,
                       observation_id, installed_revision
                FROM groundloop_observation_currency
                ORDER BY subject_kind, subject_id, chunk_version_id, task_type
                """
            ).fetchall()
        )
        if currency != tuple(sorted(expected_currency)):
            raise ArtifactConflictError("M3 current-observation currency closure drift")
    return tuple(observation_ids)


def _load_documents(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    epoch_id: int,
    *,
    pristine: bool,
) -> tuple[_M3DocumentVersion, ...]:
    rows = connection.execute(
        """
        SELECT document.document_id, version.document_version_id,
               version.content_hash, document.source_uri,
               document.authority_class, version.valid_from_epoch,
               version.valid_to_epoch
        FROM groundloop_document_version AS version
        JOIN groundloop_document AS document USING (document_id)
        WHERE version.valid_from_epoch = %s
        ORDER BY version.document_version_id
        """,
        (epoch_id,),
    ).fetchall()
    unordered_documents = tuple(
        _M3DocumentVersion(
            document_id=str(row[0]),
            document_version_id=str(row[1]),
            content_hash=_text(row[2]),
            source_uri=str(row[3]),
        )
        for row in rows
    )
    chunk_document_rows = connection.execute(
        """
        SELECT chunk_version_id, document_version_id
        FROM groundloop_chunk_version
        WHERE chunk_version_id = ANY(%s::text[])
        """,
        (list(manifest.chunk_version_ids),),
    ).fetchall()
    document_version_by_chunk = {
        str(row[0]): str(row[1]) for row in chunk_document_rows
    }
    nonempty_document_order: list[str] = []
    for chunk_id in manifest.chunk_version_ids:
        document_version_id = document_version_by_chunk.get(chunk_id)
        if (
            document_version_id is not None
            and document_version_id not in nonempty_document_order
        ):
            nonempty_document_order.append(document_version_id)
    document_by_version = {
        item.document_version_id: item for item in unordered_documents
    }
    nonempty_document_ids = set(nonempty_document_order)
    has_empty_document = nonempty_document_ids != set(document_by_version)
    if not has_empty_document and len(nonempty_document_order) == len(
        unordered_documents
    ):
        # Manifest chunks are flattened in M3 document order, so this is the
        # exact source ordering even when symlink aliases share one source URI.
        documents = tuple(
            document_by_version[document_version_id]
            for document_version_id in nonempty_document_order
        )
    else:
        # Empty documents have no chunk in the manifest.  Their ordinary M3
        # ordering can still be recovered from the decoded absolute file path;
        # URI lexical order is wrong for percent-encoded Unicode/punctuation.
        documents = tuple(
            sorted(
                unordered_documents,
                key=lambda item: _m3_source_uri_sort_key(item.source_uri),
            )
        )
    frozen_chunker = FixedCharChunker()
    corpus_hash_matches = (
        stable_digest(
            "m3-corpus-v1",
            *(
                value
                for item in documents
                for value in (
                    item.document_id,
                    item.document_version_id,
                    item.content_hash,
                )
            ),
        )
        == manifest.corpus_hash
    )
    if (
        not documents
        or any(
            str(row[4]) != "local-m3"
            or int(row[5]) != epoch_id
            or (row[6] is not None and (pristine or int(row[6]) <= epoch_id))
            for row in rows
        )
        or any(not item.source_uri.strip() for item in documents)
        or len({item.document_id for item in documents}) != len(documents)
        or len({item.document_version_id for item in documents}) != len(documents)
        or any(
            item.document_version_id
            != stable_digest(
                "document-version-v1",
                item.document_id,
                item.content_hash,
                frozen_chunker.artifact_id,
            )
            for item in documents
        )
        # M3 did not persist the relative path of an empty symlink.  If its
        # original order cannot be reconstructed from the canonical file URI,
        # activation must reject it rather than weaken the frozen corpus
        # commitment for every mixed corpus.
        or not corpus_hash_matches
    ):
        raise ArtifactConflictError("M3 corpus identity/provenance drift")
    if pristine:
        counts = connection.execute(
            """
            SELECT (SELECT count(*) FROM groundloop_document),
                   (SELECT count(*) FROM groundloop_document_version)
            """
        ).fetchone()
        if counts is None or not (int(counts[0]) == int(counts[1]) == len(documents)):
            raise ArtifactConflictError("M3 document/version closure drift")
    return documents


def _load_chunks(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    epoch_id: int,
    documents: tuple[_M3DocumentVersion, ...],
    *,
    pristine: bool,
) -> tuple[ChunkDraft, ...]:
    rows = connection.execute(
        """
        SELECT chunk.chunk_version_id, chunk.document_version_id,
               chunk.chunk_index, chunk.text, chunk.text_hash,
               provenance.chunker_artifact_id, chunk.valid_from_epoch,
               chunk.valid_to_epoch, version.valid_from_epoch,
               version.valid_to_epoch, chunk.chunker_version,
               version.document_id, version.content_hash,
               document.source_uri, document.authority_class,
               provenance.input_hash
        FROM groundloop_chunk_version AS chunk
        JOIN groundloop_chunk_provenance AS provenance USING (chunk_version_id)
        JOIN groundloop_document_version AS version USING (document_version_id)
        JOIN groundloop_document AS document USING (document_id)
        WHERE chunk.chunk_version_id = ANY(%s::text[])
        ORDER BY chunk.chunk_version_id
        """,
        (list(manifest.chunk_version_ids),),
    ).fetchall()
    expected_hashes = dict(manifest.chunk_text_hashes)
    if tuple(str(row[0]) for row in rows) != tuple(sorted(manifest.chunk_version_ids)):
        raise ArtifactConflictError("M3 active chunk universe differs from manifest")
    document_by_version = {item.document_version_id: item for item in documents}
    chunks: list[ChunkDraft] = []
    for row in rows:
        chunk_id = str(row[0])
        document = document_by_version.get(str(row[1]))
        if (
            document is None
            or document.document_id != str(row[11])
            or document.content_hash != _text(row[12])
            or document.source_uri != str(row[13])
            or _text(row[4]) != expected_hashes.get(chunk_id)
            or int(row[6]) != epoch_id
            or (row[7] is not None and (pristine or int(row[7]) <= epoch_id))
            or int(row[8]) != epoch_id
            or (row[9] is not None and (pristine or int(row[9]) <= epoch_id))
            or str(row[10]) != "fixed-char-v1"
            or str(row[14]) != "local-m3"
            or _text(row[15]) != _text(row[12])
        ):
            raise ArtifactConflictError("M3 chunk/version interval provenance drift")
        chunks.append(
            ChunkDraft(
                chunk_version_id=chunk_id,
                document_version_id=str(row[1]),
                chunk_index=int(row[2]),
                text=str(row[3]),
                text_hash=_text(row[4]),
                chunker_artifact_id=str(row[5]),
            )
        )
    if pristine:
        chunk_counts = connection.execute(
            """
            SELECT (SELECT count(*) FROM groundloop_chunk_version),
                   (SELECT count(*) FROM groundloop_chunk_provenance)
            """
        ).fetchone()
        if chunk_counts is None or not (
            int(chunk_counts[0]) == int(chunk_counts[1]) == len(chunks)
        ):
            raise ArtifactConflictError("M3 chunk/provenance closure drift")

    embedding_rows = connection.execute(
        """
        SELECT embedding.chunk_version_id, embedding.model_artifact_id,
               artifact.task, embedding.input_hash,
               abs(vector_norm(embedding.embedding) - 1.0) <= 1e-6
        FROM groundloop_chunk_embedding AS embedding
        JOIN groundloop_model_artifact AS artifact USING (model_artifact_id)
        WHERE embedding.chunk_version_id = ANY(%s::text[])
        ORDER BY embedding.chunk_version_id, embedding.model_artifact_id
        """,
        (list(manifest.chunk_version_ids),),
    ).fetchall()
    chunk_text_by_id = {item.chunk_version_id: item.text for item in chunks}
    expected_embedding_ids = {
        item.embedding_model_artifact_id for item in manifest.retrieval_candidates
    }
    if (
        len(embedding_rows) != len(chunks)
        or tuple(str(row[0]) for row in embedding_rows)
        != tuple(item.chunk_version_id for item in chunks)
        or any(str(row[2]) != "embedding" for row in embedding_rows)
        or {str(row[1]) for row in embedding_rows} != expected_embedding_ids
        or any(
            _text(row[3]) != input_sha256(chunk_text_by_id[str(row[0])])
            or not bool(row[4])
            for row in embedding_rows
        )
    ):
        raise ArtifactConflictError("M3 chunk embedding closure drift")
    return tuple(chunks)


def _validate_states(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    epoch_id: int,
    answer_id: str,
    claim_ids: tuple[str, ...],
) -> None:
    expected_claims = {item.claim_id: item for item in manifest.claim_states}
    expected_answers = {item.answer_version_id: item for item in manifest.answer_states}
    if tuple(sorted(expected_claims)) != claim_ids or tuple(expected_answers) != (
        answer_id,
    ):
        raise ArtifactConflictError("M3 manifest state universe drift")
    oracle = read_oracle_states(connection)
    if oracle.claims != expected_claims or oracle.answers != expected_answers:
        raise ValidationError("M3 source differs from independent SQL recomputation")
    if read_mismatch_counts(connection) != MismatchCounts(0, 0, 0):
        raise ValidationError("M3 materialized source fails SQL oracle checks")

    claim_rows = connection.execute(
        """
        SELECT claim_id, support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, status, updated_epoch, updated_revision
        FROM groundloop_claim_state_materialized
        ORDER BY claim_id
        """
    ).fetchall()
    actual_claims = tuple(
        ClaimState(
            claim_id=str(row[0]),
            support_count=int(row[1]),
            refute_count=int(row[2]),
            best_support_score=None if row[3] is None else float(row[3]),
            best_refute_score=None if row[4] is None else float(row[4]),
            supporting_observation_ids=tuple(str(value) for value in row[5]),
            refuting_observation_ids=tuple(str(value) for value in row[6]),
            status=ClaimStatus(str(row[7])),
        )
        for row in claim_rows
    )
    # ClaimState converts/validates enum-compatible string values at runtime.
    if {item.claim_id: item for item in actual_claims} != expected_claims or any(
        int(row[8]) != epoch_id or int(row[9]) != 0 for row in claim_rows
    ):
        raise ArtifactConflictError("M3 materialized claim state drift")

    answer_rows = connection.execute(
        """
        SELECT answer_version_id, required_claim_count, supported_count,
               unsupported_count, refuted_count, conflicted_count, status,
               updated_epoch, updated_revision
        FROM groundloop_answer_state_materialized
        ORDER BY answer_version_id
        """
    ).fetchall()
    actual_answers = tuple(
        AnswerState(
            answer_version_id=str(row[0]),
            required_claim_count=int(row[1]),
            supported_count=int(row[2]),
            unsupported_count=int(row[3]),
            refuted_count=int(row[4]),
            conflicted_count=int(row[5]),
            status=AnswerStatus(str(row[6])),
        )
        for row in answer_rows
    )
    answers_differ = {
        item.answer_version_id: item for item in actual_answers
    } != expected_answers
    if answers_differ or any(
        int(row[7]) != epoch_id or int(row[8]) != 0 for row in answer_rows
    ):
        raise ArtifactConflictError("M3 materialized answer state drift")

    certificate_rows = connection.execute(
        """
        SELECT claim_id, support_observation_id, refute_observation_id,
               repaired_epoch, repaired_revision
        FROM groundloop_claim_certificate
        ORDER BY claim_id
        """
    ).fetchall()
    if tuple(str(row[0]) for row in certificate_rows) != claim_ids or any(
        int(row[3]) != epoch_id or int(row[4]) != 0 for row in certificate_rows
    ):
        raise ArtifactConflictError("M3 claim certificate closure drift")


def _validate_audit_closure(
    connection: Connection[Any],
    manifest: PipelineRunManifest,
    documents: tuple[_M3DocumentVersion, ...],
    chunks: tuple[ChunkDraft, ...],
    claim_ids: tuple[str, ...],
    observation_ids: tuple[str, ...],
    answer_id: str,
    *,
    pristine: bool,
) -> None:
    chunker_ids = tuple(sorted({item.chunker_artifact_id for item in chunks}))
    if len(chunker_ids) != 1:
        raise ArtifactConflictError("M3 activation requires one chunker artifact")
    chunker_rows = connection.execute(
        """
        SELECT chunker_artifact_id, chunker_version,
               normalization_version, config_hash
        FROM groundloop_chunker_artifact
        """
    ).fetchall()
    frozen_chunker = FixedCharChunker()
    expected_chunker = (
        frozen_chunker.artifact_id,
        "fixed-char-v1",
        "v1",
        stable_digest(
            "fixed-char-v1",
            frozen_chunker.artifact_id,
            str(frozen_chunker.max_characters),
            "no-overlap",
        ),
    )
    normalized_chunkers = tuple(
        (str(row[0]), str(row[1]), str(row[2]), _text(row[3])) for row in chunker_rows
    )
    if normalized_chunkers != (expected_chunker,) or chunker_ids != (
        frozen_chunker.artifact_id,
    ):
        raise ArtifactConflictError("M3 chunker registry closure drift")

    verifier_run_identity = {
        (item.calibration_version, item.temperature) for item in manifest.verifications
    }
    if len(verifier_run_identity) != 1:
        raise ArtifactConflictError("M3 verifier run identity is ambiguous")
    calibration_version, temperature = next(iter(verifier_run_identity))
    expected_input_hash = stable_digest(
        "m3-run-input-v1",
        manifest.corpus_hash,
        manifest.question_id,
        manifest.config_hash,
        frozen_chunker.artifact_id,
        manifest.decision_policy_version,
        calibration_version,
        repr(temperature),
        *manifest.model_artifact_ids,
        *manifest.prompt_artifact_ids,
    )
    if manifest.input_hash != expected_input_hash or manifest.run_id != (
        "run-" + expected_input_hash
    ):
        raise ArtifactConflictError("M3 run input identity drift")

    embedding_rows = connection.execute(
        """
        SELECT chunk_version_id, model_artifact_id
        FROM groundloop_chunk_embedding
        ORDER BY chunk_version_id, model_artifact_id
        """
    ).fetchall()
    expected_use: set[tuple[str, str]] = {
        *(("model", item) for item in manifest.model_artifact_ids),
        *(("prompt", item) for item in manifest.prompt_artifact_ids),
        ("chunker", chunker_ids[0]),
        *(("embedding", f"embedding:{row[0]}:{row[1]}") for row in embedding_rows),
        *(("retrieval", item.candidate_id) for item in manifest.retrieval_candidates),
        ("generation", answer_id),
        *(("extraction", item) for item in claim_ids),
        *(("verification", item) for item in observation_ids),
    }
    rows = connection.execute(
        """
        SELECT artifact_kind, artifact_id, reused
        FROM groundloop_pipeline_artifact_use
        WHERE run_id = %s
        ORDER BY artifact_kind, artifact_id
        """,
        (manifest.run_id,),
    ).fetchall()
    actual_use = {(str(row[0]), str(row[1])) for row in rows}
    if actual_use != expected_use or len(rows) != len(expected_use):
        raise ArtifactConflictError("M3 pipeline artifact-use closure drift")
    reused = set(manifest.reused_artifact_ids)
    new = set(manifest.new_artifact_ids)
    ledger_ids = {artifact_id for _kind, artifact_id in actual_use}
    expected_manifest_artifact_ids = {
        *ledger_ids,
        manifest.decision_policy_version,
        *(item.document_version_id for item in documents),
        *(item.chunk_version_id for item in chunks),
    }
    if reused & new or reused | new != expected_manifest_artifact_ids:
        raise ArtifactConflictError("M3 manifest artifact ledger closure drift")
    if any(bool(row[2]) != (str(row[1]) in reused) for row in rows):
        raise ArtifactConflictError("M3 artifact reuse provenance drift")

    timing_rows = connection.execute(
        """
        SELECT component, elapsed_ms, cold_start
        FROM groundloop_component_timing
        WHERE run_id = %s
        ORDER BY component, cold_start
        """,
        (manifest.run_id,),
    ).fetchall()
    actual_timings = tuple(
        (str(row[0]), float(row[1]), bool(row[2])) for row in timing_rows
    )
    expected_timings = tuple(
        sorted(
            (item.component, item.elapsed_ms, item.cold_start)
            for item in manifest.timings
        )
    )
    if actual_timings != expected_timings:
        raise ArtifactConflictError("M3 component timing closure drift")
    if pristine and _count_table(
        connection, _current_schema(connection), "groundloop_status_delta"
    ):
        raise ArtifactConflictError(
            "static M3 source unexpectedly contains status deltas"
        )


def _load_source(
    connection: Connection[Any],
    *,
    run_id: str,
    lock_run: bool = False,
    pristine: bool = True,
) -> tuple[PublishedM3ActivationSource, frozenset[str]]:
    schema_name = _current_schema(connection)
    existing = _require_local_relations(connection, schema_name)
    manifest, epoch_id, answer_id = _load_run_manifest(
        connection, run_id, lock_run=lock_run
    )
    _validate_epoch(connection, manifest, epoch_id, pristine=pristine)
    policy = _load_policy(connection, manifest, epoch_id)
    models, prompts = _validate_models_and_prompts(connection, manifest)
    claims = _validate_answer_and_claims(
        connection, manifest, epoch_id, answer_id, models, prompts
    )
    candidates = _validate_candidates(connection, manifest, models)
    observations = _validate_verifications(
        connection,
        manifest,
        epoch_id,
        answer_id,
        candidates,
        models,
        prompts,
        validate_current_currency=pristine,
    )
    documents = _load_documents(connection, manifest, epoch_id, pristine=pristine)
    chunks = _load_chunks(
        connection,
        manifest,
        epoch_id,
        documents,
        pristine=pristine,
    )
    if pristine:
        _validate_states(
            connection,
            manifest,
            epoch_id,
            answer_id,
            tuple(item.claim_id for item in claims),
        )
    _validate_audit_closure(
        connection,
        manifest,
        documents,
        chunks,
        tuple(item.claim_id for item in claims),
        observations,
        answer_id,
        pristine=pristine,
    )
    return (
        PublishedM3ActivationSource(
            schema_name=schema_name,
            manifest=manifest,
            epoch_id=epoch_id,
            answer_version_id=answer_id,
            claims=claims,
            chunks=chunks,
            observation_ids=observations,
            decision_policy=policy,
        ),
        existing,
    )


def _policy_id(run_id: str, snapshot_id: str, policy_hash: str) -> str:
    digest = stable_m4_digest(
        "m4-m3-run-candidate-policy-id-v1",
        run_id,
        snapshot_id,
        policy_hash,
    )
    return f"m4-m3-policy-{digest}"


def _build_policy(
    *,
    source: PublishedM3ActivationSource,
    embeddings: M4BgeRoleAdapter,
    verifier_spec: VerificationAdapterSpec,
    repo_root: Path,
    lexical_config_path: Path | None,
    approximate_cap_per_inserted_chunk: int,
    frontier_depth: int,
    server: PostgresAdmissionServerIdentity,
) -> CandidatePolicyManifest:
    lexical = (
        load_frozen_lexical_v1(repo_root)
        if lexical_config_path is None
        else LexicalV1Config.load(lexical_config_path)
    )
    vector = ExactPgvectorConfig(
        dimensions=embeddings.spec.dimension,
        pgvector_version=server.pgvector_version,
    )
    provisional = build_candidate_policy_manifest(
        policy_id="m4-m3-policy-pending",
        embedding_model_artifact_id=embeddings.spec.model_artifact.artifact_id,
        claim_role_template=CLAIM_ROLE_TEMPLATE,
        chunk_role_template=CHUNK_ROLE_TEMPLATE,
        vector_method_version="exact-reverse-pgvector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config=vector.build_config,
        vector_search_config=vector.search_config,
        lexical_method_version="postgres-lexical-v1",
        lexical_config=lexical,
        lexical_postgres_version=server.postgres_version,
        lexical_regconfig_identity=server.regconfig_identity,
        claim_registry_snapshot_id=source.registry_snapshot_id,
        claim_count=len(source.claims),
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=approximate_cap_per_inserted_chunk,
        frontier_depth=frontier_depth,
        verifier_execution_spec_hash=verifier_spec.execution_spec_hash,
        decision_policy_version=source.decision_policy.policy_version,
        lineage_safety_override=True,
    )
    return replace(
        provisional,
        policy_id=_policy_id(
            source.run_id,
            source.registry_snapshot_id,
            provisional.policy_hash,
        ),
    )


def _certificate_digest(state: ClaimState) -> str:
    return stable_m4_digest(
        "m4-claim-certificate-v1",
        state.claim_id,
        state.supporting_observation_ids[0] if state.supporting_observation_ids else "",
        state.refuting_observation_ids[0] if state.refuting_observation_ids else "",
    )


def _validate_publication(
    connection: Connection[Any],
    source: PublishedM3ActivationSource,
    *,
    allow_history: bool,
) -> None:
    head = connection.execute(
        "SELECT singleton, epoch_id FROM groundloop_m4_publication_head"
    ).fetchall()
    if (
        len(head) != 1
        or head[0][0] is not True
        or int(head[0][1]) < source.epoch_id
        or (not allow_history and int(head[0][1]) != source.epoch_id)
    ):
        raise EventConflictError("M4 publication head differs from M3 base epoch")

    source_observations = connection.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type,
               observation_id
        FROM groundloop_semantic_observation
        WHERE observation_id = ANY(%s::text[])
        ORDER BY subject_kind, subject_id, chunk_version_id, task_type,
                 observation_id
        """,
        (list(source.observation_ids),),
    ).fetchall()
    published = connection.execute(
        """
        SELECT subject_kind, subject_id, chunk_version_id, task_type,
               observation_id, valid_from_epoch, valid_to_epoch
        FROM groundloop_published_observation_currency
        WHERE valid_from_epoch = %s
        ORDER BY subject_kind, subject_id, chunk_version_id, task_type,
                 valid_from_epoch
        """,
        (source.epoch_id,),
    ).fetchall()
    expected_published = tuple((*row, source.epoch_id) for row in source_observations)
    normalized_published = tuple((*row[:5], int(row[5])) for row in published)
    if normalized_published != expected_published or any(
        row[6] is not None and (not allow_history or int(row[6]) <= source.epoch_id)
        for row in published
    ):
        raise ArtifactConflictError("M4 published observation baseline drift")

    expected_claims = {item.claim_id: item for item in source.manifest.claim_states}
    claim_rows = connection.execute(
        """
        SELECT claim_id, valid_from_epoch, valid_to_epoch, support_count,
               refute_count, best_support_score, best_refute_score,
               supporting_observation_ids, refuting_observation_ids, status,
               certificate_digest
        FROM groundloop_published_claim_state
        WHERE valid_from_epoch = %s
        ORDER BY claim_id, valid_from_epoch
        """,
        (source.epoch_id,),
    ).fetchall()
    if len(claim_rows) != len(expected_claims):
        raise ArtifactConflictError("M4 published claim baseline is incomplete")
    for row in claim_rows:
        claim_state = expected_claims.get(str(row[0]))
        if claim_state is None:
            raise ArtifactConflictError("M4 published claim baseline has extra rows")
        actual_claim = (
            int(row[1]),
            row[2],
            int(row[3]),
            int(row[4]),
            None if row[5] is None else float(row[5]),
            None if row[6] is None else float(row[6]),
            tuple(str(value) for value in row[7]),
            tuple(str(value) for value in row[8]),
            str(row[9]),
            _text(row[10]),
        )
        expected_claim = (
            source.epoch_id,
            claim_state.support_count,
            claim_state.refute_count,
            claim_state.best_support_score,
            claim_state.best_refute_score,
            claim_state.supporting_observation_ids,
            claim_state.refuting_observation_ids,
            claim_state.status.value,
            _certificate_digest(claim_state),
        )
        normalized_claim = (actual_claim[0], *actual_claim[2:])
        if normalized_claim != expected_claim or (
            actual_claim[1] is not None
            and (not allow_history or int(actual_claim[1]) <= source.epoch_id)
        ):
            raise ArtifactConflictError("M4 published claim baseline drift")

    expected_answers = {
        item.answer_version_id: item for item in source.manifest.answer_states
    }
    answer_rows = connection.execute(
        """
        SELECT answer_version_id, valid_from_epoch, valid_to_epoch,
               required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status
        FROM groundloop_published_answer_state
        WHERE valid_from_epoch = %s
        ORDER BY answer_version_id, valid_from_epoch
        """,
        (source.epoch_id,),
    ).fetchall()
    if len(answer_rows) != len(expected_answers):
        raise ArtifactConflictError("M4 published answer baseline is incomplete")
    for row in answer_rows:
        answer_state = expected_answers.get(str(row[0]))
        if answer_state is None:
            raise ArtifactConflictError("M4 published answer baseline has extra rows")
        actual_answer = (
            int(row[1]),
            row[2],
            int(row[3]),
            int(row[4]),
            int(row[5]),
            int(row[6]),
            int(row[7]),
            str(row[8]),
        )
        expected_answer = (
            source.epoch_id,
            answer_state.required_claim_count,
            answer_state.supported_count,
            answer_state.unsupported_count,
            answer_state.refuted_count,
            answer_state.conflicted_count,
            answer_state.status.value,
        )
        normalized_answer = (actual_answer[0], *actual_answer[2:])
        if normalized_answer != expected_answer or (
            actual_answer[1] is not None
            and (not allow_history or int(actual_answer[1]) <= source.epoch_id)
        ):
            raise ArtifactConflictError("M4 published answer baseline drift")


def _validate_activation_artifacts(
    connection: Connection[Any],
    *,
    source: PublishedM3ActivationSource,
    existing: frozenset[str],
    allow_history: bool = False,
    expected_embedding_spec_hash: str | None = None,
) -> M3M4ActivationReceipt:
    if not allow_history:
        _require_only_activation_m4(connection, source.schema_name, existing)
    _validate_publication(connection, source, allow_history=allow_history)

    snapshot_rows = connection.execute(
        """
        SELECT claim_registry_snapshot_id, claim_count, claim_set_hash
        FROM groundloop_m4_claim_registry_snapshot
        """
    ).fetchall()
    expected_set_hash = stable_m4_digest(
        "m4-claim-registry-snapshot-v1", *source.claim_ids
    )
    expected_snapshot = (
        source.registry_snapshot_id,
        len(source.claims),
        expected_set_hash,
    )
    normalized_snapshot = tuple(
        (str(row[0]), int(row[1]), _text(row[2])) for row in snapshot_rows
    )
    if normalized_snapshot != (expected_snapshot,):
        raise ArtifactConflictError("M4 claim registry snapshot drift")
    members = tuple(
        (str(row[0]), int(row[1]))
        for row in connection.execute(
            """
            SELECT claim_id, member_ordinal
            FROM groundloop_m4_claim_registry_member
            WHERE claim_registry_snapshot_id = %s
            ORDER BY member_ordinal
            """,
            (source.registry_snapshot_id,),
        ).fetchall()
    )
    expected_members = tuple(
        (claim_id, ordinal) for ordinal, claim_id in enumerate(source.claim_ids)
    )
    if members != expected_members:
        raise ArtifactConflictError("M4 claim registry membership drift")

    policy_rows = connection.execute(
        "SELECT candidate_policy_id FROM groundloop_candidate_policy"
    ).fetchall()
    if len(policy_rows) != 1:
        raise ArtifactConflictError("M4 activation requires one candidate policy")
    policy_id = str(policy_rows[0][0])
    policy = PostgresM4RuntimeStore(connection).read_candidate_policy(policy_id)
    if (
        policy.claim_registry_snapshot_id != source.registry_snapshot_id
        or policy.claim_count != len(source.claims)
        or policy.decision_policy_version != source.decision_policy.policy_version
        or policy.claim_role_template_hash != sha256_text(CLAIM_ROLE_TEMPLATE)
        or policy.chunk_role_template_hash != sha256_text(CHUNK_ROLE_TEMPLATE)
        or policy.policy_id
        != _policy_id(source.run_id, source.registry_snapshot_id, policy.policy_hash)
    ):
        raise ArtifactConflictError("M4 candidate policy/source binding drift")

    role_rows = connection.execute(
        """
        SELECT subject_id, embedding_role, model_artifact_id,
               role_template_hash, input_hash, adapter_spec_hash,
               artifact_id, model_id, model_revision, tokenizer_revision,
               vector_hash, token_count, max_tokens, truncated,
               abs(vector_norm(embedding) - 1.0) <= 1e-6,
               CASE
                   WHEN embedding_role = 'chunk_passage'
                    AND subject_id = ANY(%s::text[])
                   THEN EXISTS (
                       SELECT 1 FROM groundloop_chunk_embedding AS baseline
                       WHERE baseline.chunk_version_id = subject_id
                         AND baseline.model_artifact_id =
                             groundloop_m4_role_embedding_artifact.model_artifact_id
                         AND baseline.embedding =
                             groundloop_m4_role_embedding_artifact.embedding
                   )
                   ELSE true
               END
        FROM groundloop_m4_role_embedding_artifact
        ORDER BY embedding_role, subject_id
        """,
        (list(source.chunk_ids),),
    ).fetchall()
    expected_roles = tuple(
        sorted(
            (
                *(
                    (
                        claim.claim_id,
                        "claim_query",
                        policy.embedding_model_artifact_id,
                        policy.claim_role_template_hash,
                        sha256_text(CLAIM_ROLE_TEMPLATE.format(text=claim.text)),
                    )
                    for claim in source.claims
                ),
                *(
                    (
                        chunk.chunk_version_id,
                        "chunk_passage",
                        policy.embedding_model_artifact_id,
                        policy.chunk_role_template_hash,
                        sha256_text(CHUNK_ROLE_TEMPLATE.format(text=chunk.text)),
                    )
                    for chunk in source.chunks
                ),
            ),
            key=lambda item: (item[1], item[0]),
        )
    )
    actual_roles = tuple(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            _text(row[3]),
            _text(row[4]),
        )
        for row in role_rows
    )
    actual_by_key = {(item[1], item[0]): item for item in actual_roles}
    baseline_roles = tuple(
        actual_by_key.get((item[1], item[0])) for item in expected_roles
    )
    extra_claim_roles = tuple(
        item
        for item in actual_roles
        if item[1] == "claim_query"
        and (item[1], item[0])
        not in {(expected[1], expected[0]) for expected in expected_roles}
    )
    baseline_role_rows = tuple(
        row
        for row in role_rows
        if (str(row[1]), str(row[0])) in {(item[1], item[0]) for item in expected_roles}
    )
    adapter_hashes = {_text(row[5]) for row in baseline_role_rows}
    role_payload_valid = True
    for row in baseline_role_rows:
        role = EmbeddingRole(str(row[1]))
        expected_artifact_id = RoleEmbeddingProvenance.build_artifact_id(
            subject_id=str(row[0]),
            role=role,
            model_artifact_id=str(row[2]),
            model_id=str(row[7]),
            model_revision=str(row[8]),
            tokenizer_revision=str(row[9]),
            role_template_hash=_text(row[3]),
            input_hash=_text(row[4]),
            vector_hash=_text(row[10]),
            adapter_spec_hash=_text(row[5]),
        )
        token_count = None if row[11] is None else int(row[11])
        if (
            _text(row[6]) != expected_artifact_id
            or (token_count is not None and token_count < 0)
            or int(row[12]) <= 0
            or not isinstance(row[13], bool)
            or not bool(row[14])
            or not bool(row[15])
        ):
            role_payload_valid = False
    if (
        baseline_roles != expected_roles
        or len(baseline_role_rows) != len(expected_roles)
        or extra_claim_roles
        or (not allow_history and actual_roles != expected_roles)
        or len(adapter_hashes) != 1
        or (
            expected_embedding_spec_hash is not None
            and adapter_hashes != {expected_embedding_spec_hash}
        )
        or not role_payload_valid
    ):
        raise ArtifactConflictError("M4 role embedding closure drift")

    index_rows = tuple(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            _text(row[3]),
            _text(row[4]),
            bool(row[5]),
            bool(row[6]),
        )
        for row in connection.execute(
            """
            SELECT admission.claim_registry_snapshot_id, admission.claim_id,
                   admission.embedding_model_artifact_id,
                   admission.claim_role_template_hash,
                   admission.embedding_input_hash,
                   admission.embedding = role.embedding,
                   admission.lexical_tsv =
                       to_tsvector(%s::regconfig, claim.text)
            FROM groundloop_m4_claim_admission_index AS admission
            JOIN groundloop_claim AS claim USING (claim_id)
            JOIN groundloop_m4_role_embedding_artifact AS role
              ON role.embedding_role = 'claim_query'
             AND role.claim_id = admission.claim_id
             AND role.model_artifact_id = admission.embedding_model_artifact_id
             AND role.role_template_hash = admission.claim_role_template_hash
             AND role.input_hash = admission.embedding_input_hash
            ORDER BY admission.claim_id, role.artifact_id
            """,
            (policy.lexical_regconfig_identity,),
        ).fetchall()
    )
    expected_index = tuple(
        (
            source.registry_snapshot_id,
            claim.claim_id,
            policy.embedding_model_artifact_id,
            policy.claim_role_template_hash,
            sha256_text(CLAIM_ROLE_TEMPLATE.format(text=claim.text)),
            True,
            True,
        )
        for claim in source.claims
    )
    if index_rows != expected_index:
        raise ArtifactConflictError("M4 claim admission index closure drift")

    verifier_identity = next(
        iter(
            {
                (
                    item.model_artifact_id,
                    item.prompt_artifact_id,
                    item.calibration_version,
                    item.temperature,
                )
                for item in source.manifest.verifications
            }
        )
    )

    return M3M4ActivationReceipt(
        schema_version=ACTIVATION_SCHEMA_VERSION,
        run_id=source.run_id,
        base_epoch_id=source.epoch_id,
        answer_version_id=source.answer_version_id,
        claim_registry_snapshot_id=source.registry_snapshot_id,
        candidate_policy_id=policy.policy_id,
        candidate_policy_hash=policy.policy_hash,
        claim_count=len(source.claims),
        chunk_count=len(source.chunks),
        observation_count=len(source.observation_ids),
        role_artifact_count=len(expected_roles),
        claim_index_count=len(index_rows),
        claim_ids=source.claim_ids,
        chunk_ids=source.chunk_ids,
        embedding_model_artifact_id=policy.embedding_model_artifact_id,
        verifier_model_artifact_id=verifier_identity[0],
        verifier_prompt_artifact_id=verifier_identity[1],
        calibration_version=verifier_identity[2],
        calibration_temperature=verifier_identity[3],
        decision_policy_version=policy.decision_policy_version,
        verifier_execution_spec_hash=policy.verifier_execution_spec_hash,
        vector_method_version=policy.vector_method_version,
        vector_index_kind=policy.vector_index_kind.value,
        vector_index_build_config_hash=policy.vector_index_build_config_hash,
        vector_search_config_hash=policy.vector_search_config_hash,
        lexical_method_version=policy.lexical_method_version,
        lexical_config_hash=policy.lexical_config_hash,
        created_role_artifact_count=0,
        reused_role_artifact_count=len(expected_roles),
        activation_embedding_request_count=0,
        baseline_python_mismatch_count=0,
        baseline_sql_claim_mismatch_count=0,
        baseline_sql_answer_mismatch_count=0,
        global_closure_valid=True,
        replayed=True,
    )


def _validate_registered_model(
    connection: Connection[Any], artifact: ModelArtifact
) -> None:
    row = connection.execute(
        """
        SELECT task, provider, model_id, immutable_revision,
               tokenizer_revision, license_id, config_hash, artifact_sha256
        FROM groundloop_model_artifact
        WHERE model_artifact_id = %s
        """,
        (artifact.artifact_id,),
    ).fetchone()
    actual = None
    if row is not None:
        actual = (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            _text(row[6]),
            None if row[7] is None else _text(row[7]),
        )
    expected = (
        artifact.task.value,
        artifact.provider,
        artifact.model_id,
        artifact.immutable_revision,
        artifact.tokenizer_revision,
        artifact.license_id,
        artifact.config_hash,
        artifact.artifact_sha256,
    )
    if actual != expected:
        raise ArtifactConflictError(
            f"runtime model artifact {artifact.artifact_id} differs from M3"
        )


def _validate_registered_prompt(
    connection: Connection[Any], artifact: PromptArtifact
) -> None:
    row = connection.execute(
        """
        SELECT task, version, template, template_hash, decoding_config_hash
        FROM groundloop_prompt_artifact
        WHERE prompt_artifact_id = %s
        """,
        (artifact.artifact_id,),
    ).fetchone()
    actual = None
    if row is not None:
        actual = (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            _text(row[3]),
            _text(row[4]),
        )
    expected = (
        artifact.task.value,
        artifact.version,
        artifact.template,
        artifact.template_hash,
        artifact.decoding_config_hash,
    )
    if actual != expected:
        raise ArtifactConflictError(
            f"runtime prompt artifact {artifact.artifact_id} differs from M3"
        )


def _validate_adapter_bindings(
    connection: Connection[Any],
    *,
    source: PublishedM3ActivationSource,
    embedding_adapter: M4BgeRoleAdapter,
    verifier_spec: VerificationAdapterSpec,
    candidate_policy: CandidatePolicyManifest | None = None,
    require_materialized_calibration: bool = False,
) -> None:
    if embedding_adapter.spec.dimension != EMBEDDING_DIMENSION:
        raise ArtifactConflictError("M4 activation requires 384-dimensional BGE")
    if embedding_adapter.spec.adapter_version != "m4-m3-bge-role-adapter-v1":
        raise ArtifactConflictError("M4 embedding adapter version is not frozen")
    if verifier_spec.adapter_version != "m4-m3-verifier-adapter-v1":
        raise ArtifactConflictError("M4 verifier adapter version is not frozen")
    if verifier_spec.decision_policy != source.decision_policy:
        raise ArtifactConflictError("M4 verifier decision policy differs from M3")
    verification_model_ids = {
        item.model_artifact_id for item in source.manifest.verifications
    }
    verification_prompt_ids = {
        item.prompt_artifact_id for item in source.manifest.verifications
    }
    calibration_versions = {
        item.calibration_version for item in source.manifest.verifications
    }
    temperatures = {item.temperature for item in source.manifest.verifications}
    candidate_embedding_ids = {
        item.embedding_model_artifact_id
        for item in source.manifest.retrieval_candidates
    }
    if verification_model_ids != {verifier_spec.model_artifact.artifact_id}:
        raise ArtifactConflictError("M4 verifier model differs from M3 run")
    if verification_prompt_ids != {verifier_spec.prompt_artifact.artifact_id}:
        raise ArtifactConflictError("M4 verifier prompt differs from M3 run")
    if calibration_versions != {verifier_spec.calibration_version}:
        raise ArtifactConflictError("M4 verifier calibration differs from M3 run")
    if temperatures != {verifier_spec.temperature}:
        raise ArtifactConflictError("M4 verifier temperature differs from M3 run")
    claim_text_by_local_id = {
        item.local_claim_id: item.text for item in source.manifest.claims
    }
    chunk_text_by_id = {item.chunk_version_id: item.text for item in source.chunks}
    for result in source.manifest.verifications:
        claim_text = claim_text_by_local_id.get(result.claim_id)
        chunk_text = chunk_text_by_id.get(result.chunk_version_id)
        if claim_text is None or chunk_text is None:
            raise ArtifactConflictError("M3 verifier input identity is unresolved")
        expected_input_hash = stable_digest(
            "verification-input-v1",
            claim_text.strip(),
            chunk_text.strip(),
            verifier_spec.model_artifact.artifact_id,
            verifier_spec.prompt_artifact.artifact_id,
            str(verifier_spec.max_length),
            verifier_spec.calibration_version,
            repr(verifier_spec.temperature),
        )
        if result.input_hash != expected_input_hash:
            raise ArtifactConflictError(
                "M4 verifier max_length/input identity differs from M3 run"
            )
    if candidate_embedding_ids != {embedding_adapter.spec.model_artifact.artifact_id}:
        raise ArtifactConflictError("M4 embedding model differs from M3 run")
    if (
        require_materialized_calibration
        and verifier_spec.calibration_artifact_sha256 is None
    ):
        raise ArtifactConflictError(
            "dynamic M4 composition requires a calibration artifact hash"
        )
    _validate_registered_model(connection, embedding_adapter.spec.model_artifact)
    _validate_registered_model(connection, verifier_spec.model_artifact)
    _validate_registered_prompt(connection, verifier_spec.prompt_artifact)
    if candidate_policy is None:
        return
    expected_policy_binding = (
        source.registry_snapshot_id,
        len(source.claims),
        embedding_adapter.spec.model_artifact.artifact_id,
        sha256_text(embedding_adapter.spec.claim_role_template),
        sha256_text(embedding_adapter.spec.chunk_role_template),
        verifier_spec.execution_spec_hash,
        source.decision_policy.policy_version,
    )
    actual_policy_binding = (
        candidate_policy.claim_registry_snapshot_id,
        candidate_policy.claim_count,
        candidate_policy.embedding_model_artifact_id,
        candidate_policy.claim_role_template_hash,
        candidate_policy.chunk_role_template_hash,
        candidate_policy.verifier_execution_spec_hash,
        candidate_policy.decision_policy_version,
    )
    if actual_policy_binding != expected_policy_binding:
        raise ArtifactConflictError("runtime adapters differ from activated policy")


def _validate_m4_history_chain(
    connection: Connection[Any],
    *,
    source: PublishedM3ActivationSource,
    receipt: M3M4ActivationReceipt,
) -> None:
    head_row = connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
    ).fetchone()
    if head_row is None:
        raise EventConflictError("activated M4 runtime has no publication head")
    head = int(head_row[0])
    rows = connection.execute(
        """
        SELECT update.epoch_id, update.previous_published_epoch_id,
               update.candidate_policy_id, update.registry_snapshot_id,
               epoch.structural_status, epoch.semantic_status,
               epoch.evaluation_state, epoch.publication_mode,
               epoch.sealed_at IS NOT NULL
        FROM groundloop_m4_update AS update
        JOIN groundloop_epoch AS epoch USING (epoch_id)
        ORDER BY update.epoch_id
        """
    ).fetchall()
    by_epoch = {int(row[0]): row for row in rows}
    if len(by_epoch) != len(rows):
        raise ArtifactConflictError("M4 history contains duplicate update epochs")
    chain: set[int] = set()
    current = head
    while current != source.epoch_id:
        if current < source.epoch_id or current in chain:
            raise ArtifactConflictError("M4 publication ancestry is cyclic or stale")
        row = by_epoch.get(current)
        if row is None or row[1] is None:
            raise ArtifactConflictError("M4 publication ancestry is incomplete")
        previous = int(row[1])
        if (
            previous >= current
            or str(row[2]) != receipt.candidate_policy_id
            or str(row[3]) != receipt.claim_registry_snapshot_id
            or str(row[4]) != "committed"
            or str(row[5]) != "sealed"
            or str(row[6]) != "complete"
            or str(row[7]) != "strict"
            or not bool(row[8])
        ):
            raise ArtifactConflictError("M4 publication ancestry contains drift")
        chain.add(current)
        current = previous

    runtime_book = PostgresM4RuntimeStore(connection).read_book()
    runtime_epochs = {epoch.epoch_id: epoch for epoch in runtime_book.epochs}
    if set(runtime_epochs) != set(by_epoch):
        raise ArtifactConflictError("M4 runtime history projection is incomplete")

    sealed_epochs = {source.epoch_id, *chain}
    active_epochs: list[int] = []
    for epoch_id, row in sorted(by_epoch.items()):
        row_previous = None if row[1] is None else int(row[1])
        expected_previous = max(
            sealed_epoch for sealed_epoch in sealed_epochs if sealed_epoch < epoch_id
        )
        if (
            row_previous != expected_previous
            or str(row[2]) != receipt.candidate_policy_id
            or str(row[3]) != receipt.claim_registry_snapshot_id
        ):
            raise ArtifactConflictError("M4 update ancestry or policy binding drift")

        state = runtime_epochs[epoch_id].state
        persisted_state = (
            str(row[4]),
            str(row[5]),
            str(row[6]),
            str(row[7]),
            bool(row[8]),
        )
        if state is RuntimeEpochState.SEALED:
            if epoch_id not in chain or persisted_state != (
                "committed",
                "sealed",
                "complete",
                "strict",
                True,
            ):
                raise ArtifactConflictError("M4 sealed history contains drift")
        elif state is RuntimeEpochState.FAILED:
            if epoch_id in chain or persisted_state != (
                "failed",
                "failed",
                "failed",
                "provisional",
                False,
            ):
                raise ArtifactConflictError("M4 failed history contains drift")
        elif state is RuntimeEpochState.SEMANTIC_PENDING:
            active_epochs.append(epoch_id)
            if epoch_id in chain or persisted_state != (
                "committed",
                "pending",
                "pending",
                "provisional",
                False,
            ):
                raise ArtifactConflictError("M4 pending history contains drift")
        elif state is RuntimeEpochState.SEMANTIC_COMPLETE:
            active_epochs.append(epoch_id)
            if epoch_id in chain or persisted_state != (
                "committed",
                "complete",
                "complete",
                "provisional",
                False,
            ):
                raise ArtifactConflictError("M4 complete history contains drift")
        else:  # pragma: no cover - exhaustive over the frozen runtime enum.
            raise ArtifactConflictError("M4 history contains an unknown state")
    if len(active_epochs) > 1 or (active_epochs and active_epochs[0] != max(by_epoch)):
        raise ArtifactConflictError("M4 history contains invalid active-epoch order")
    epoch_ids = {
        int(row[0])
        for row in connection.execute(
            "SELECT epoch_id FROM groundloop_epoch"
        ).fetchall()
    }
    if epoch_ids != {source.epoch_id, *by_epoch}:
        raise ArtifactConflictError("M4 history contains an unrelated epoch")


def _validate_runtime_activation_ancestry(
    connection: Connection[Any],
    *,
    run_id: str,
    embedding_spec_hash: str,
) -> _RuntimeActivationAncestry:
    _require_autocommit(connection)
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        source, existing = _load_source(connection, run_id=run_id, pristine=False)
        receipt = _validate_activation_artifacts(
            connection,
            source=source,
            existing=existing,
            allow_history=True,
            expected_embedding_spec_hash=embedding_spec_hash,
        )
        _validate_m4_history_chain(connection, source=source, receipt=receipt)
    return _RuntimeActivationAncestry(source, receipt)


def _validate_all_role_adapter_specs(
    connection: Connection[Any],
    *,
    policy: CandidatePolicyManifest,
    expected_spec_hash: str,
) -> None:
    rows = connection.execute(
        """
        SELECT DISTINCT adapter_spec_hash
        FROM groundloop_m4_role_embedding_artifact
        WHERE model_artifact_id = %s
          AND (
              (embedding_role = 'claim_query' AND role_template_hash = %s)
              OR
              (embedding_role = 'chunk_passage' AND role_template_hash = %s)
          )
        ORDER BY adapter_spec_hash
        """,
        (
            policy.embedding_model_artifact_id,
            policy.claim_role_template_hash,
            policy.chunk_role_template_hash,
        ),
    ).fetchall()
    if tuple(_text(row[0]) for row in rows) != (expected_spec_hash,):
        raise ArtifactConflictError("runtime role artifacts use another adapter spec")


def inspect_m3_m4_activation(
    connection: Connection[Any], *, run_id: str
) -> M3M4ActivationInspection:
    """Validate the complete source and return an exact replay if activated."""
    _require_autocommit(connection)
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        source, existing = _load_source(connection, run_id=run_id)
        head = connection.execute(
            "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
        ).fetchone()
        if head is None:
            _require_empty_m4(connection, source.schema_name, existing)
            return M3M4ActivationInspection(source, None)
        if int(head[0]) != source.epoch_id:
            raise EventConflictError(
                "M4 publication head already advanced beyond the selected M3 run"
            )
        receipt = _validate_activation_artifacts(
            connection, source=source, existing=existing
        )
        return M3M4ActivationInspection(source, receipt)


def validate_m3_m4_activation_replay(
    connection: Connection[Any], *, run_id: str
) -> M3M4ActivationReceipt | None:
    """Return a locked, validation-only replay or ``None`` before activation."""
    inspection = inspect_m3_m4_activation(connection, run_id=run_id)
    if inspection.replay_receipt is None:
        return None
    with connection.transaction():
        locked_head = lock_m3_m4_lifecycle(connection)
        if locked_head != inspection.source.epoch_id:
            raise EventConflictError("M4 activation head changed during exact replay")
        locked_source, existing = _load_source(connection, run_id=run_id, lock_run=True)
        if locked_source != inspection.source:
            raise EventConflictError("M3 source changed during exact activation replay")
        return _validate_activation_artifacts(
            connection, source=locked_source, existing=existing
        )


def activate_published_m3_run(
    connection: Connection[Any],
    *,
    run_id: str,
    embeddings: M4BgeRoleAdapter,
    verifier_spec: VerificationAdapterSpec,
    repo_root: Path,
    lexical_config_path: Path | None = None,
    approximate_cap_per_inserted_chunk: int | None = None,
    frontier_depth: int = 1,
    failure_injector: Callable[[str], None] | None = None,
) -> M3M4ActivationReceipt:
    """Serialize same-run activation callers before any model work."""
    _require_autocommit(connection)
    schema_name = _current_schema(connection)
    lock_digest = hashlib.sha256(
        (f"groundloop-m3-m4-activation-lock-v1\0{schema_name}\0{run_id}").encode()
    ).digest()
    lock_keys = (
        int.from_bytes(lock_digest[:4], "big", signed=True),
        int.from_bytes(lock_digest[4:8], "big", signed=True),
    )
    connection.execute(
        "SELECT pg_advisory_lock(%s, %s)",
        lock_keys,
    ).fetchone()
    try:
        return _activate_published_m3_run_serialized(
            connection,
            run_id=run_id,
            embeddings=embeddings,
            verifier_spec=verifier_spec,
            repo_root=repo_root,
            lexical_config_path=lexical_config_path,
            approximate_cap_per_inserted_chunk=(approximate_cap_per_inserted_chunk),
            frontier_depth=frontier_depth,
            failure_injector=failure_injector,
        )
    finally:
        unlocked = connection.execute(
            "SELECT pg_advisory_unlock(%s, %s)",
            lock_keys,
        ).fetchone()
        if unlocked != (True,):
            raise RuntimeError("M3-to-M4 activation advisory lock was not held")


def _activate_published_m3_run_serialized(
    connection: Connection[Any],
    *,
    run_id: str,
    embeddings: M4BgeRoleAdapter,
    verifier_spec: VerificationAdapterSpec,
    repo_root: Path,
    lexical_config_path: Path | None = None,
    approximate_cap_per_inserted_chunk: int | None = None,
    frontier_depth: int = 1,
    failure_injector: Callable[[str], None] | None = None,
) -> M3M4ActivationReceipt:
    """Activate one exact M3 run or return a validation-only replay receipt.

    Model inference is performed only after the read-only inspection proves
    that no activation exists.  The final transaction serializes against M3
    publication, revalidates the source, registers immutable M4 artifacts,
    and invokes the frozen bootstrap last.
    """
    inspection = inspect_m3_m4_activation(connection, run_id=run_id)
    source = inspection.source
    _validate_adapter_bindings(
        connection,
        source=source,
        embedding_adapter=embeddings,
        verifier_spec=verifier_spec,
    )

    cap = (
        len(source.claims)
        if approximate_cap_per_inserted_chunk is None
        else approximate_cap_per_inserted_chunk
    )
    if cap <= 0:
        raise ValidationError("M4 activation admission cap must be positive")
    if frontier_depth <= 0:
        raise ValidationError("M4 activation frontier depth must be positive")

    server = PostgresAdmissionServerIdentity.inspect(connection)
    policy = _build_policy(
        source=source,
        embeddings=embeddings,
        verifier_spec=verifier_spec,
        repo_root=repo_root,
        lexical_config_path=lexical_config_path,
        approximate_cap_per_inserted_chunk=cap,
        frontier_depth=frontier_depth,
        server=server,
    )
    if inspection.replay_receipt is not None:
        replayed = validate_m3_m4_activation_replay(connection, run_id=run_id)
        assert replayed is not None
        if (
            replayed.candidate_policy_id != policy.policy_id
            or replayed.candidate_policy_hash != policy.policy_hash
        ):
            raise ArtifactConflictError(
                "activation replay configuration differs from frozen policy"
            )
        _validate_all_role_adapter_specs(
            connection,
            policy=policy,
            expected_spec_hash=embeddings.spec.spec_hash,
        )
        return replayed

    service = M4AdmissionEmbeddingService(embeddings)
    artifacts = service.embed_for_admission(
        claims=source.claims,
        chunks=source.chunks,
    )
    injector = failure_injector or (lambda _checkpoint: None)

    with connection.transaction():
        locked_head = lock_m3_m4_lifecycle(connection)
        if locked_head is not None:
            if locked_head != source.epoch_id:
                raise EventConflictError(
                    "M4 activation head changed while artifacts were prepared"
                )
            locked_source, existing = _load_source(
                connection, run_id=run_id, lock_run=True
            )
            concurrent = _validate_activation_artifacts(
                connection,
                source=locked_source,
                existing=existing,
                expected_embedding_spec_hash=embeddings.spec.spec_hash,
            )
            if (
                concurrent.candidate_policy_id != policy.policy_id
                or concurrent.candidate_policy_hash != policy.policy_hash
            ):
                raise ArtifactConflictError(
                    "concurrent activation used another frozen policy"
                )
            return concurrent
        locked_source, existing = _load_source(connection, run_id=run_id, lock_run=True)
        if locked_source != source:
            raise EventConflictError("M3 source changed while activation was prepared")
        _require_empty_m4(connection, source.schema_name, existing)

        registry = PostgresM4ArtifactRegistry(connection)
        runtime = PostgresM4RuntimeStore(connection)
        registry.register_model(embeddings.spec.model_artifact)
        registry.register_model(verifier_spec.model_artifact)
        registry.register_prompt(verifier_spec.prompt_artifact)
        injector("after_model_artifacts")
        runtime.register_claim_registry_snapshot(
            source.registry_snapshot_id, source.claim_ids
        )
        injector("after_claim_registry")
        runtime.register_candidate_policy(policy)
        injector("after_candidate_policy")
        seed = registry.seed_claim_admission_index(
            manifest=policy,
            artifacts=artifacts.claims,
            claim_texts={item.claim_id: item.text for item in source.claims},
        )
        injector("after_claim_artifacts")
        inserted_chunks = 0
        for artifact in artifacts.chunks:
            inserted_chunks += int(registry.register_role_embedding(artifact))
        injector("after_chunk_artifacts")
        if (
            seed.claim_count != len(source.claims)
            or seed.inserted_role_artifacts != len(source.claims)
            or seed.reused_role_artifacts
            or seed.inserted_index_rows != len(source.claims)
            or seed.reused_index_rows
            or inserted_chunks != len(source.chunks)
        ):
            raise ArtifactConflictError(
                "fresh activation unexpectedly reused M4 artifacts"
            )
        injector("after_artifacts")
        injector("before_bootstrap")

        bootstrap_m4_publication(connection, sealed_epoch_id=source.epoch_id)
        injector("after_bootstrap")
        receipt = _validate_activation_artifacts(
            connection,
            source=source,
            existing=existing,
            expected_embedding_spec_hash=embeddings.spec.spec_hash,
        )
        if (
            receipt.candidate_policy_id != policy.policy_id
            or receipt.candidate_policy_hash != policy.policy_hash
        ):
            raise ArtifactConflictError("activated candidate policy identity drift")

    return replace(
        receipt,
        created_role_artifact_count=receipt.role_artifact_count,
        reused_role_artifact_count=0,
        activation_embedding_request_count=service.request_count,
        replayed=False,
    )


def compose_activated_m3_runtime(
    connection: Connection[Any],
    *,
    run_id: str,
    embedding_adapter: M4BgeRoleAdapter,
    verifier_adapter: M4CalibratedVerifierAdapter,
    repo_root: Path,
    structural_payloads: Mapping[str, StructuralPayload],
    lexical_config_path: Path | None = None,
    verification_split_id: str = "m4-m3-activated-runtime-v1",
    failure_injector: Callable[[str], None] | None = None,
) -> M3M4RuntimeComposition:
    """Compose the production measured M4 runtime from an activated M3 base.

    This constructor performs no inference. It accepts a pristine activation
    or a sealed descendant history, revalidates the immutable M3 ancestry and
    activated artifacts, and then wires the existing public M4 components.
    """
    if not verification_split_id.strip():
        raise ValidationError("verification split identity must be non-empty")
    ancestry = _validate_runtime_activation_ancestry(
        connection,
        run_id=run_id,
        embedding_spec_hash=embedding_adapter.spec.spec_hash,
    )
    source = ancestry.source
    receipt = ancestry.receipt
    policy = PostgresM4RuntimeStore(connection).read_candidate_policy(
        receipt.candidate_policy_id
    )
    if (
        policy.policy_hash != receipt.candidate_policy_hash
        or policy.policy_id != receipt.candidate_policy_id
    ):
        raise ArtifactConflictError("activated candidate-policy receipt drift")
    _validate_adapter_bindings(
        connection,
        source=source,
        embedding_adapter=embedding_adapter,
        verifier_spec=verifier_adapter.spec,
        candidate_policy=policy,
        require_materialized_calibration=True,
    )
    _validate_all_role_adapter_specs(
        connection,
        policy=policy,
        expected_spec_hash=embedding_adapter.spec.spec_hash,
    )

    lexical_config = (
        load_frozen_lexical_v1(repo_root)
        if lexical_config_path is None
        else LexicalV1Config.load(lexical_config_path)
    )
    if (
        lexical_config.config_hash != policy.lexical_config_hash
        or lexical_config.postgres_regconfig
        != policy.lexical_regconfig_identity.rsplit(".", 1)[-1]
    ):
        raise ArtifactConflictError("runtime lexical policy differs from activation")
    server = PostgresAdmissionServerIdentity.inspect(connection)
    vector_config = ExactPgvectorConfig(
        dimensions=embedding_adapter.spec.dimension,
        pgvector_version=server.pgvector_version,
    )
    vector = PostgresExactReverseVectorIndex(
        connection=connection,
        server=server,
        manifest=policy,
        config=vector_config,
    )
    vector.validate_registry()
    analyzer = PostgresSimpleLexemeAnalyzer(connection, server)
    lexical_registry = LexicalRegistrySnapshot(
        policy.claim_registry_snapshot_id,
        tuple(
            (claim.claim_id, analyzer.analyze(claim.text)) for claim in source.claims
        ),
    )
    lexical_backend = PostgresLexicalSearchBackend(
        connection=connection,
        server=server,
        manifest=policy,
    )
    lexical_backend.validate_registry(lexical_registry)
    lexical = LexicalV1Policy(
        config=lexical_config,
        registry=lexical_registry,
        analyzer=analyzer,
        backend=lexical_backend,
    )

    embeddings = PersistingM4AdmissionEmbeddingService(
        M4AdmissionEmbeddingService(embedding_adapter),
        PostgresM4ArtifactRegistry(connection),
    )
    admission = PostgresHybridAdmissionPort(
        connection=connection,
        manifest=policy,
        embeddings=embeddings,
        vector_index=vector,
        lexical_policy=lexical,
        fresh_frontier_retriever=PostgresExactFreshFrontierRetriever(connection),
    )
    verifier = M4VerificationApplicationPort(
        PostgresPairInputResolver(connection), verifier_adapter
    )
    writer = PostgresM4VerificationExecutionWriter(
        verifier, split_id=verification_split_id
    )
    identity = M4ExecutionIdentity.build(
        manifest=policy,
        embedding_adapter_spec_hash=embedding_adapter.spec.spec_hash,
        vector_adapter_artifact_id=vector.artifact_id,
        lexical_analyzer_artifact_id=analyzer.artifact_id,
        lexical_backend_artifact_id=lexical_backend.artifact_id,
        frontier_retriever_artifact_id=EXACT_FRESH_FRONTIER_RETRIEVER_ID,
    )
    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads=structural_payloads,
        verification_provenance_writer=writer,
        failure_injector=failure_injector,
        execution_mode=M4ExecutionMode.MEASURED,
    )
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(connection, admission),
        verifier=verifier,
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            identity.impact_discovery_hash,
            identity.frontier_retrieval_hash,
            identity.verifier_hash,
        ),
    )
    return M3M4RuntimeComposition(
        activation_receipt=receipt,
        source=source,
        application=application,
        ports=ports,
        embeddings=embeddings,
        verifier=verifier,
        identity=identity,
        candidate_policy=policy,
    )
