"""One bounded M3-to-M4 end-to-end FYP demonstration.

The deterministic backend makes the system wiring reproducible, not the
semantic judgments correct.  Exactness claims begin only after the generated
M3 observations have been stored.  The optional real backend is local-only and
is an empirical diagnostic rather than an AI-quality gate.
"""

from __future__ import annotations

import hashlib
import json
import math
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, cast

import psycopg
from psycopg import Connection, sql

from groundloop.ai.application import (
    M3Application,
    M3ApplicationConfig,
    StaticEmbeddingProvider,
)
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import (
    DeterministicClaimExtractor,
    QwenClaimExtractor,
)
from groundloop.ai.contracts import (
    AnswerGenerator,
    AtomicClaim,
    ChunkDraft,
    ClaimExtractor,
    EmbeddingRecord,
    EvidenceVerifier,
    ModelArtifact,
    PipelineRunManifest,
    PromptArtifact,
    QueryKind,
    VerificationResult,
    stable_digest,
)
from groundloop.ai.embeddings import BgeSmallEmbedder, DeterministicFakeEmbedder
from groundloop.ai.embeddings.common import (
    ArtifactUnavailableError,
    EmbeddedQuery,
    EmbeddingInputAudit,
)
from groundloop.ai.generation import (
    DeterministicAnswerGenerator,
    QwenAnswerGenerator,
    QwenCompletionBackend,
)
from groundloop.ai.manifest import canonical_manifest_json
from groundloop.ai.persistence import PostgresArtifactStore
from groundloop.ai.pipeline import claim_version_id
from groundloop.ai.verification import (
    DeterministicFakeVerifier,
    PinnedMiniLMVerifier,
)
from groundloop.domain import (
    AnswerStatus,
    ChunkVersion,
    ClaimStatus,
    DecisionPolicy,
    DocumentVersion,
    normalized_text_hash,
)
from groundloop.errors import ValidationError
from groundloop.m4.application import DynamicEventPlan, EventRunResult, EventRunState
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind, stable_m4_digest
from groundloop.m4.m3_activation import (
    M3M4ActivationReceipt,
    M3M4RuntimeComposition,
    PublishedM3ActivationSource,
    activate_published_m3_run,
    compose_activated_m3_runtime,
    inspect_m3_m4_activation,
)
from groundloop.m4.models.config import (
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
)
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import M4BgeRoleAdapter
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter
from groundloop.m4.pipeline import InsertedDocument, StructuralPayload
from groundloop.m4.runtime import RuntimeEpochState
from groundloop.postgres import ServerMetadata, apply_m2_schema, read_server_metadata

SCHEMA_VERSION = "groundloop-fyp-end-to-end-demo-v1"
CONFIG_HASH = hashlib.sha256(b"groundloop-fyp-end-to-end-demo-config-v1").hexdigest()
CALIBRATION_ARTIFACT_SHA256 = hashlib.sha256(
    b"groundloop-fyp-end-to-end-demo-deterministic-calibration-v1"
).hexdigest()

QUESTION = "What does GroundLoop maintain?"
REAL_QUESTION = (
    "What exact guarantee does GroundLoop provide, and what does it not guarantee?"
)
BASE_TEXT = "GroundLoop maintains claim grounding relative to versioned evidence."
AUXILIARY_TEXT = "Auxiliary replacement fallback document 1."
REAL_GUIDE_TEXT = (
    "GroundLoop maintains grounding state for generated answers by decomposing each "
    "answer into required atomic claims. Each claim is linked to versioned evidence "
    "chunks and immutable support, refute, and neutral score observations.\n\n"
    "GroundLoop applies a versioned decision policy to stored scores. Its exact "
    "guarantee is that incremental structured claim and answer state equals full "
    "recomputation over the same stored observations; it does not guarantee that a "
    "neural verifier has discovered objective truth."
)
SUPPORT_TEXT = "Deterministic evidence candidate 7."
REFUTE_TEXT = "Deterministic evidence candidate 5."

LIMITATIONS = (
    "Exactness is relative to stored, versioned model judgments, not objective truth.",
    "The deterministic backend is a system-correctness fixture, not AI-quality "
    "evidence.",
    "The optional real backend is a local pinned-model diagnostic, not an accuracy "
    "result.",
    "This bounded run does not establish arbitrary-corpus support or representative "
    "speedup.",
    "Dashboard, deployment, M5, independent natural-history evaluation, and the full "
    "FYP remain pending.",
)
_LOWER_HEX = frozenset("0123456789abcdef")


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in _LOWER_HEX for character in value)


@dataclass(frozen=True, slots=True)
class FypEndToEndDemoConfig:
    """Inputs for one disposable end-to-end execution."""

    database_url: str
    repo_root: Path
    artifact_root: Path
    backend: str = "deterministic"
    model_config_path: Path | None = None
    lexical_config_path: Path | None = None
    schema_prefix: str = "groundloop_fyp_e2e"
    keep_schema: bool = False

    def __post_init__(self) -> None:
        if not self.database_url.strip():
            raise ArtifactUnavailableError("PostgreSQL database URL is absent")
        if self.backend not in {"deterministic", "real"}:
            raise ValidationError("backend must be deterministic or real")
        if not self.schema_prefix or any(
            not (character.isalnum() or character == "_")
            for character in self.schema_prefix
        ):
            raise ValidationError(
                "schema prefix must contain only letters, digits, and underscores"
            )

    @property
    def resolved_model_config_path(self) -> Path:
        return self.model_config_path or (
            self.repo_root / "configs/m4/models/m3_reuse_v1.json"
        )

    @property
    def resolved_lexical_config_path(self) -> Path:
        return self.lexical_config_path or (
            self.repo_root / "configs/m4/impact/lexical_v1.json"
        )


@dataclass(frozen=True, slots=True)
class ObjectStateEvidence:
    claim_states: tuple[tuple[str, str], ...]
    answer_version_id: str
    answer_status: str


@dataclass(frozen=True, slots=True)
class ReplayEvidence:
    state: str
    epoch_id: int
    publication_id: str | None
    discovery_call_count: int
    embedding_request_count: int
    verifier_request_count: int
    verifier_backend_pair_calls: int
    projection_before_sha256: str
    projection_after_sha256: str
    projection_equal: bool


@dataclass(frozen=True, slots=True)
class EventEvidence:
    event_id: str
    update_kind: str
    payload_hash: str
    previous_published_epoch_id: int
    epoch_id: int
    publication_id: str
    state: str
    discovery_call_count: int
    embedding_request_count: int
    verifier_call_count: int
    verifier_request_count: int
    verifier_backend_pair_calls: int
    observation_artifact_count: int
    effective_observation_count: int
    inactive_completion_count: int
    python_full_recomputation_equal: bool
    sql_full_recomputation_equal: bool
    persisted_state_equal: bool
    sql_claim_mismatch_count: int
    sql_answer_mismatch_count: int
    open_job_count: int
    open_scope_count: int
    states: ObjectStateEvidence
    replay: ReplayEvidence


@dataclass(frozen=True, slots=True)
class M3Evidence:
    manifest_sha256: str
    run_id: str
    semantic_epoch_id: int
    question_id: str
    question_text: str
    answer_version_id: str
    answer_text: str
    answer_cited_chunk_version_ids: tuple[str, ...]
    local_claim_ids: tuple[str, ...]
    global_claim_ids: tuple[str, ...]
    claims: tuple[M3ClaimEvidence, ...]
    chunk_version_ids: tuple[str, ...]
    model_artifact_ids: tuple[str, ...]
    prompt_artifact_ids: tuple[str, ...]
    decision_policy_version: str
    generation_execution_ids: tuple[str, ...]
    extraction_execution_ids: tuple[str, ...]
    retrieval_candidate_ids: tuple[str, ...]
    verification_observation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class M3ClaimEvidence:
    local_claim_id: str
    global_claim_id: str
    text: str
    required: bool
    cited_chunk_version_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FypEndToEndResult:
    schema_version: str
    backend: str
    model_downloads_allowed: bool
    backend_status: str
    schema_name: str
    keep_schema_requested: bool
    schema_removed: bool
    server: ServerMetadata
    m3: M3Evidence
    activation: M3M4ActivationReceipt
    baseline: ObjectStateEvidence
    activation_replay_receipt: M3M4ActivationReceipt
    activation_replay: ReplayEvidence
    events: tuple[EventEvidence, ...]
    m3_provenance_before_sha256: str
    m3_provenance_after_sha256: str
    m3_provenance_equal: bool
    limitations: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], asdict(self))

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"

    @property
    def canonical_sha256(self) -> str:
        encoded = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class _BackendRuntime:
    m3_embedder: StaticEmbeddingProvider
    generator: AnswerGenerator
    extractor: ClaimExtractor
    verifier: EvidenceVerifier
    policy: DecisionPolicy
    config_hash: str
    embedding_spec: EmbeddingAdapterSpec
    verifier_spec: VerificationAdapterSpec
    adapter_factory: Callable[[], tuple[M4BgeRoleAdapter, M4CalibratedVerifierAdapter]]


@dataclass(frozen=True, slots=True)
class _PendingEvent:
    plan: DynamicEventPlan
    payload: StructuralPayload
    result: EventRunResult
    evidence_without_replay: Mapping[str, object]


class _FailOnCallEmbedder:
    def __init__(self, spec: EmbeddingAdapterSpec) -> None:
        self.model_artifact = spec.model_artifact
        self.dimension = spec.dimension
        self.last_audit: tuple[EmbeddingInputAudit, ...] = ()

    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        del chunks
        raise AssertionError("exact replay called the passage embedder")

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        del text, query_kind
        raise AssertionError("exact replay called the query embedder")


class _FailOnCallVerifier:
    def __init__(self, spec: VerificationAdapterSpec) -> None:
        self.model_artifact: ModelArtifact = spec.model_artifact
        self.prompt_artifact: PromptArtifact = spec.prompt_artifact
        self.calibration_version = spec.calibration_version
        self.temperature = spec.temperature
        self.max_length = spec.max_length

    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[VerificationResult, ...]:
        del pairs
        raise AssertionError("exact replay called the verifier backend")


def _psycopg_url(value: str) -> str:
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _set_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )


def _connect(config: FypEndToEndDemoConfig, schema_name: str) -> Connection[Any]:
    connection = psycopg.connect(_psycopg_url(config.database_url), autocommit=True)
    _set_schema(connection, schema_name)
    return connection


def _deterministic_backend() -> _BackendRuntime:
    policy = DecisionPolicy("m3-policy-v1", 0.8, 0.8)
    m3_embedder = DeterministicFakeEmbedder()
    m3_verifier = DeterministicFakeVerifier()
    embedding_spec = EmbeddingAdapterSpec(
        model_artifact=m3_embedder.model_artifact,
        dimension=m3_embedder.dimension,
    )
    verifier_spec = VerificationAdapterSpec(
        model_artifact=m3_verifier.model_artifact,
        prompt_artifact=m3_verifier.prompt_artifact,
        calibration_version=m3_verifier.calibration_version,
        calibration_artifact_sha256=CALIBRATION_ARTIFACT_SHA256,
        temperature=m3_verifier.temperature,
        max_length=m3_verifier.max_length,
        decision_policy=policy,
    )

    def adapter_factory() -> tuple[M4BgeRoleAdapter, M4CalibratedVerifierAdapter]:
        return (
            M4BgeRoleAdapter(DeterministicFakeEmbedder(), embedding_spec),
            M4CalibratedVerifierAdapter(
                DeterministicFakeVerifier(), verifier_spec, batch_size=8
            ),
        )

    return _BackendRuntime(
        m3_embedder=m3_embedder,
        generator=DeterministicAnswerGenerator(),
        extractor=DeterministicClaimExtractor(),
        verifier=m3_verifier,
        policy=policy,
        config_hash=CONFIG_HASH,
        embedding_spec=embedding_spec,
        verifier_spec=verifier_spec,
        adapter_factory=adapter_factory,
    )


def _real_backend(config: FypEndToEndDemoConfig) -> _BackendRuntime:
    reuse = PinnedM3ReuseConfig.load(config.resolved_model_config_path)
    first_bundle = build_pinned_m3_adapters(reuse, artifact_root=config.artifact_root)
    m3_embedder = BgeSmallEmbedder(
        cache_dir=config.artifact_root / reuse.embedding_cache_relative_path,
        allow_download=False,
    )
    m3_verifier = PinnedMiniLMVerifier(
        model_path=str(first_bundle.availability.verifier_checkpoint),
        model_revision=reuse.verifier_revision,
        temperature=reuse.calibration_temperature,
        max_length=reuse.verifier_max_length,
        batch_size=reuse.verifier_batch_size,
        local_files_only=True,
        artifact_sha256=reuse.verifier_checkpoint_tree_sha256,
        logical_model_id=reuse.verifier_logical_model_id,
        calibration_version=reuse.calibration_version,
    )
    qwen = QwenCompletionBackend(allow_download=False)
    config_payload = json.dumps(asdict(reuse), sort_keys=True, separators=(",", ":"))
    config_hash = hashlib.sha256(
        ("groundloop-fyp-e2e-real-v1\0" + config_payload).encode("utf-8")
    ).hexdigest()

    def adapter_factory() -> tuple[M4BgeRoleAdapter, M4CalibratedVerifierAdapter]:
        bundle = build_pinned_m3_adapters(reuse, artifact_root=config.artifact_root)
        return bundle.embeddings, bundle.verifier

    return _BackendRuntime(
        m3_embedder=m3_embedder,
        generator=QwenAnswerGenerator(backend=qwen),
        extractor=QwenClaimExtractor(backend=qwen),
        verifier=m3_verifier,
        policy=reuse.decision_policy,
        config_hash=config_hash,
        embedding_spec=first_bundle.embeddings.spec,
        verifier_spec=first_bundle.verifier.spec,
        adapter_factory=adapter_factory,
    )


def _backend_runtime(config: FypEndToEndDemoConfig) -> _BackendRuntime:
    if config.backend == "deterministic":
        return _deterministic_backend()
    return _real_backend(config)


def _fail_embedding_adapter(spec: EmbeddingAdapterSpec) -> M4BgeRoleAdapter:
    return M4BgeRoleAdapter(_FailOnCallEmbedder(spec), spec)


def _fail_verifier_adapter(
    spec: VerificationAdapterSpec,
) -> M4CalibratedVerifierAdapter:
    return M4CalibratedVerifierAdapter(_FailOnCallVerifier(spec), spec, batch_size=8)


def _projection(connection: Connection[Any]) -> dict[str, tuple[str, ...]]:
    tables = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = current_schema()
            ORDER BY tablename
            """
        ).fetchall()
    )
    return {
        table: tuple(
            str(row[0])
            for row in connection.execute(
                sql.SQL(
                    "SELECT to_jsonb(item)::text FROM {} AS item "
                    "ORDER BY to_jsonb(item)::text"
                ).format(sql.Identifier(table))
            ).fetchall()
        )
        for table in tables
    }


def _mapping_sha256(value: Mapping[str, tuple[str, ...]]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _evidence_int(values: Mapping[str, object], key: str) -> int:
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"event evidence {key} is not an integer")
    return value


def _m3_provenance_projection(
    connection: Connection[Any], *, run_id: str, answer_id: str, epoch_id: int
) -> dict[str, tuple[str, ...]]:
    statements: dict[str, tuple[str, tuple[object, ...]]] = {
        "pipeline_run": (
            "SELECT to_jsonb(item)::text FROM groundloop_pipeline_run AS item "
            "WHERE run_id = %s ORDER BY run_id",
            (run_id,),
        ),
        "answer": (
            "SELECT to_jsonb(item)::text FROM groundloop_answer_version AS item "
            "WHERE answer_version_id = %s ORDER BY answer_version_id",
            (answer_id,),
        ),
        "answer_citations": (
            "SELECT to_jsonb(item)::text FROM groundloop_answer_citation AS item "
            "WHERE answer_version_id = %s ORDER BY citation_ordinal",
            (answer_id,),
        ),
        "generation_execution": (
            "SELECT to_jsonb(item)::text FROM groundloop_generation_execution "
            "AS item WHERE run_id = %s ORDER BY answer_version_id",
            (run_id,),
        ),
        "claims": (
            "SELECT to_jsonb(item)::text FROM groundloop_claim AS item "
            "WHERE answer_version_id = %s ORDER BY claim_id",
            (answer_id,),
        ),
        "extraction_execution": (
            "SELECT to_jsonb(item)::text FROM groundloop_claim_extraction_execution "
            "AS item WHERE run_id = %s ORDER BY claim_id",
            (run_id,),
        ),
        "retrieval_candidates": (
            "SELECT to_jsonb(item)::text FROM groundloop_retrieval_candidate AS item "
            "WHERE run_id = %s ORDER BY candidate_id",
            (run_id,),
        ),
        "verification_executions": (
            "SELECT to_jsonb(item)::text FROM groundloop_verification_execution "
            "AS item WHERE run_id = %s ORDER BY observation_id",
            (run_id,),
        ),
        "original_observations": (
            "SELECT to_jsonb(item)::text FROM groundloop_semantic_observation AS item "
            "WHERE produced_epoch = %s ORDER BY observation_id",
            (epoch_id,),
        ),
    }
    return {
        name: tuple(
            str(row[0]) for row in connection.execute(statement, values).fetchall()
        )
        for name, (statement, values) in statements.items()
    }


def _current_states(
    connection: Connection[Any], source: PublishedM3ActivationSource
) -> ObjectStateEvidence:
    claim_rows = connection.execute(
        """
        SELECT claim_id, status::text
        FROM groundloop_published_claim_state
        WHERE claim_id = ANY(%s) AND valid_to_epoch IS NULL
        ORDER BY claim_id
        """,
        (list(source.claim_ids),),
    ).fetchall()
    answer_row = connection.execute(
        """
        SELECT status::text FROM groundloop_published_answer_state
        WHERE answer_version_id = %s AND valid_to_epoch IS NULL
        """,
        (source.answer_version_id,),
    ).fetchone()
    claim_states = tuple((str(row[0]), str(row[1])) for row in claim_rows)
    if len(claim_states) != len(source.claim_ids) or answer_row is None:
        raise ValidationError("published claim or answer state is incomplete")
    return ObjectStateEvidence(
        claim_states=claim_states,
        answer_version_id=source.answer_version_id,
        answer_status=str(answer_row[0]),
    )


def _sql_oracle_mismatches(
    connection: Connection[Any], epoch_id: int
) -> tuple[int, int]:
    claim_row = connection.execute(
        """
        SELECT count(*) FROM (
            (SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_working_claim_state WHERE epoch_id = %s
             EXCEPT ALL
             SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_claim_state_oracle WHERE epoch_id = %s)
            UNION ALL
            (SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_claim_state_oracle WHERE epoch_id = %s
             EXCEPT ALL
             SELECT claim_id, support_count, refute_count, best_support_score,
                    best_refute_score, supporting_observation_ids,
                    refuting_observation_ids, status::text
             FROM groundloop_m4_working_claim_state WHERE epoch_id = %s)
        ) AS mismatch
        """,
        (epoch_id,) * 4,
    ).fetchone()
    answer_row = connection.execute(
        """
        SELECT count(*) FROM (
            (SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_working_answer_state WHERE epoch_id = %s
             EXCEPT ALL
             SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_answer_state_oracle WHERE epoch_id = %s)
            UNION ALL
            (SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_answer_state_oracle WHERE epoch_id = %s
             EXCEPT ALL
             SELECT answer_version_id, required_claim_count, supported_count,
                    unsupported_count, refuted_count, conflicted_count, status::text
             FROM groundloop_m4_working_answer_state WHERE epoch_id = %s)
        ) AS mismatch
        """,
        (epoch_id,) * 4,
    ).fetchone()
    if claim_row is None or answer_row is None:
        raise ValidationError("SQL oracle comparison returned no count")
    return int(claim_row[0]), int(answer_row[0])


def _inserted_document(
    *, document_id: str, version_namespace: str, text: str
) -> tuple[InsertedDocument, str]:
    chunker = FixedCharChunker()
    content_hash = normalized_text_hash(text)
    version_id = stable_m4_digest(version_namespace, document_id, content_hash)
    drafts = chunker.chunk(version_id, text)
    if len(drafts) != 1:
        raise ValidationError("bounded demo documents must have exactly one chunk")
    draft = drafts[0]
    return (
        InsertedDocument(
            version=DocumentVersion(version_id, document_id, content_hash),
            chunks=(
                ChunkVersion(
                    draft.chunk_version_id,
                    draft.document_version_id,
                    draft.chunk_index,
                    draft.text,
                    draft.text_hash,
                ),
            ),
            source_uri=f"demo://{document_id}",
            authority_class="fyp-end-to-end-demo",
            chunker_version="fixed-char-v1",
            chunker_artifact_id=chunker.artifact_id,
            chunker_input_hash=content_hash,
        ),
        draft.chunk_version_id,
    )


def _compose(
    connection: Connection[Any],
    *,
    config: FypEndToEndDemoConfig,
    run_id: str,
    backend: _BackendRuntime,
    payloads: Mapping[str, StructuralPayload],
    fail_on_call: bool = False,
) -> M3M4RuntimeComposition:
    if fail_on_call:
        embeddings = _fail_embedding_adapter(backend.embedding_spec)
        verifier = _fail_verifier_adapter(backend.verifier_spec)
    else:
        embeddings, verifier = backend.adapter_factory()
    return compose_activated_m3_runtime(
        connection,
        run_id=run_id,
        embedding_adapter=embeddings,
        verifier_adapter=verifier,
        repo_root=config.repo_root,
        structural_payloads=payloads,
        lexical_config_path=config.resolved_lexical_config_path,
    )


def _event_plan(
    *,
    event_id: str,
    kind: UpdateKind,
    previous_epoch_id: int,
    composition: M3M4RuntimeComposition,
    payload: StructuralPayload,
    inserted_chunk_ids: tuple[str, ...] = (),
    deactivated_chunk_ids: tuple[str, ...] = (),
) -> DynamicEventPlan:
    policy = composition.candidate_policy
    update = CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=stable_m4_digest(
            "fyp-end-to-end-demo-event-v1",
            event_id,
            kind.value,
            json.dumps(payload.manifest, sort_keys=True, separators=(",", ":")),
            policy.policy_hash,
            str(previous_epoch_id),
        ),
        update_kind=kind,
        previous_published_epoch_id=previous_epoch_id,
        candidate_policy_id=policy.policy_id,
    )
    return DynamicEventPlan(
        update=update,
        inserted_chunk_version_ids=inserted_chunk_ids,
        deactivated_chunk_version_ids=deactivated_chunk_ids,
        registered_claim_ids=(),
        claim_registry_snapshot_id=policy.claim_registry_snapshot_id,
    )


def _derived_event_id(run_id: str, kind: UpdateKind) -> str:
    return f"fyp-e2e-{kind.value}-" + stable_m4_digest(
        "fyp-end-to-end-demo-event-id-v1", run_id, kind.value
    )


def _activation_identity(receipt: M3M4ActivationReceipt) -> dict[str, object]:
    """Return the invariant identity shared by fresh activation and replay."""
    identity = receipt.to_dict()
    for field in (
        "created_role_artifact_count",
        "reused_role_artifact_count",
        "activation_embedding_request_count",
        "replayed",
    ):
        identity.pop(field)
    return identity


def _nonempty_unique(values: tuple[str, ...]) -> bool:
    return (
        bool(values)
        and all(value.strip() for value in values)
        and len(set(values)) == len(values)
    )


def _event_exactness(
    connection: Connection[Any],
    *,
    composition: M3M4RuntimeComposition,
    source: PublishedM3ActivationSource,
    result: EventRunResult,
) -> dict[str, object]:
    if result.state is not EventRunState.SEALED or result.publication_id is None:
        raise ValidationError(
            f"fresh demo event {result.event_id} did not seal: {result.state.value}"
        )
    composition.ports.audit_grounding_exactness(result.epoch_id)
    claim_mismatches, answer_mismatches = _sql_oracle_mismatches(
        connection, result.epoch_id
    )
    if claim_mismatches or answer_mismatches:
        raise ValidationError("event differs from the independent SQL oracle")
    header = composition.ports.runtime_store.read_epoch_header_point(result.epoch_id)
    if (
        header.state is not RuntimeEpochState.SEALED
        or header.open_job_count
        or header.open_scope_count
    ):
        raise ValidationError("sealed event retains open coordination work")
    epoch_row = connection.execute(
        """
        SELECT structural_status, semantic_status, evaluation_state,
               publication_mode, sealed_at IS NOT NULL
        FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone()
    if epoch_row != ("committed", "sealed", "complete", "strict", True):
        raise ValidationError("event epoch is not a strict complete publication")
    return {
        "python_full_recomputation_equal": True,
        "sql_full_recomputation_equal": True,
        "persisted_state_equal": True,
        "sql_claim_mismatch_count": claim_mismatches,
        "sql_answer_mismatch_count": answer_mismatches,
        "open_job_count": header.open_job_count,
        "open_scope_count": header.open_scope_count,
        "states": _current_states(connection, source),
    }


def _replay_evidence(
    *,
    before: Mapping[str, tuple[str, ...]],
    after: Mapping[str, tuple[str, ...]],
    result: EventRunResult,
    composition: M3M4RuntimeComposition | None,
) -> ReplayEvidence:
    return ReplayEvidence(
        state=result.state.value,
        epoch_id=result.epoch_id,
        publication_id=result.publication_id,
        discovery_call_count=result.discovery_call_count,
        embedding_request_count=(
            0 if composition is None else composition.embeddings.request_count
        ),
        verifier_request_count=(
            0 if composition is None else composition.verifier.request_count
        ),
        verifier_backend_pair_calls=(
            0 if composition is None else composition.verifier.backend_pair_calls
        ),
        projection_before_sha256=_mapping_sha256(before),
        projection_after_sha256=_mapping_sha256(after),
        projection_equal=before == after,
    )


def _m3_evidence(
    manifest: PipelineRunManifest,
    source: PublishedM3ActivationSource,
    *,
    question_text: str,
) -> M3Evidence:
    if (
        manifest.semantic_epoch_id is None
        or manifest.answer_version_id is None
        or manifest.answer is None
    ):
        raise ValidationError("published M3 manifest lacks epoch or answer identity")
    claims = tuple(
        sorted(
            (
                M3ClaimEvidence(
                    local_claim_id=item.local_claim_id,
                    global_claim_id=claim_version_id(manifest.answer_version_id, item),
                    text=item.text,
                    required=item.required,
                    cited_chunk_version_ids=item.cited_chunk_version_ids,
                )
                for item in manifest.claims
            ),
            key=lambda item: item.global_claim_id,
        )
    )
    return M3Evidence(
        manifest_sha256=hashlib.sha256(
            canonical_manifest_json(manifest).encode("utf-8")
        ).hexdigest(),
        run_id=manifest.run_id,
        semantic_epoch_id=manifest.semantic_epoch_id,
        question_id=manifest.question_id,
        question_text=question_text,
        answer_version_id=manifest.answer_version_id,
        answer_text=manifest.answer.text,
        answer_cited_chunk_version_ids=manifest.answer.cited_chunk_version_ids,
        local_claim_ids=tuple(item.local_claim_id for item in manifest.claims),
        global_claim_ids=source.claim_ids,
        claims=claims,
        chunk_version_ids=source.chunk_ids,
        model_artifact_ids=manifest.model_artifact_ids,
        prompt_artifact_ids=manifest.prompt_artifact_ids,
        decision_policy_version=manifest.decision_policy_version,
        generation_execution_ids=(manifest.answer_version_id,),
        extraction_execution_ids=source.claim_ids,
        retrieval_candidate_ids=tuple(
            item.candidate_id for item in manifest.retrieval_candidates
        ),
        verification_observation_ids=source.observation_ids,
    )


def _publish_m3(
    connection: Connection[Any],
    corpus: Path,
    backend: _BackendRuntime,
    *,
    question: str,
) -> PipelineRunManifest:
    top_k = 1 if isinstance(backend.m3_embedder, DeterministicFakeEmbedder) else 2
    application = M3Application(
        store=PostgresArtifactStore(connection),
        chunker=FixedCharChunker(),
        embedder=backend.m3_embedder,
        generator=backend.generator,
        extractor=backend.extractor,
        verifier=backend.verifier,
        config=M3ApplicationConfig(
            config_hash=backend.config_hash,
            question_top_k=top_k,
            claim_top_k=top_k,
            policy=backend.policy,
        ),
    )
    return application.register(corpus, question)


def run_fyp_end_to_end_demo(config: FypEndToEndDemoConfig) -> FypEndToEndResult:
    """Execute and validate the bounded same-schema M3-to-M4 history."""
    schema_name = f"{config.schema_prefix}_{uuid.uuid4().hex}"
    backend = _backend_runtime(config)
    created = False
    provisional_result: FypEndToEndResult | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="groundloop-fyp-e2e-") as temp_dir:
            corpus = Path(temp_dir) / "corpus"
            corpus.mkdir()
            if config.backend == "deterministic":
                (corpus / "groundloop.txt").write_text(
                    BASE_TEXT + "\n", encoding="utf-8"
                )
                (corpus / "auxiliary.txt").write_text(
                    AUXILIARY_TEXT + "\n", encoding="utf-8"
                )
                question = QUESTION
            else:
                (corpus / "guide.txt").write_text(
                    REAL_GUIDE_TEXT + "\n", encoding="utf-8"
                )
                (corpus / "unrelated.txt").write_text(
                    AUXILIARY_TEXT + "\n", encoding="utf-8"
                )
                question = REAL_QUESTION
            with psycopg.connect(
                _psycopg_url(config.database_url), autocommit=True
            ) as connection:
                connection.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
                )
                created = True
                _set_schema(connection, schema_name)
                with connection.transaction():
                    apply_m2_schema(connection)
                server = read_server_metadata(connection)
                manifest = _publish_m3(connection, corpus, backend, question=question)
                inspection = inspect_m3_m4_activation(
                    connection, run_id=manifest.run_id
                )
                if inspection.replay_receipt is not None:
                    raise ValidationError("fresh M3 run unexpectedly has M4 history")
                source = inspection.source
                provenance_before = _m3_provenance_projection(
                    connection,
                    run_id=manifest.run_id,
                    answer_id=source.answer_version_id,
                    epoch_id=source.epoch_id,
                )
                activation_embeddings, _ = backend.adapter_factory()
                activation = activate_published_m3_run(
                    connection,
                    run_id=manifest.run_id,
                    embeddings=activation_embeddings,
                    verifier_spec=backend.verifier_spec,
                    repo_root=config.repo_root,
                    lexical_config_path=config.resolved_lexical_config_path,
                )
                if activation.replayed:
                    raise ValidationError("first activation unexpectedly replayed")
                baseline = _current_states(connection, source)
                activation_projection = _projection(connection)

            with _connect(config, schema_name) as replay_connection:
                activation_replay_receipt = activate_published_m3_run(
                    replay_connection,
                    run_id=manifest.run_id,
                    embeddings=_fail_embedding_adapter(backend.embedding_spec),
                    verifier_spec=backend.verifier_spec,
                    repo_root=config.repo_root,
                    lexical_config_path=config.resolved_lexical_config_path,
                )
                activation_after = _projection(replay_connection)
            if not activation_replay_receipt.replayed:
                raise ValidationError("activation replay did not reuse the baseline")
            activation_replay_result = EventRunResult(
                event_id="m3-activation",
                epoch_id=activation_replay_receipt.base_epoch_id,
                state=EventRunState.REPLAYED,
                publication_id=None,
                discovery_call_count=0,
                verifier_call_count=0,
                observation_artifact_count=0,
                effective_observation_count=0,
                inactive_completion_count=0,
            )
            activation_replay = _replay_evidence(
                before=activation_projection,
                after=activation_after,
                result=activation_replay_result,
                composition=None,
            )
            if not activation_replay.projection_equal:
                raise ValidationError("activation replay changed persisted rows")

            inserted, inserted_chunk_id = _inserted_document(
                document_id="fyp-e2e-support-document",
                version_namespace="fyp-e2e-support-version-v1",
                text=SUPPORT_TEXT,
            )
            replacement, replacement_chunk_id = _inserted_document(
                document_id=inserted.version.document_id,
                version_namespace="fyp-e2e-refuting-version-v1",
                text=REFUTE_TEXT,
            )
            if config.backend == "deterministic":
                try:
                    base_chunk = next(
                        item for item in source.chunks if item.text == BASE_TEXT
                    )
                except StopIteration as error:
                    raise ValidationError(
                        "published M3 run lacks the controlled base chunk"
                    ) from error
            else:
                try:
                    base_chunk = next(
                        item for item in source.chunks if item.text == AUXILIARY_TEXT
                    )
                except StopIteration as error:
                    raise ValidationError(
                        "published M3 run lacks the unrelated deletion chunk"
                    ) from error
            insert_event_id = _derived_event_id(manifest.run_id, UpdateKind.INSERT)
            delete_event_id = _derived_event_id(manifest.run_id, UpdateKind.DELETE)
            replace_event_id = _derived_event_id(manifest.run_id, UpdateKind.REPLACE)
            payloads = {
                insert_event_id: StructuralPayload(inserted=inserted),
                delete_event_id: StructuralPayload(
                    deactivated_document_version_id=base_chunk.document_version_id
                ),
                replace_event_id: StructuralPayload(
                    inserted=replacement,
                    deactivated_document_version_id=(
                        inserted.version.document_version_id
                    ),
                ),
            }
            specs = (
                (
                    insert_event_id,
                    UpdateKind.INSERT,
                    (inserted_chunk_id,),
                    (),
                ),
                (
                    delete_event_id,
                    UpdateKind.DELETE,
                    (),
                    (base_chunk.chunk_version_id,),
                ),
                (
                    replace_event_id,
                    UpdateKind.REPLACE,
                    (replacement_chunk_id,),
                    (inserted_chunk_id,),
                ),
            )
            pending: list[_PendingEvent] = []
            previous_epoch_id = activation.base_epoch_id
            for event_id, kind, inserted_ids, deactivated_ids in specs:
                with _connect(config, schema_name) as event_connection:
                    composition = _compose(
                        event_connection,
                        config=config,
                        run_id=manifest.run_id,
                        backend=backend,
                        payloads=payloads,
                    )
                    plan = _event_plan(
                        event_id=event_id,
                        kind=kind,
                        previous_epoch_id=previous_epoch_id,
                        composition=composition,
                        payload=payloads[event_id],
                        inserted_chunk_ids=inserted_ids,
                        deactivated_chunk_ids=deactivated_ids,
                    )
                    event_result = composition.application.run_event(plan)
                    exactness = _event_exactness(
                        event_connection,
                        composition=composition,
                        source=source,
                        result=event_result,
                    )
                    if event_result.discovery_call_count < 1:
                        raise ValidationError("fresh event skipped impact discovery")
                    if kind is UpdateKind.DELETE and (
                        composition.embeddings.request_count
                        or event_result.verifier_call_count
                        or composition.verifier.request_count
                        or composition.verifier.backend_pair_calls
                    ):
                        raise ValidationError("delete performed forbidden model work")
                    pending.append(
                        _PendingEvent(
                            plan=plan,
                            payload=payloads[event_id],
                            result=event_result,
                            evidence_without_replay={
                                "embedding_request_count": (
                                    composition.embeddings.request_count
                                ),
                                "verifier_request_count": (
                                    composition.verifier.request_count
                                ),
                                "verifier_backend_pair_calls": (
                                    composition.verifier.backend_pair_calls
                                ),
                                **exactness,
                            },
                        )
                    )
                    previous_epoch_id = event_result.epoch_id

            events: list[EventEvidence] = []
            for event in pending:
                with _connect(config, schema_name) as replay_connection:
                    before = _projection(replay_connection)
                    replay_composition = _compose(
                        replay_connection,
                        config=config,
                        run_id=manifest.run_id,
                        backend=backend,
                        payloads={event.plan.update.event_id: event.payload},
                        fail_on_call=True,
                    )
                    replay_result = replay_composition.application.run_event(event.plan)
                    after = _projection(replay_connection)
                    replay = _replay_evidence(
                        before=before,
                        after=after,
                        result=replay_result,
                        composition=replay_composition,
                    )
                if replay_result.state is not EventRunState.REPLAYED:
                    raise ValidationError("event replay did not return REPLAYED")
                if (
                    replay_result.epoch_id != event.result.epoch_id
                    or replay_result.publication_id != event.result.publication_id
                    or replay.discovery_call_count
                    or replay.embedding_request_count
                    or replay.verifier_request_count
                    or replay.verifier_backend_pair_calls
                    or not replay.projection_equal
                ):
                    raise ValidationError("event replay was not exact and work-free")
                values = event.evidence_without_replay
                publication_id = event.result.publication_id
                if publication_id is None:
                    raise ValidationError("sealed event lacks publication identity")
                previous_published_epoch_id = (
                    event.plan.update.previous_published_epoch_id
                )
                if previous_published_epoch_id is None:
                    raise ValidationError("event lacks its previous published epoch")
                events.append(
                    EventEvidence(
                        event_id=event.plan.update.event_id,
                        update_kind=event.plan.update.update_kind.value,
                        payload_hash=event.plan.update.payload_hash,
                        previous_published_epoch_id=previous_published_epoch_id,
                        epoch_id=event.result.epoch_id,
                        publication_id=publication_id,
                        state=event.result.state.value,
                        discovery_call_count=event.result.discovery_call_count,
                        embedding_request_count=_evidence_int(
                            values, "embedding_request_count"
                        ),
                        verifier_call_count=event.result.verifier_call_count,
                        verifier_request_count=_evidence_int(
                            values, "verifier_request_count"
                        ),
                        verifier_backend_pair_calls=_evidence_int(
                            values, "verifier_backend_pair_calls"
                        ),
                        observation_artifact_count=(
                            event.result.observation_artifact_count
                        ),
                        effective_observation_count=(
                            event.result.effective_observation_count
                        ),
                        inactive_completion_count=(
                            event.result.inactive_completion_count
                        ),
                        python_full_recomputation_equal=bool(
                            values["python_full_recomputation_equal"]
                        ),
                        sql_full_recomputation_equal=bool(
                            values["sql_full_recomputation_equal"]
                        ),
                        persisted_state_equal=bool(values["persisted_state_equal"]),
                        sql_claim_mismatch_count=_evidence_int(
                            values, "sql_claim_mismatch_count"
                        ),
                        sql_answer_mismatch_count=_evidence_int(
                            values, "sql_answer_mismatch_count"
                        ),
                        open_job_count=_evidence_int(values, "open_job_count"),
                        open_scope_count=_evidence_int(values, "open_scope_count"),
                        states=cast(ObjectStateEvidence, values["states"]),
                        replay=replay,
                    )
                )

            with _connect(config, schema_name) as final_connection:
                provenance_after = _m3_provenance_projection(
                    final_connection,
                    run_id=manifest.run_id,
                    answer_id=source.answer_version_id,
                    epoch_id=source.epoch_id,
                )
            provisional_result = FypEndToEndResult(
                schema_version=SCHEMA_VERSION,
                backend=config.backend,
                model_downloads_allowed=False,
                backend_status="executed",
                schema_name=schema_name,
                keep_schema_requested=config.keep_schema,
                schema_removed=False,
                server=server,
                m3=_m3_evidence(
                    manifest,
                    source,
                    question_text=question,
                ),
                activation=activation,
                baseline=baseline,
                activation_replay_receipt=activation_replay_receipt,
                activation_replay=activation_replay,
                events=tuple(events),
                m3_provenance_before_sha256=_mapping_sha256(provenance_before),
                m3_provenance_after_sha256=_mapping_sha256(provenance_after),
                m3_provenance_equal=provenance_before == provenance_after,
                limitations=LIMITATIONS,
            )
            # Materialize every database-backed field while the disposable
            # schema is still queryable. Cleanup evidence is finalized below.
            provisional_result.to_dict()
    finally:
        if created and not config.keep_schema:
            with psycopg.connect(
                _psycopg_url(config.database_url), autocommit=True
            ) as cleanup:
                cleanup.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema_name)
                    )
                )

    if provisional_result is None:
        raise ValidationError("end-to-end demo produced no result")
    demo_result = replace(
        provisional_result,
        schema_removed=not config.keep_schema,
    )
    validate_fyp_end_to_end_result(demo_result)
    return demo_result


def validate_fyp_end_to_end_result(result: FypEndToEndResult) -> None:
    """Fail closed over the canonical result before reporting PASS."""
    if result.schema_version != SCHEMA_VERSION:
        raise ValidationError("unexpected end-to-end result schema")
    if result.backend not in {"deterministic", "real"}:
        raise ValidationError("unexpected result backend")
    if result.model_downloads_allowed:
        raise ValidationError("end-to-end demo must disable model downloads")
    if result.backend_status != "executed":
        raise ValidationError("backend did not execute")
    if not result.schema_name.strip():
        raise ValidationError("result lacks a disposable schema identity")
    if result.schema_removed != (not result.keep_schema_requested):
        raise ValidationError("schema cleanup evidence contradicts the request")
    if (
        not result.server.postgres_version.strip()
        or result.server.postgres_version_num <= 0
        or result.server.pgvector_available_version is None
        or not result.server.pgvector_available_version.strip()
        or result.server.pgvector_installed_version is None
        or not result.server.pgvector_installed_version.strip()
    ):
        raise ValidationError("result lacks the installed pgvector identity")
    m3 = result.m3
    normalized_question = " ".join(m3.question_text.split())
    expected_question_id = "question-" + stable_digest(
        "m3-question-v1", normalized_question
    )
    identity_collections = (
        m3.local_claim_ids,
        m3.global_claim_ids,
        m3.chunk_version_ids,
        m3.model_artifact_ids,
        m3.prompt_artifact_ids,
        m3.generation_execution_ids,
        m3.extraction_execution_ids,
        m3.retrieval_candidate_ids,
        m3.verification_observation_ids,
    )
    if (
        not m3.run_id.strip()
        or m3.semantic_epoch_id <= 0
        or not normalized_question
        or m3.question_id != expected_question_id
        or not m3.answer_version_id.strip()
        or not m3.decision_policy_version.strip()
        or any(not _nonempty_unique(values) for values in identity_collections)
        or m3.generation_execution_ids != (m3.answer_version_id,)
        or m3.extraction_execution_ids != m3.global_claim_ids
    ):
        raise ValidationError("result lacks derived M3 claim or chunk identities")
    ordered_claims = tuple(sorted(m3.claims, key=lambda item: item.global_claim_id))
    if (
        not m3.answer_text.strip()
        or not m3.answer_cited_chunk_version_ids
        or not ordered_claims
        or any(
            not item.local_claim_id.strip()
            or not item.global_claim_id.strip()
            or not item.text.strip()
            or not item.cited_chunk_version_ids
            or len(set(item.cited_chunk_version_ids))
            != len(item.cited_chunk_version_ids)
            or not set(item.cited_chunk_version_ids).issubset(m3.chunk_version_ids)
            or claim_version_id(
                m3.answer_version_id,
                AtomicClaim(
                    item.local_claim_id,
                    item.text,
                    item.required,
                    item.cited_chunk_version_ids,
                ),
            )
            != item.global_claim_id
            for item in ordered_claims
        )
        or len(set(m3.answer_cited_chunk_version_ids))
        != len(m3.answer_cited_chunk_version_ids)
        or not set(m3.answer_cited_chunk_version_ids).issubset(m3.chunk_version_ids)
        or tuple(item.global_claim_id for item in ordered_claims) != m3.global_claim_ids
        or tuple(sorted(item.local_claim_id for item in ordered_claims))
        != tuple(sorted(m3.local_claim_ids))
    ):
        raise ValidationError("result lacks complete generated M3 content evidence")
    activation = result.activation
    activation_count_fields = (
        activation.claim_count,
        activation.chunk_count,
        activation.observation_count,
        activation.role_artifact_count,
        activation.claim_index_count,
        activation.created_role_artifact_count,
        activation.reused_role_artifact_count,
        activation.activation_embedding_request_count,
        activation.baseline_python_mismatch_count,
        activation.baseline_sql_claim_mismatch_count,
        activation.baseline_sql_answer_mismatch_count,
    )
    if (
        activation.schema_version != "groundloop-m3-m4-activation-v1"
        or activation.run_id != m3.run_id
        or activation.base_epoch_id != m3.semantic_epoch_id
        or activation.answer_version_id != m3.answer_version_id
        or activation.claim_ids != m3.global_claim_ids
        or activation.chunk_ids != m3.chunk_version_ids
        or activation.replayed
        or any(value < 0 for value in activation_count_fields)
        or activation.claim_count != len(m3.global_claim_ids)
        or activation.chunk_count != len(m3.chunk_version_ids)
        or activation.observation_count != len(m3.verification_observation_ids)
        or activation.claim_index_count != activation.claim_count
        or activation.role_artifact_count <= 0
        or activation.created_role_artifact_count
        + activation.reused_role_artifact_count
        != activation.role_artifact_count
        or activation.embedding_model_artifact_id not in m3.model_artifact_ids
        or activation.verifier_model_artifact_id not in m3.model_artifact_ids
        or activation.verifier_prompt_artifact_id not in m3.prompt_artifact_ids
        or activation.decision_policy_version != m3.decision_policy_version
        or not activation.claim_registry_snapshot_id.strip()
        or not activation.candidate_policy_id.strip()
        or not activation.calibration_version.strip()
        or not math.isfinite(activation.calibration_temperature)
        or activation.calibration_temperature <= 0
        or not activation.vector_method_version.strip()
        or not activation.vector_index_kind.strip()
        or not activation.lexical_method_version.strip()
        or not _is_sha256(activation.candidate_policy_hash)
        or not _is_sha256(activation.verifier_execution_spec_hash)
        or not _is_sha256(activation.vector_index_build_config_hash)
        or not _is_sha256(activation.vector_search_config_hash)
        or not _is_sha256(activation.lexical_config_hash)
        or activation.baseline_python_mismatch_count
        or activation.baseline_sql_claim_mismatch_count
        or activation.baseline_sql_answer_mismatch_count
        or not activation.global_closure_valid
    ):
        raise ValidationError("activation evidence does not match the published M3 run")
    activation_replay_receipt = result.activation_replay_receipt
    if (
        not activation_replay_receipt.replayed
        or _activation_identity(activation_replay_receipt)
        != _activation_identity(activation)
        or activation_replay_receipt.created_role_artifact_count
        or activation_replay_receipt.reused_role_artifact_count
        != activation_replay_receipt.role_artifact_count
        or activation_replay_receipt.activation_embedding_request_count
    ):
        raise ValidationError("activation replay receipt does not match activation")
    if (
        not _is_sha256(m3.manifest_sha256)
        or not _is_sha256(result.activation_replay.projection_before_sha256)
        or result.activation_replay.projection_before_sha256
        != result.activation_replay.projection_after_sha256
        or result.activation_replay.state != EventRunState.REPLAYED.value
        or result.activation_replay.epoch_id != activation.base_epoch_id
        or result.activation_replay.publication_id is not None
        or result.activation_replay.discovery_call_count
        or result.activation_replay.embedding_request_count
        or result.activation_replay.verifier_request_count
        or result.activation_replay.verifier_backend_pair_calls
        or not result.activation_replay.projection_equal
    ):
        raise ValidationError("activation replay evidence is not work-free and exact")
    if tuple(event.update_kind for event in result.events) != (
        UpdateKind.INSERT.value,
        UpdateKind.DELETE.value,
        UpdateKind.REPLACE.value,
    ):
        raise ValidationError("result does not contain the required event history")
    previous_epoch = activation.base_epoch_id
    expected_claim_ids = m3.global_claim_ids
    valid_claim_statuses = frozenset(item.value for item in ClaimStatus)
    valid_answer_statuses = frozenset(item.value for item in AnswerStatus)
    state_evidence = (result.baseline,) + tuple(event.states for event in result.events)
    if any(
        tuple(claim_id for claim_id, _ in state.claim_states) != expected_claim_ids
        or state.answer_version_id != m3.answer_version_id
        or state.answer_status not in valid_answer_statuses
        or any(status not in valid_claim_statuses for _, status in state.claim_states)
        for state in state_evidence
    ):
        raise ValidationError("published states do not match the M3 object identities")
    for event, expected_kind in zip(
        result.events,
        (UpdateKind.INSERT, UpdateKind.DELETE, UpdateKind.REPLACE),
        strict=True,
    ):
        fresh_counts = (
            event.discovery_call_count,
            event.embedding_request_count,
            event.verifier_call_count,
            event.verifier_request_count,
            event.verifier_backend_pair_calls,
            event.observation_artifact_count,
            event.effective_observation_count,
            event.inactive_completion_count,
            event.sql_claim_mismatch_count,
            event.sql_answer_mismatch_count,
            event.open_job_count,
            event.open_scope_count,
        )
        if (
            event.event_id != _derived_event_id(m3.run_id, expected_kind)
            or event.previous_published_epoch_id != previous_epoch
            or event.epoch_id <= previous_epoch
            or not _is_sha256(event.payload_hash)
        ):
            raise ValidationError("event history is not a contiguous published chain")
        previous_epoch = event.epoch_id
        if (
            event.state != EventRunState.SEALED.value
            or any(value < 0 for value in fresh_counts)
            or event.discovery_call_count < 1
            or event.verifier_call_count != event.verifier_request_count
            or event.verifier_call_count != event.verifier_backend_pair_calls
            or event.publication_id
            != stable_m4_digest("m4-publication-v1", str(event.epoch_id))
            or not event.python_full_recomputation_equal
            or not event.sql_full_recomputation_equal
            or not event.persisted_state_equal
            or event.sql_claim_mismatch_count
            or event.sql_answer_mismatch_count
            or event.open_job_count
            or event.open_scope_count
        ):
            raise ValidationError("fresh event lacks complete exactness evidence")
        replay = event.replay
        replay_counts = (
            replay.discovery_call_count,
            replay.embedding_request_count,
            replay.verifier_request_count,
            replay.verifier_backend_pair_calls,
        )
        if (
            replay.state != EventRunState.REPLAYED.value
            or replay.epoch_id != event.epoch_id
            or replay.publication_id != event.publication_id
            or any(value < 0 for value in replay_counts)
            or replay.discovery_call_count
            or replay.embedding_request_count
            or replay.verifier_request_count
            or replay.verifier_backend_pair_calls
            or not _is_sha256(replay.projection_before_sha256)
            or replay.projection_before_sha256 != replay.projection_after_sha256
            or not replay.projection_equal
        ):
            raise ValidationError("event replay evidence is not work-free and exact")
    delete = result.events[1]
    if (
        delete.embedding_request_count
        or delete.verifier_call_count
        or delete.verifier_request_count
        or delete.verifier_backend_pair_calls
    ):
        raise ValidationError("delete event performed model work")
    if (
        not result.m3_provenance_equal
        or not _is_sha256(result.m3_provenance_before_sha256)
        or result.m3_provenance_before_sha256 != result.m3_provenance_after_sha256
    ):
        raise ValidationError("M4 history changed immutable M3 provenance")
    if result.limitations != LIMITATIONS:
        raise ValidationError("result limitations are missing or weakened")
    if result.backend == "deterministic":
        if len(result.m3.global_claim_ids) != 1:
            raise ValidationError("deterministic demo must derive exactly one claim")
        expected = (
            (ClaimStatus.UNSUPPORTED.value, AnswerStatus.UNSUPPORTED.value),
            (ClaimStatus.SUPPORTED.value, AnswerStatus.VALID.value),
            (ClaimStatus.SUPPORTED.value, AnswerStatus.VALID.value),
            (ClaimStatus.REFUTED.value, AnswerStatus.CONTRADICTED.value),
        )
        observed_states = (result.baseline,) + tuple(
            event.states for event in result.events
        )
        observed = tuple(
            (state.claim_states[0][1], state.answer_status)
            for state in observed_states
            if len(state.claim_states) == 1
        )
        if observed != expected:
            raise ValidationError("deterministic semantic trajectory changed")
        expected_calls = ((1, 1), (0, 0), (1, 1))
        observed_calls = tuple(
            (event.embedding_request_count, event.verifier_call_count)
            for event in result.events
        )
        if observed_calls != expected_calls:
            raise ValidationError("deterministic event model-call counts changed")


def write_fyp_end_to_end_result(result: FypEndToEndResult, output: Path) -> None:
    validate_fyp_end_to_end_result(result)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(result.to_json(), encoding="utf-8")


def format_fyp_end_to_end_summary(result: FypEndToEndResult, *, output: Path) -> str:
    validate_fyp_end_to_end_result(result)
    lines = [
        "GroundLoop FYP end-to-end demo: PASS",
        f"Backend: {result.backend} (downloads disabled)",
        (
            f"M3: run={result.m3.run_id} answer={result.m3.answer_version_id} "
            f"claims={len(result.m3.global_claim_ids)}"
        ),
        f"Question: {result.m3.question_text}",
        f"Generated answer: {result.m3.answer_text}",
        "Derived claims:",
    ]
    lines.extend(f"  {item.global_claim_id}: {item.text}" for item in result.m3.claims)
    lines.append(
        "Activation: "
        f"epoch={result.activation.base_epoch_id} "
        f"embedding_requests={result.activation.activation_embedding_request_count}"
    )
    for event in result.events:
        claim_statuses = ",".join(status for _, status in event.states.claim_states)
        lines.append(
            f"{event.update_kind.upper()}: epoch={event.epoch_id} "
            f"claims={claim_statuses} answer={event.states.answer_status} "
            f"model_work={event.embedding_request_count}/{event.verifier_call_count} "
            "exact=yes replay_zero_work=yes"
        )
    lines.extend(
        (
            f"M3 provenance unchanged: {str(result.m3_provenance_equal).lower()}",
            f"Disposable schema removed: {str(result.schema_removed).lower()}",
            f"Canonical manifest SHA-256: {result.canonical_sha256}",
            f"Machine result: {output.resolve()}",
            "Boundary: exact relative to stored model judgments; AI quality is not "
            "claimed.",
        )
    )
    return "\n".join(lines)
