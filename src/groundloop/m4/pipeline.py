"""Live PostgreSQL composition for the deterministic M4 vertical slice.

This module is deliberately a composition layer.  The pure runtime remains
the coordination oracle, :class:`IncrementalMaintenanceEngine` remains the
production structured-maintenance algorithm, and the SQL M4 oracle remains an
independent full recomputation used only as an equality gate.

Model calls and admission execute outside transactions.  Structural opening,
verifier completion, and publication each enter PostgreSQL through one
all-or-nothing transaction boundary.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, cast

from psycopg import Connection, Cursor

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    AnswerVersion,
    ChunkVersion,
    Claim,
    ClaimState,
    ClaimStatus,
    DecisionPolicy,
    DocumentVersion,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import (
    EventConflictError,
    InvalidEventError,
    ValidationError,
)
from groundloop.events import (
    DeleteDocumentVersionEvent,
    Event,
    InsertDocumentEvent,
    ObserveEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.incremental import (
    IncrementalMaintenanceEngine,
    IncrementalStatePatch,
)
from groundloop.m4.application import (
    DiscoveryResult,
    DynamicEventPlan,
    JobLease,
    ObservationCompletionReceipt,
    OpenEventReceipt,
    PublicationReceipt,
    SealingSnapshot,
    StructuralWithdrawal,
)
from groundloop.m4.contracts import (
    AdmittedPair,
    ChannelHit,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.evaluation_overlay import (
    ClaimJobDelta,
    EvaluationLifecycle,
    EvaluationTransition,
    EvaluationTransitionKind,
    PostgresEvaluationOverlayStore,
)
from groundloop.m4.models.contracts import PairVerificationInput
from groundloop.m4.models.ports import M4VerificationApplicationPort
from groundloop.m4.persistence import (
    _RUNTIME_MANIFEST_KEY,
    PointAttemptRecord,
    PointEpochHeader,
    PostgresM4RuntimeStore,
    _TypedFailurePointJobImage,
)
from groundloop.m4.runtime import (
    CandidateDependency,
    CompletionPlan,
    ObservationDependency,
    ReverseDependencyIndex,
    RuntimeEpochState,
    answer_evaluation_state,
    claim_evaluation_state,
    plan_withdrawal,
)
from groundloop.reference import compute_all_states
from groundloop.repository import InMemoryRepository

SqlExecutor = Connection[Any] | Cursor[Any]


def _length_frame_direct_declaration(value: bytes) -> bytes:
    return len(value).to_bytes(8, byteorder="big", signed=False) + value


def _canonical_direct_declaration_scalar(value: object) -> bytes:
    if value is None:
        return b"n"
    if isinstance(value, bool):
        return b"b1" if value else b"b0"
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii")
    if isinstance(value, str):
        return b"s" + value.encode("utf-8")
    raise ValidationError(
        f"unsupported typed-direct declaration scalar: {type(value)!r}"
    )


def _require_autocommit(connection: Connection[Any]) -> None:
    """Prevent implicit transactions from spanning external model work."""
    if not connection.autocommit:
        raise ValidationError(
            "M4 application composition requires a psycopg autocommit connection; "
            "its atomic writes use explicit transaction blocks"
        )


def _certificate_digest(claim_id: str, state: ClaimState) -> str:
    return stable_m4_digest(
        "m4-claim-certificate-v1",
        claim_id,
        state.supporting_observation_ids[0] if state.supporting_observation_ids else "",
        state.refuting_observation_ids[0] if state.refuting_observation_ids else "",
    )


class M4ExecutionMode(StrEnum):
    """Choose inline differential auditing or physically measured execution."""

    AUDIT = "audit"
    MEASURED = "measured"


@dataclass(frozen=True, slots=True)
class _TypedDirectEpochFailureJobImage:
    """Exact pre-write M4 job/attempt image for typed-M5 epoch failure."""

    job: LogicalJobSpec
    state: JobState
    completed_revision: int | None
    completion_digest: str | None
    latest_attempt: PointAttemptRecord | None


@dataclass(frozen=True, slots=True)
class _TypedDirectEpochFailureJobLocks:
    epoch_id: int
    expected_revision: int
    jobs: tuple[_TypedDirectEpochFailureJobImage, ...]


@dataclass(frozen=True, slots=True)
class _TypedDirectEpochFailureLockedPlan:
    job_locks: _TypedDirectEpochFailureJobLocks
    target_job: LogicalJobSpec | None
    target_attempt: JobAttempt | None
    cancelled_jobs: tuple[LogicalJobSpec, ...]


@dataclass(frozen=True, slots=True)
class _TypedDirectEpochFailureStage:
    target_job: LogicalJobSpec | None
    target_attempt: JobAttempt | None
    cancelled_jobs: tuple[LogicalJobSpec, ...]
    resulting_revision: int


def _canonical_typed_failure_authority_value(value: object) -> object:
    """Freeze one exact M4 failure-plan value into primitive immutable bytes."""

    concrete = type(value)
    if value is None:
        return ("none",)
    if concrete is str:
        return ("str", value)
    if concrete is int:
        return ("int", value)
    if concrete is bool:
        return ("bool", value)
    if concrete is datetime:
        timestamp = cast(datetime, value)
        return (
            "datetime",
            timestamp.isoformat(timespec="microseconds"),
            timestamp.fold,
        )
    if concrete is tuple:
        return (
            "tuple",
            tuple(
                _canonical_typed_failure_authority_value(item)
                for item in cast(tuple[object, ...], value)
            ),
        )
    if concrete in {JobKind, JobState}:
        return (concrete.__qualname__, cast(JobKind | JobState, value).value)
    if concrete not in {
        _TypedDirectEpochFailureJobImage,
        _TypedDirectEpochFailureJobLocks,
        _TypedDirectEpochFailureLockedPlan,
        LogicalJobSpec,
        PairKey,
        PointAttemptRecord,
        JobAttempt,
    }:
        raise ValidationError(
            "typed failure authority contains an inexact nested value"
        )
    return (
        concrete.__qualname__,
        tuple(
            (
                descriptor.name,
                _canonical_typed_failure_authority_value(
                    getattr(value, descriptor.name)
                ),
            )
            for descriptor in fields(cast(Any, value))
        ),
    )


@dataclass(frozen=True, slots=True)
class InsertedDocument:
    """One immutable document version installed by an M4 event."""

    version: DocumentVersion
    chunks: tuple[ChunkVersion, ...]
    source_uri: str | None = None
    authority_class: str = "dynamic"
    chunker_version: str = "fixed-char-v1"
    chunker_artifact_id: str | None = None
    chunker_input_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.chunks:
            raise ValidationError("an inserted document version requires chunks")
        ids = tuple(chunk.chunk_version_id for chunk in self.chunks)
        indexes = tuple(chunk.chunk_index for chunk in self.chunks)
        if ids != tuple(sorted(set(ids))):
            raise ValidationError("inserted chunk IDs must be sorted and unique")
        if len(set(indexes)) != len(indexes):
            raise ValidationError("inserted chunk indexes must be unique")
        if any(
            chunk.document_version_id != self.version.document_version_id
            for chunk in self.chunks
        ):
            raise ValidationError("inserted chunks name another document version")
        if not self.authority_class.strip() or not self.chunker_version.strip():
            raise ValidationError("document authority and chunker must be non-empty")
        if (self.chunker_artifact_id is None) != (self.chunker_input_hash is None):
            raise ValidationError(
                "chunker artifact identity and input hash must be supplied together"
            )
        if self.chunker_input_hash is not None and len(self.chunker_input_hash) != 64:
            raise ValidationError("chunker input hash must be SHA-256")


@dataclass(frozen=True, slots=True)
class StructuralPayload:
    """Database content bound to one :class:`DynamicEventPlan`."""

    inserted: InsertedDocument | None = None
    deactivated_document_version_id: str | None = None

    @property
    def inserted_chunk_ids(self) -> tuple[str, ...]:
        return (
            ()
            if self.inserted is None
            else tuple(chunk.chunk_version_id for chunk in self.inserted.chunks)
        )

    @property
    def manifest(self) -> dict[str, object]:
        return {
            "schema": "groundloop-m4-structural-payload-v1",
            "deactivated_document_version_id": (self.deactivated_document_version_id),
            "inserted_document_version_id": (
                None
                if self.inserted is None
                else self.inserted.version.document_version_id
            ),
            "inserted_document_id": (
                None if self.inserted is None else self.inserted.version.document_id
            ),
            "inserted_content_hash": (
                None if self.inserted is None else self.inserted.version.content_hash
            ),
            "inserted_chunk_ids": list(self.inserted_chunk_ids),
            "inserted_chunk_hashes": (
                []
                if self.inserted is None
                else [chunk.text_hash for chunk in self.inserted.chunks]
            ),
            "inserted_chunks": (
                []
                if self.inserted is None
                else [
                    {
                        "chunk_version_id": chunk.chunk_version_id,
                        "document_version_id": chunk.document_version_id,
                        "chunk_index": chunk.chunk_index,
                        "text": chunk.text,
                        "text_hash": chunk.text_hash,
                    }
                    for chunk in self.inserted.chunks
                ]
            ),
            "source_uri": (None if self.inserted is None else self.inserted.source_uri),
            "authority_class": (
                None if self.inserted is None else self.inserted.authority_class
            ),
            "chunker_version": (
                None if self.inserted is None else self.inserted.chunker_version
            ),
            "chunker_artifact_id": (
                None if self.inserted is None else self.inserted.chunker_artifact_id
            ),
            "chunker_input_hash": (
                None if self.inserted is None else self.inserted.chunker_input_hash
            ),
        }


class AdmissionDelegate(Protocol):
    def discover(self, epoch_id: int, root_job: LogicalJobSpec) -> DiscoveryResult: ...


class VerificationProvenanceWriter(Protocol):
    """Optional real-model provenance hook executed in the completion tx."""

    def __call__(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
    ) -> None: ...

    def raw_output_hash(self, artifact_id: str) -> str: ...


@dataclass(slots=True)
class PostgresPairInputResolver:
    """Resolve a VERIFY_PAIR input from immutable relational identities."""

    connection: Connection[Any]

    def __post_init__(self) -> None:
        _require_autocommit(self.connection)

    def resolve_pair_input(
        self, *, epoch_id: int, pair: PairKey
    ) -> PairVerificationInput:
        if epoch_id <= 0:
            raise ValidationError("resolver epoch_id must be positive")
        row = self.connection.execute(
            """
            SELECT claim.text, claim.required, claim.answer_version_id,
                   chunk.document_version_id, chunk.chunk_index, chunk.text,
                   chunk.text_hash, provenance.chunker_artifact_id
            FROM groundloop_claim AS claim
            CROSS JOIN groundloop_chunk_version AS chunk
            JOIN groundloop_chunk_provenance AS provenance
              ON provenance.chunk_version_id = chunk.chunk_version_id
            WHERE claim.claim_id = %s AND chunk.chunk_version_id = %s
            """,
            (pair.claim_id, pair.chunk_version_id),
        ).fetchone()
        if row is None:
            raise ValidationError(
                "verification input lacks a claim, chunk, or chunk provenance"
            )
        citations = tuple(
            str(item[0])
            for item in self.connection.execute(
                """
                SELECT chunk_version_id FROM groundloop_answer_citation
                WHERE answer_version_id = %s ORDER BY citation_ordinal
                """,
                (row[2],),
            ).fetchall()
        )
        return PairVerificationInput(
            pair=pair,
            claim_text=str(row[0]),
            claim_required=bool(row[1]),
            claim_cited_chunk_version_ids=citations,
            document_version_id=str(row[3]),
            chunk_index=int(row[4]),
            chunk_text=str(row[5]),
            chunk_text_hash=str(row[6]).strip(),
            chunker_artifact_id=str(row[7]),
        )


@dataclass(slots=True)
class PostgresM4VerificationExecutionWriter:
    """Persist the full real-model artifact in the verifier completion tx."""

    verifier_port: M4VerificationApplicationPort
    split_id: str = "m4-dynamic-smoke"

    def raw_output_hash(self, artifact_id: str) -> str:
        return self.verifier_port.artifact_by_id(artifact_id).result.raw_output_hash

    def __call__(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
    ) -> None:
        pair = verifier_job.pair
        if pair is None:
            raise ValidationError("verification provenance requires a PairKey")
        artifact = self.verifier_port.artifact_by_id(completion.result_artifact_id)
        spec = self.verifier_port.adapter.spec
        expected_observation = artifact.to_semantic_observation(
            model_id=spec.model_artifact.model_id,
            model_revision=spec.model_artifact.immutable_revision,
            prompt_version=spec.prompt_artifact.version,
        )
        if expected_observation != observation:
            raise EventConflictError(
                "persisted observation differs from model artifact"
            )
        raw_logits = artifact.result.raw_logits
        calibration_hash = self.verifier_port.adapter.spec.calibration_artifact_sha256
        if raw_logits is None or calibration_hash is None:
            raise ValidationError(
                "durable M4 model provenance requires raw logits and calibration hash"
            )
        admitted = cursor.execute(
            """
            SELECT admitted_pair_id FROM groundloop_admitted_pair
            WHERE epoch_id = %s AND claim_id = %s AND chunk_version_id = %s
              AND candidate_policy_id = %s
            """,
            (
                epoch_id,
                pair.claim_id,
                pair.chunk_version_id,
                verifier_job.candidate_policy_id,
            ),
        ).fetchone()
        if admitted is None:
            raise ValidationError("verifier job has no persisted admitted pair")
        cursor.execute(
            """
            INSERT INTO groundloop_m4_verification_execution (
                observation_id, job_id, admitted_pair_id, model_artifact_id,
                prompt_artifact_id, execution_spec_hash, pair_input_hash,
                calibration_version, calibration_artifact_sha256, temperature,
                raw_logits, raw_output_hash, reused_from_observation_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL
            ) ON CONFLICT (observation_id) DO NOTHING
            """,
            (
                observation.observation_id,
                verifier_job.job_id,
                admitted[0],
                spec.model_artifact.artifact_id,
                spec.prompt_artifact.artifact_id,
                artifact.execution_spec_hash,
                artifact.pair_input_hash,
                artifact.result.calibration_version,
                calibration_hash,
                artifact.result.temperature,
                list(raw_logits),
                artifact.result.raw_output_hash,
            ),
        )
        execution_row = cursor.execute(
            """
            SELECT job_id, admitted_pair_id, model_artifact_id,
                   prompt_artifact_id, execution_spec_hash, pair_input_hash,
                   calibration_version, calibration_artifact_sha256,
                   temperature, raw_logits, raw_output_hash,
                   reused_from_observation_id
            FROM groundloop_m4_verification_execution
            WHERE observation_id = %s
            """,
            (observation.observation_id,),
        ).fetchone()
        expected_execution = (
            verifier_job.job_id,
            admitted[0],
            spec.model_artifact.artifact_id,
            spec.prompt_artifact.artifact_id,
            artifact.execution_spec_hash,
            artifact.pair_input_hash,
            artifact.result.calibration_version,
            calibration_hash,
            artifact.result.temperature,
            list(raw_logits),
            artifact.result.raw_output_hash,
            None,
        )
        if execution_row is None or tuple(execution_row) != expected_execution:
            raise EventConflictError(
                "verification-execution identity was reused with different content"
            )
        judgment = artifact.to_pair_judgment(split_id=self.split_id)
        judgment_id = stable_m4_digest(
            "m4-pair-judgment-v1", artifact.artifact_id, self.split_id
        )
        cursor.execute(
            """
            INSERT INTO groundloop_pair_judgment (
                judgment_id, claim_id, chunk_version_id, source_kind,
                source_artifact_id, decision_policy_or_guideline_id,
                derived_label, support_score, refute_score, neutral_score,
                input_hash, split_id, manifest_id
            ) VALUES (
                %s, %s, %s, 'model', %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT DO NOTHING
            """,
            (
                judgment_id,
                pair.claim_id,
                pair.chunk_version_id,
                judgment.source_artifact_id,
                judgment.decision_policy_or_guideline_id,
                judgment.derived_label.value,
                judgment.support_score,
                judgment.refute_score,
                judgment.neutral_score,
                judgment.input_hash,
                judgment.split_id,
                verifier_job.candidate_policy_id,
            ),
        )
        judgment_row = cursor.execute(
            """
            SELECT claim_id, chunk_version_id, source_kind,
                   source_artifact_id, decision_policy_or_guideline_id,
                   derived_label, support_score, refute_score, neutral_score,
                   input_hash, split_id, manifest_id
            FROM groundloop_pair_judgment WHERE judgment_id = %s
            """,
            (judgment_id,),
        ).fetchone()
        expected_judgment = (
            pair.claim_id,
            pair.chunk_version_id,
            "model",
            judgment.source_artifact_id,
            judgment.decision_policy_or_guideline_id,
            judgment.derived_label.value,
            judgment.support_score,
            judgment.refute_score,
            judgment.neutral_score,
            judgment.input_hash,
            judgment.split_id,
            verifier_job.candidate_policy_id,
        )
        if judgment_row is None or tuple(judgment_row) != expected_judgment:
            raise EventConflictError(
                "pair-judgment identity was reused with different content"
            )


@dataclass(slots=True)
class PersistingAdmissionPort:
    """Compatibility wrapper; persistence now belongs to expansion closure."""

    connection: Connection[Any]
    delegate: AdmissionDelegate

    def discover(self, epoch_id: int, root_job: LogicalJobSpec) -> DiscoveryResult:
        return self.delegate.discover(epoch_id, root_job)


def _load_repository_snapshot(
    connection: Connection[Any],
    published_epoch_id: int,
    *,
    working_epoch_id: int | None,
) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
    """Rebuild a published or one-event working snapshot after startup."""
    repository = InMemoryRepository()
    repository.advance_epoch()
    question_rows = connection.execute(
        "SELECT question_id, text FROM groundloop_question ORDER BY question_id"
    ).fetchall()
    for question_id, text in question_rows:
        repository.register_question(Question(str(question_id), str(text)))

    registry_epoch_id = (
        published_epoch_id if working_epoch_id is None else working_epoch_id
    )
    registry_row = connection.execute(
        """
        SELECT registry_snapshot_id FROM groundloop_m4_update
        WHERE epoch_id = %s
        """,
        (registry_epoch_id,),
    ).fetchone()

    answer_rows = connection.execute(
        """
        SELECT answer_version_id, question_id, text, generator_model_id,
               generator_model_version, prompt_version
        FROM groundloop_answer_version ORDER BY answer_version_id
        """
    ).fetchall()
    claims_by_answer: dict[str, list[Claim]] = {}
    if registry_row is None:
        claim_rows = connection.execute(
            """
            SELECT claim_id, answer_version_id, text, extractor_model_id,
                   extractor_model_version, extractor_prompt_version, required
            FROM groundloop_claim ORDER BY answer_version_id, claim_id
            """
        ).fetchall()
    else:
        claim_rows = connection.execute(
            """
            SELECT claim.claim_id, claim.answer_version_id, claim.text,
                   claim.extractor_model_id, claim.extractor_model_version,
                   claim.extractor_prompt_version, claim.required
            FROM groundloop_m4_claim_registry_member AS member
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE member.claim_registry_snapshot_id = %s
            ORDER BY claim.answer_version_id, claim.claim_id
            """,
            (str(registry_row[0]),),
        ).fetchall()
    for row in claim_rows:
        claims_by_answer.setdefault(str(row[1]), []).append(
            Claim(
                str(row[0]),
                str(row[1]),
                str(row[2]),
                ModelStamp(str(row[3]), str(row[4]), str(row[5])),
                bool(row[6]),
            )
        )
    for row in answer_rows:
        answer = AnswerVersion(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            ModelStamp(str(row[3]), str(row[4]), str(row[5])),
        )
        answer_claims = tuple(claims_by_answer.get(answer.answer_version_id, ()))
        if not answer_claims:
            continue
        repository.register_answer(answer, answer_claims)

    policy_row = connection.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY valid_from_epoch DESC LIMIT 1
        """,
        (published_epoch_id, published_epoch_id),
    ).fetchone()
    if policy_row is None:
        raise ValidationError("published state has no active decision policy")
    repository.activate_policy(
        DecisionPolicy(
            str(policy_row[0]),
            float(policy_row[1]),
            float(policy_row[2]),
            str(policy_row[3]),
        ),
        repository.current_epoch,
    )

    if working_epoch_id is None:
        version_rows = connection.execute(
            """
            SELECT document_version_id, document_id, content_hash
            FROM groundloop_document_version AS version
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = version.valid_from_epoch
             AND creator.semantic_status = 'sealed'
            WHERE valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY document_version_id
            """,
            (published_epoch_id, published_epoch_id),
        ).fetchall()
    else:
        version_rows = connection.execute(
            """
            SELECT document_version_id, document_id, content_hash
            FROM groundloop_m4_effective_document_version
            WHERE epoch_id = %s ORDER BY document_version_id
            """,
            (working_epoch_id,),
        ).fetchall()
    for version_row in version_rows:
        version_id = str(version_row[0])
        if working_epoch_id is None:
            chunk_rows = connection.execute(
                """
                SELECT chunk_version_id, chunk_index, text, text_hash
                FROM groundloop_chunk_version
                WHERE document_version_id = %s
                  AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                ORDER BY chunk_version_id
                """,
                (version_id, published_epoch_id, published_epoch_id),
            ).fetchall()
        else:
            chunk_rows = connection.execute(
                """
                SELECT chunk_version_id, chunk_index, text, text_hash
                FROM groundloop_m4_effective_chunk_version
                WHERE epoch_id = %s AND document_version_id = %s
                ORDER BY chunk_version_id
                """,
                (working_epoch_id, version_id),
            ).fetchall()
        version = DocumentVersion(version_id, str(version_row[1]), str(version_row[2]))
        chunks = tuple(
            ChunkVersion(
                str(row[0]),
                version_id,
                int(row[1]),
                str(row[2]),
                str(row[3]).strip(),
            )
            for row in chunk_rows
        )
        repository.register_document_version(version, chunks, repository.current_epoch)

    if working_epoch_id is None:
        observation_rows = connection.execute(
            """
            SELECT observation.observation_id, observation.subject_kind,
                   observation.subject_id, observation.chunk_version_id,
                   observation.task_type, observation.support_score,
                   observation.refute_score, observation.neutral_score,
                   observation.model_id, observation.model_version,
                   observation.prompt_version, observation.input_hash
            FROM groundloop_observation_currency AS currency
            JOIN groundloop_semantic_observation AS observation
              ON observation.observation_id = currency.observation_id
            WHERE currency.subject_kind = 'claim'
            ORDER BY observation.observation_id
            """
        ).fetchall()
    else:
        observation_rows = connection.execute(
            """
            SELECT observation.observation_id, observation.subject_kind,
                   observation.subject_id, observation.chunk_version_id,
                   observation.task_type, observation.support_score,
                   observation.refute_score, observation.neutral_score,
                   observation.model_id, observation.model_version,
                   observation.prompt_version, observation.input_hash
            FROM groundloop_m4_effective_observation_currency AS currency
            JOIN groundloop_semantic_observation AS observation
              ON observation.observation_id = currency.observation_id
            JOIN groundloop_m4_effective_chunk_version AS chunk
              ON chunk.epoch_id = currency.epoch_id
             AND chunk.chunk_version_id = currency.chunk_version_id
            WHERE currency.epoch_id = %s
              AND currency.subject_kind = 'claim'
            ORDER BY observation.observation_id
            """,
            (working_epoch_id,),
        ).fetchall()
    for row in observation_rows:
        repository.register_observation(
            SemanticObservation(
                observation_id=str(row[0]),
                subject_kind=SubjectKind(str(row[1])),
                subject_id=str(row[2]),
                chunk_version_id=str(row[3]),
                task_type=str(row[4]),
                support_score=float(row[5]),
                refute_score=float(row[6]),
                neutral_score=float(row[7]),
                producer=ModelStamp(str(row[8]), str(row[9]), str(row[10])),
                input_hash=str(row[11]),
            )
        )
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    return repository, engine


def _load_published_repository(
    connection: Connection[Any], epoch_id: int
) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
    """Rebuild process-local strict state after startup, never during an event."""
    return _load_repository_snapshot(connection, epoch_id, working_epoch_id=None)


def _load_working_repository(
    connection: Connection[Any], epoch_id: int, published_epoch_id: int
) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
    """Rebuild one pending event from its durable structural/semantic overlay."""
    return _load_repository_snapshot(
        connection, published_epoch_id, working_epoch_id=epoch_id
    )


def bootstrap_m4_publication(
    connection: Connection[Any], *, sealed_epoch_id: int
) -> None:
    """Initialize interval publication from one already sealed M2/M3 state."""
    with connection.transaction():
        epoch = connection.execute(
            "SELECT semantic_status FROM groundloop_epoch WHERE epoch_id = %s",
            (sealed_epoch_id,),
        ).fetchone()
        if epoch is None or str(epoch[0]) != "sealed":
            raise InvalidEventError("M4 bootstrap requires a sealed epoch")
        head = connection.execute(
            "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
        ).fetchone()
        if head is not None and int(head[0]) != sealed_epoch_id:
            raise EventConflictError("M4 publication head already names another epoch")
        connection.execute(
            """
            INSERT INTO groundloop_m4_publication_head(singleton, epoch_id)
            VALUES (true, %s)
            ON CONFLICT (singleton) DO NOTHING
            """,
            (sealed_epoch_id,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_published_observation_currency (
                subject_kind, subject_id, chunk_version_id, task_type,
                observation_id, valid_from_epoch, valid_to_epoch
            )
            SELECT subject_kind, subject_id, chunk_version_id, task_type,
                   observation_id, %s, NULL
            FROM groundloop_observation_currency
            WHERE subject_kind = 'claim'
            ON CONFLICT DO NOTHING
            """,
            (sealed_epoch_id,),
        )
        repository, engine = _load_published_repository(connection, sealed_epoch_id)
        reference_claims, reference_answers = compute_all_states(repository)
        if (
            engine.claim_states != reference_claims
            or engine.answer_states != reference_answers
        ):
            raise ValidationError(
                "bootstrap Python IVM differs from full recomputation"
            )
        for claim_state in engine.claim_states.values():
            connection.execute(
                """
                INSERT INTO groundloop_published_claim_state VALUES (
                    %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT DO NOTHING
                """,
                (
                    claim_state.claim_id,
                    sealed_epoch_id,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.status.value,
                    _certificate_digest(claim_state.claim_id, claim_state),
                ),
            )
        for answer_state in engine.answer_states.values():
            connection.execute(
                """
                INSERT INTO groundloop_published_answer_state VALUES (
                    %s, %s, NULL, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT DO NOTHING
                """,
                (
                    answer_state.answer_version_id,
                    sealed_epoch_id,
                    answer_state.required_claim_count,
                    answer_state.supported_count,
                    answer_state.unsupported_count,
                    answer_state.refuted_count,
                    answer_state.conflicted_count,
                    answer_state.status.value,
                ),
            )


class PostgresM4ApplicationPorts:
    """One object implementing the storage-facing M4 application ports."""

    def __init__(
        self,
        connection: Connection[Any],
        *,
        structural_payloads: Mapping[str, StructuralPayload],
        verification_provenance_writer: VerificationProvenanceWriter | None = None,
        failure_injector: Callable[[str], None] | None = None,
        execution_mode: M4ExecutionMode = M4ExecutionMode.AUDIT,
    ) -> None:
        _require_autocommit(connection)
        self.connection = connection
        self.execution_mode = execution_mode
        self.runtime_store = PostgresM4RuntimeStore(
            connection, audit_transitions=not self._measured
        )
        self.evaluation_store = PostgresEvaluationOverlayStore(connection)
        self._payloads = dict(structural_payloads)
        self._verification_writer = verification_provenance_writer
        self._failure_injector = failure_injector
        self._fallback_blocked: set[str] = set()
        # These registries make the two private C7 lock values cursor/xact-local,
        # byte-exact and single-use without adding process-visible fields to their
        # frozen contract shapes. Durable authority remains in locked SQL.
        self._typed_failure_job_lock_authority: dict[
            int,
            tuple[
                int,
                str,
                _TypedDirectEpochFailureJobLocks,
                tuple[object, ...],
            ],
        ] = {}
        self._typed_failure_plan_authority: dict[
            int,
            tuple[
                int,
                str,
                _TypedDirectEpochFailureLockedPlan,
                tuple[object, ...],
                str | None,
            ],
        ] = {}
        head = self._publication_head()
        self._published_repository, self._published_engine = _load_published_repository(
            connection, head
        )
        if self._measured:
            # Startup hydration is outside the measured event kernel.  The
            # measured path advances this cache with affected-key patches and
            # adopts the same objects at seal; it never clones full state.
            self._working_repository = self._published_repository
            self._working_engine = self._published_engine
        else:
            self._working_repository = deepcopy(self._published_repository)
            self._working_engine = deepcopy(self._published_engine)
        self._active_epoch_id: int | None = None

    @property
    def _measured(self) -> bool:
        return self.execution_mode is M4ExecutionMode.MEASURED

    def _register_execution_accounting(
        self, cursor: Cursor[Any], epoch_id: int
    ) -> None:
        cursor.execute(
            """
            INSERT INTO groundloop_m4_execution_accounting (
                epoch_id, execution_mode
            ) VALUES (%s, %s) ON CONFLICT (epoch_id) DO NOTHING
            """,
            (epoch_id, self.execution_mode.value),
        )
        row = cursor.execute(
            """
            SELECT execution_mode FROM groundloop_m4_execution_accounting
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if row is None or str(row[0]) != self.execution_mode.value:
            raise EventConflictError(
                "M4 event replay changed the physical execution mode"
            )

    @staticmethod
    def _account(
        cursor: Cursor[Any], epoch_id: int, column: str, amount: int = 1
    ) -> None:
        allowed = {
            "inline_grounding_oracle_calls",
            "working_claim_rows_written",
            "working_answer_rows_written",
            "evaluation_default_rows_written",
            "evaluation_override_rows_written",
            "active_chunk_rows_examined",
            "published_claim_versions_written",
            "published_answer_versions_written",
        }
        if column not in allowed or amount < 0:
            raise ValidationError("invalid M4 execution-accounting update")
        cursor.execute(
            f"""
            UPDATE groundloop_m4_execution_accounting
            SET {column} = {column} + %s WHERE epoch_id = %s
            """,
            (amount, epoch_id),
        )

    def _inject(self, point: str) -> None:
        if self._failure_injector is not None:
            self._failure_injector(point)

    def _publication_head(self) -> int:
        row = self.connection.execute(
            "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
        ).fetchone()
        if row is None:
            raise ValidationError("M4 publication is not bootstrapped")
        return int(row[0])

    def _adopt_direct_working_cache(
        self,
        epoch_id: int,
        repository: InMemoryRepository,
        engine: IncrementalMaintenanceEngine,
    ) -> None:
        """Adopt a detached cache only after its outer transaction commits."""
        self._working_repository = repository
        self._working_engine = engine
        self._active_epoch_id = epoch_id
        self._fallback_blocked.clear()

    def _hydrate_direct_working_cache(self, epoch_id: int) -> None:
        """Recover an unsealed direct cache from its durable working overlay."""
        previous = self.connection.execute(
            """
            SELECT previous_published_epoch_id FROM groundloop_m4_update
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if previous is None or previous[0] is None:
            raise ValidationError("direct working cache lacks a published base")
        repository, engine = _load_working_repository(
            self.connection, epoch_id, int(previous[0])
        )
        self._adopt_direct_working_cache(epoch_id, repository, engine)

    def _adopt_direct_failure_cache(self) -> None:
        head = self._publication_head()
        self._published_repository, self._published_engine = _load_published_repository(
            self.connection, head
        )
        self._working_repository = self._published_repository
        self._working_engine = self._published_engine
        self._active_epoch_id = None

    def _adopt_direct_seal_cache(self) -> None:
        self._published_repository = self._working_repository
        self._published_engine = self._working_engine
        self._active_epoch_id = None

    def register_claim_registry_snapshot(
        self, snapshot_id: str, claim_ids: tuple[str, ...]
    ) -> bool:
        """Build immutable policy-time registry state outside event execution."""
        return self.runtime_store.register_claim_registry_snapshot(
            snapshot_id, claim_ids
        )

    def _direct_claim_registry_members(self, snapshot_id: str) -> tuple[str, ...]:
        """Read one exact immutable measured registry for the direct bridge."""

        if type(snapshot_id) is not str or not snapshot_id.strip():
            raise ValidationError("direct claim registry ID must be exact text")
        with self.connection.transaction():
            header = self.connection.execute(
                """
                SELECT claim_count, claim_set_hash
                FROM groundloop_m4_claim_registry_snapshot
                WHERE claim_registry_snapshot_id = %s
                """,
                (snapshot_id,),
            ).fetchone()
            rows = self.connection.execute(
                """
                SELECT member_ordinal, claim_id
                FROM groundloop_m4_claim_registry_member
                WHERE claim_registry_snapshot_id = %s
                ORDER BY member_ordinal
                """,
                (snapshot_id,),
            ).fetchall()
        if header is None:
            raise InvalidEventError("direct claim registry snapshot is absent")
        if (
            type(header[0]) is not int
            or type(header[1]) is not str
            or any(
                type(row[0]) is not int or type(row[1]) is not str or not row[1].strip()
                for row in rows
            )
        ):
            raise ValidationError("direct claim registry snapshot has unsafe values")
        ordinals = tuple(row[0] for row in rows)
        members = tuple(row[1] for row in rows)
        expected_hash = stable_m4_digest("m4-claim-registry-snapshot-v1", *members)
        if (
            ordinals != tuple(range(len(rows)))
            or members != tuple(sorted(set(members)))
            or len(members) != header[0]
            or header[1].strip() != expected_hash
        ):
            raise ValidationError("direct claim registry snapshot is incomplete")
        return members

    def plan_exact_withdrawal(self, event: DynamicEventPlan) -> StructuralWithdrawal:
        deactivated_chunk_version_ids = event.deactivated_chunk_version_ids
        existing = self.connection.execute(
            "SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s",
            (event.update.event_id,),
        ).fetchone()
        if existing is not None:
            epoch_id = int(existing[0])
            observation_rows = self.connection.execute(
                """
                SELECT delta.base_observation_id, delta.subject_id,
                       delta.chunk_version_id
                FROM groundloop_working_observation_delta AS delta
                WHERE delta.epoch_id = %s
                  AND delta.subject_kind = 'claim'
                  AND delta.base_observation_id IS NOT NULL
                  AND delta.chunk_version_id = ANY(%s)
                ORDER BY delta.base_observation_id
                """,
                (epoch_id, list(deactivated_chunk_version_ids)),
            ).fetchall()
            fallback_rows = self.connection.execute(
                """
                SELECT claim_id FROM groundloop_semantic_job
                WHERE epoch_id = %s AND parent_job_id IS NULL
                  AND job_kind = 'frontier_retrieve'
                ORDER BY claim_id
                """,
                (epoch_id,),
            ).fetchall()
            observations = tuple(
                ObservationDependency(str(row[0]), PairKey(str(row[1]), str(row[2])))
                for row in observation_rows
            )
            plan = plan_withdrawal(
                ReverseDependencyIndex.build(observations, ()),
                deactivated_chunk_version_ids,
            )
            fallback = tuple(str(row[0]) for row in fallback_rows)
            return StructuralWithdrawal(plan, fallback)
        observation_rows = self.connection.execute(
            """
            SELECT observation.observation_id, observation.subject_id,
                   observation.chunk_version_id
            FROM groundloop_observation_currency AS currency
            JOIN groundloop_semantic_observation AS observation
              ON observation.observation_id = currency.observation_id
            WHERE currency.subject_kind = 'claim'
              AND observation.chunk_version_id = ANY(%s)
            ORDER BY observation.observation_id
            """,
            (list(deactivated_chunk_version_ids),),
        ).fetchall()
        candidate_rows = self.connection.execute(
            """
            SELECT claim_id, chunk_version_id, candidate_policy_id,
                   candidate_artifact_hash
            FROM groundloop_candidate_frontier AS frontier
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = frontier.valid_from_epoch
             AND creator.semantic_status = 'sealed'
            WHERE valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
              AND chunk_version_id = ANY(%s)
            ORDER BY chunk_version_id, claim_id, candidate_policy_id
            """,
            (
                event.update.previous_published_epoch_id,
                event.update.previous_published_epoch_id,
                list(deactivated_chunk_version_ids),
            ),
        ).fetchall()
        observations = tuple(
            ObservationDependency(str(row[0]), PairKey(str(row[1]), str(row[2])))
            for row in observation_rows
        )
        candidates = tuple(
            CandidateDependency(
                stable_m4_digest(
                    "m4-frontier-edge-v1",
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]).strip(),
                ),
                PairKey(str(row[0]), str(row[1])),
            )
            for row in candidate_rows
        )
        plan = plan_withdrawal(
            ReverseDependencyIndex.build(observations, candidates),
            deactivated_chunk_version_ids,
        )
        return StructuralWithdrawal(plan, plan.affected_claim_ids)

    def _validate_payload(
        self, event: DynamicEventPlan, payload: StructuralPayload
    ) -> None:
        existing_epoch = self.connection.execute(
            """
            SELECT update_row.epoch_id
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_epoch AS epoch USING (epoch_id)
            WHERE epoch.event_id = %s
            """,
            (event.update.event_id,),
        ).fetchone()
        snapshot = (
            self.connection.execute(
                """
                SELECT claim_count, claim_set_hash
                FROM groundloop_m4_claim_registry_snapshot
                WHERE claim_registry_snapshot_id = %s
                """,
                (event.claim_registry_snapshot_id,),
            ).fetchone()
            if self._measured
            else None
        )
        registered: tuple[str, ...]
        if self._measured:
            if event.registered_claim_ids:
                raise ValidationError(
                    "measured events bind a prebuilt registry by identity only"
                )
            if snapshot is None:
                raise InvalidEventError(
                    "measured event requires a prebuilt claim registry snapshot"
                )
            policy = self.connection.execute(
                """
                SELECT claim_count, claim_registry_snapshot_id
                FROM groundloop_candidate_policy
                WHERE candidate_policy_id = %s
                """,
                (event.update.candidate_policy_id,),
            ).fetchone()
            if policy is None or (
                int(policy[0]),
                str(policy[1]),
            ) != (int(snapshot[0]), event.claim_registry_snapshot_id):
                raise EventConflictError(
                    "candidate policy differs from its frozen claim registry"
                )
            registered = ()
        elif snapshot is not None:
            expected_hash = stable_m4_digest(
                "m4-claim-registry-snapshot-v1", *event.registered_claim_ids
            )
            if (int(snapshot[0]), str(snapshot[1]).strip()) != (
                len(event.registered_claim_ids),
                expected_hash,
            ):
                raise EventConflictError(
                    "event claim registry differs from its frozen snapshot"
                )
            registered = event.registered_claim_ids
        elif existing_epoch is None:
            registered = tuple(
                str(row[0])
                for row in self.connection.execute(
                    "SELECT claim_id FROM groundloop_claim ORDER BY claim_id"
                ).fetchall()
            )
        else:
            registered = tuple(
                str(row[0])
                for row in self.connection.execute(
                    """
                    SELECT claim_id FROM groundloop_m4_claim_registry_member
                    WHERE claim_registry_snapshot_id = %s
                    ORDER BY member_ordinal
                    """,
                    (event.claim_registry_snapshot_id,),
                ).fetchall()
            )
        if event.registered_claim_ids != registered:
            raise EventConflictError(
                "event claim registry differs from the current registered claims"
            )
        if payload.inserted_chunk_ids != event.inserted_chunk_version_ids:
            raise EventConflictError("structural payload inserted chunks differ")
        expected_old = self._deactivated_chunk_ids(event, payload)
        if expected_old != event.deactivated_chunk_version_ids:
            raise EventConflictError("structural payload deactivated chunks differ")
        if event.update.update_kind is UpdateKind.INSERT and (
            payload.inserted is None
            or payload.deactivated_document_version_id is not None
        ):
            raise ValidationError("INSERT structural payload has the wrong shape")
        if event.update.update_kind is UpdateKind.DELETE and (
            payload.inserted is not None
            or payload.deactivated_document_version_id is None
        ):
            raise ValidationError("DELETE structural payload has the wrong shape")
        if event.update.update_kind is UpdateKind.REPLACE and (
            payload.inserted is None or payload.deactivated_document_version_id is None
        ):
            raise ValidationError("REPLACE structural payload has the wrong shape")

    def _deactivated_chunk_ids(
        self, event: DynamicEventPlan, payload: StructuralPayload
    ) -> tuple[str, ...]:
        if payload.deactivated_document_version_id is None:
            return ()
        rows = self.connection.execute(
            """
            SELECT chunk.chunk_version_id
            FROM groundloop_chunk_version AS chunk
            JOIN groundloop_document_version AS version
              ON version.document_version_id = chunk.document_version_id
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = version.valid_from_epoch
             AND creator.semantic_status = 'sealed'
            WHERE chunk.document_version_id = %s
              AND chunk.valid_from_epoch <= %s
              AND (
                  chunk.valid_to_epoch IS NULL OR %s < chunk.valid_to_epoch
              )
            ORDER BY chunk_version_id
            """,
            (
                payload.deactivated_document_version_id,
                event.update.previous_published_epoch_id,
                event.update.previous_published_epoch_id,
            ),
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def _answer_ids_for_claims(self, claim_ids: tuple[str, ...]) -> tuple[str, ...]:
        if not claim_ids:
            return ()
        return tuple(
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT DISTINCT answer_version_id FROM groundloop_claim
                WHERE claim_id = ANY(%s) ORDER BY answer_version_id
                """,
                (list(claim_ids),),
            ).fetchall()
        )

    def _stage_structural(
        self, event: DynamicEventPlan, payload: StructuralPayload
    ) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
        if self._measured:
            after = self._working_repository
            engine = self._working_engine
        else:
            after = deepcopy(self._published_repository)
            engine = deepcopy(self._published_engine)
        before = after if self._measured else deepcopy(after)
        self._apply_structural_event(
            event,
            payload,
            before=before,
            after=after,
            engine=engine,
            measured=self._measured,
        )
        return after, engine

    def _stage_structural_detached(
        self, event: DynamicEventPlan, payload: StructuralPayload
    ) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
        """Prepare cache state that is adopted only after the outer commit."""
        after = deepcopy(self._published_repository)
        engine = deepcopy(self._published_engine)
        before = deepcopy(after)
        self._apply_structural_event(
            event,
            payload,
            before=before,
            after=after,
            engine=engine,
            measured=self._measured,
        )
        return after, engine

    @staticmethod
    def _apply_structural_event(
        event: DynamicEventPlan,
        payload: StructuralPayload,
        *,
        before: InMemoryRepository,
        after: InMemoryRepository,
        engine: IncrementalMaintenanceEngine,
        measured: bool,
    ) -> None:
        epoch = after.advance_epoch()
        if event.update.update_kind is UpdateKind.INSERT:
            assert payload.inserted is not None
            after.register_document_version(
                payload.inserted.version, payload.inserted.chunks, epoch
            )
            semantic_event: Event = InsertDocumentEvent(
                event.update.event_id,
                payload.inserted.version.document_id,
                payload.inserted.version.document_version_id,
                payload.inserted.version.content_hash,
                (),
            )
        elif event.update.update_kind is UpdateKind.DELETE:
            assert payload.deactivated_document_version_id is not None
            after.deactivate_document_version(
                payload.deactivated_document_version_id, epoch
            )
            semantic_event = DeleteDocumentVersionEvent(
                event.update.event_id, payload.deactivated_document_version_id
            )
        else:
            assert payload.inserted is not None
            assert payload.deactivated_document_version_id is not None
            old = before.document_version(payload.deactivated_document_version_id)
            if old.document_id != payload.inserted.version.document_id:
                raise ValidationError("replacement changes the document identity")
            after.deactivate_document_version(
                payload.deactivated_document_version_id, epoch
            )
            after.register_document_version(
                payload.inserted.version, payload.inserted.chunks, epoch
            )
            semantic_event = ReplaceDocumentVersionEvent(
                event.update.event_id,
                old.document_id,
                payload.deactivated_document_version_id,
                payload.inserted.version.document_version_id,
                payload.inserted.version.content_hash,
                (),
            )
        if measured:
            patch = engine.prepare_committed_event_patch(semantic_event, before, after)
            engine.apply_state_patch(patch)
        else:
            engine.apply_committed_event(semantic_event, before, after)

    def _stage_direct_open_local(
        self,
        cursor: Cursor[Any],
        event: DynamicEventPlan,
        payload: StructuralPayload,
        withdrawal: StructuralWithdrawal,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...],
    ) -> tuple[
        OpenEventReceipt,
        InMemoryRepository | None,
        IncrementalMaintenanceEngine | None,
    ]:
        """Stage the M4-v1 declaration without owning the transaction."""
        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        self._validate_payload(event, payload)
        if withdrawal.plan.deactivated_chunk_ids != (
            event.deactivated_chunk_version_ids
        ):
            raise EventConflictError("withdrawal and event deactivation differ")
        epoch_row = cursor.execute(
            "SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s",
            (event.update.event_id,),
        ).fetchone()
        if epoch_row is None:
            raise InvalidEventError("typed direct open lacks its outer epoch")
        epoch_id = int(epoch_row[0])
        existing = cursor.execute(
            "SELECT 1 FROM groundloop_m4_update WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone()
        staged_repository: InMemoryRepository | None = None
        staged_engine: IncrementalMaintenanceEngine | None = None
        if existing is None:
            staged_repository, staged_engine = self._stage_structural_detached(
                event, payload
            )
        touched_claim_ids = withdrawal.plan.affected_claim_ids
        touched_answer_ids = self._answer_ids_for_claims(touched_claim_ids)

        def structural_action(
            transaction_cursor: Cursor[Any], structural_epoch_id: int
        ) -> None:
            assert staged_engine is not None
            self._register_execution_accounting(transaction_cursor, structural_epoch_id)
            self._write_structural_rows(
                transaction_cursor, structural_epoch_id, payload
            )
            self._inject("structural_versions_written")
            self._write_withdrawal_overlay(
                transaction_cursor, structural_epoch_id, withdrawal
            )
            self._inject("structural_withdrawal_written")
            self._persist_working_states(
                transaction_cursor,
                structural_epoch_id,
                1,
                staged_engine,
                causative_digest=None,
                claim_ids=touched_claim_ids,
                answer_ids=touched_answer_ids,
            )
            self._inject("structural_working_states_written")
            self.evaluation_store.declare_epoch_local(
                transaction_cursor,
                structural_epoch_id,
                revision=1,
                confirmed_as_of_epoch=event.update.previous_published_epoch_id,
                open_discovery_scope_count=len(discovery_scopes),
            )
            self._inject("structural_evaluation_written")

        opened = self.runtime_store.stage_open_epoch_local(
            cursor,
            epoch_id,
            event.update,
            root_jobs,
            discovery_scopes,
            registry_snapshot_id=event.claim_registry_snapshot_id,
            structural_action=structural_action,
            event_manifest=payload.manifest,
            failure_injector=(
                lambda point: self._inject(
                    "structural_store_" + point.removeprefix("open_")
                )
            ),
        )
        header = opened.epoch
        if not isinstance(header, PointEpochHeader):
            raise ValidationError("direct measured open returned an audit epoch")
        failed = header.state is RuntimeEpochState.FAILED
        sealed = header.state is RuntimeEpochState.SEALED
        return (
            OpenEventReceipt(
                epoch_id,
                replayed=opened.replayed,
                already_sealed=sealed,
                publication_id=(
                    stable_m4_digest("m4-publication-v1", str(epoch_id))
                    if sealed
                    else None
                ),
                already_failed=failed,
                failure_reason=header.failure_reason if failed else None,
            ),
            staged_repository,
            staged_engine,
        )

    def _measure_typed_direct_open_local(
        self, cursor: Cursor[Any], epoch_id: int
    ) -> tuple[int, int]:
        """Measure the exact immutable M4 declaration without exposing its SQL."""

        if cursor.connection is not self.connection:
            raise ValidationError("typed-direct measurement cursor is foreign")
        if type(epoch_id) is not int or epoch_id < 1:
            raise ValidationError("typed-direct measurement epoch must be positive")
        queries = (
            (
                "m4_update",
                """
                SELECT update_kind, previous_published_epoch_id,
                       candidate_policy_id, registry_snapshot_id, manifest::text
                FROM groundloop_m4_update WHERE epoch_id = %s
                """,
            ),
            (
                "semantic_job",
                """
                SELECT btrim(job_id), parent_job_id, job_kind,
                       candidate_policy_id, btrim(payload_hash),
                       btrim(execution_spec_hash), claim_id, chunk_version_id,
                       expandable, created_revision
                FROM groundloop_semantic_job
                WHERE epoch_id = %s AND parent_job_id IS NULL
                ORDER BY job_id COLLATE "C"
                """,
            ),
            (
                "discovery_scope",
                """
                SELECT btrim(root_job_id), registry_snapshot_id, scope_kind,
                       explicit_claim_ids::text
                FROM groundloop_discovery_scope
                WHERE epoch_id = %s ORDER BY root_job_id COLLATE "C"
                """,
            ),
        )
        framed = [_length_frame_direct_declaration(b"m5-d24-direct-declaration-v1")]
        for relation_name, query in queries:
            rows = cursor.execute(query, (epoch_id,)).fetchall()
            if relation_name == "m4_update" and len(rows) != 1:
                raise ValidationError(
                    "typed-direct declaration lacks its exact M4 update"
                )
            relation = [_length_frame_direct_declaration(relation_name.encode("ascii"))]
            for row in rows:
                row_bytes = b"".join(
                    _length_frame_direct_declaration(
                        _canonical_direct_declaration_scalar(value)
                    )
                    for value in row
                )
                relation.append(_length_frame_direct_declaration(row_bytes))
            framed.append(_length_frame_direct_declaration(b"".join(relation)))
        byte_count = len(b"".join(framed))
        if byte_count < 1:
            raise ValidationError("typed-direct declaration measured zero bytes")
        return byte_count, byte_count

    def open_event(
        self,
        event: DynamicEventPlan,
        withdrawal: StructuralWithdrawal,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...],
    ) -> OpenEventReceipt:
        payload = self._payloads.get(event.update.event_id)
        if payload is None:
            raise InvalidEventError("no structural payload is bound to this event")
        existing = self.connection.execute(
            """
            SELECT epoch_id, semantic_status FROM groundloop_epoch
            WHERE event_id = %s
            """,
            (event.update.event_id,),
        ).fetchone()
        if existing is not None:
            self._validate_payload(event, payload)
            opened = self.runtime_store.open_epoch(
                event.update,
                root_jobs,
                discovery_scopes,
                registry_snapshot_id=event.claim_registry_snapshot_id,
                structural_action=lambda _cursor, _epoch_id: None,
                event_manifest=payload.manifest,
            )
            with self.connection.transaction():
                with self.connection.cursor() as cursor:
                    self._register_execution_accounting(cursor, int(existing[0]))
            sealed = str(existing[1]) == "sealed"
            failed = str(existing[1]) == "failed"
            if not sealed and not failed:
                previous_epoch = event.update.previous_published_epoch_id
                if previous_epoch is None:
                    raise ValidationError(
                        "M4 working replay requires a previous published epoch"
                    )
                working_repository, working_engine = _load_working_repository(
                    self.connection,
                    opened.epoch.epoch_id,
                    previous_epoch,
                )
                if not self._measured:
                    with self.connection.cursor() as cursor:
                        self._assert_grounding_equality(
                            cursor,
                            opened.epoch.epoch_id,
                            working_repository,
                            working_engine,
                        )
                self._working_repository = working_repository
                self._working_engine = working_engine
                self._active_epoch_id = opened.epoch.epoch_id
            return OpenEventReceipt(
                opened.epoch.epoch_id,
                replayed=True,
                already_sealed=sealed,
                publication_id=(
                    stable_m4_digest("m4-publication-v1", str(existing[0]))
                    if sealed
                    else None
                ),
                already_failed=failed,
                failure_reason=opened.epoch.failure_reason if failed else None,
            )

        self._validate_payload(event, payload)
        if withdrawal.plan.deactivated_chunk_ids != event.deactivated_chunk_version_ids:
            raise EventConflictError("withdrawal and event deactivation differ")
        try:
            staged_repository, staged_engine = self._stage_structural(event, payload)
        except Exception:
            if self._measured:
                head = self._publication_head()
                self._published_repository, self._published_engine = (
                    _load_published_repository(self.connection, head)
                )
                self._working_repository = self._published_repository
                self._working_engine = self._published_engine
            raise
        touched_claim_ids = withdrawal.plan.affected_claim_ids
        touched_answer_ids = self._answer_ids_for_claims(touched_claim_ids)

        def structural_action(cursor: Cursor[Any], epoch_id: int) -> None:
            self._register_execution_accounting(cursor, epoch_id)
            if not self._measured:
                self._write_claim_registry_members(cursor, event)
            self._inject("structural_registry_written")
            self._write_structural_rows(cursor, epoch_id, payload)
            self._inject("structural_versions_written")
            self._write_withdrawal_overlay(cursor, epoch_id, withdrawal)
            self._inject("structural_withdrawal_written")
            self._persist_working_states(
                cursor,
                epoch_id,
                1,
                staged_engine,
                causative_digest=None,
                claim_ids=(touched_claim_ids if self._measured else None),
                answer_ids=(touched_answer_ids if self._measured else None),
            )
            self._inject("structural_working_states_written")
            if self._measured:
                self.evaluation_store.declare_epoch(
                    epoch_id,
                    revision=1,
                    confirmed_as_of_epoch=event.update.previous_published_epoch_id,
                    open_discovery_scope_count=len(discovery_scopes),
                )
            else:
                self._write_initial_evaluation(
                    cursor,
                    epoch_id,
                    event,
                    root_jobs,
                    discovery_scopes,
                )
                self._assert_grounding_equality(
                    cursor, epoch_id, staged_repository, staged_engine
                )
            self._inject("structural_evaluation_written")

        try:
            opened = self.runtime_store.open_epoch(
                event.update,
                root_jobs,
                discovery_scopes,
                registry_snapshot_id=event.claim_registry_snapshot_id,
                structural_action=structural_action,
                event_manifest=payload.manifest,
            )
        except Exception:
            if self._measured:
                head = self._publication_head()
                self._published_repository, self._published_engine = (
                    _load_published_repository(self.connection, head)
                )
                self._working_repository = self._published_repository
                self._working_engine = self._published_engine
            raise
        self._working_repository = staged_repository
        self._working_engine = staged_engine
        self._active_epoch_id = opened.epoch.epoch_id
        self._fallback_blocked.clear()
        return OpenEventReceipt(opened.epoch.epoch_id, False, False)

    def _write_claim_registry_members(
        self, cursor: Cursor[Any], event: DynamicEventPlan
    ) -> None:
        policy_row = cursor.execute(
            """
            SELECT claim_registry_snapshot_id, claim_count
            FROM groundloop_candidate_policy
            WHERE candidate_policy_id = %s
            """,
            (event.update.candidate_policy_id,),
        ).fetchone()
        expected = tuple(enumerate(event.registered_claim_ids))
        if policy_row is None or str(policy_row[0]) != event.claim_registry_snapshot_id:
            raise EventConflictError("candidate policy registry identity changed")
        if int(policy_row[1]) != len(expected):
            raise EventConflictError("candidate policy claim count changed")
        snapshot_hash = stable_m4_digest(
            "m4-claim-registry-snapshot-v1", *event.registered_claim_ids
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m4_claim_registry_snapshot VALUES (
                %s, %s, %s, now()
            ) ON CONFLICT DO NOTHING
            """,
            (
                event.claim_registry_snapshot_id,
                len(expected),
                snapshot_hash,
            ),
        )
        snapshot = cursor.execute(
            """
            SELECT claim_count, claim_set_hash
            FROM groundloop_m4_claim_registry_snapshot
            WHERE claim_registry_snapshot_id = %s
            """,
            (event.claim_registry_snapshot_id,),
        ).fetchone()
        if snapshot is None or (int(snapshot[0]), str(snapshot[1]).strip()) != (
            len(expected),
            snapshot_hash,
        ):
            raise EventConflictError("claim registry snapshot content changed")
        stored_count_row = cursor.execute(
            """
                SELECT count(*)
                FROM groundloop_m4_claim_registry_member
                WHERE claim_registry_snapshot_id = %s
                """,
            (event.claim_registry_snapshot_id,),
        ).fetchone()
        assert stored_count_row is not None
        stored_count = int(stored_count_row[0])
        if stored_count == 0:
            for ordinal, claim_id in expected:
                cursor.execute(
                    """
                    INSERT INTO groundloop_m4_claim_registry_member (
                        claim_registry_snapshot_id, claim_id, member_ordinal
                    ) VALUES (%s, %s, %s)
                    """,
                    (event.claim_registry_snapshot_id, claim_id, ordinal),
                )
        elif stored_count != len(expected):
            raise EventConflictError("claim registry membership is incomplete")
        if not self._measured:
            stored = tuple(
                (int(row[0]), str(row[1]))
                for row in cursor.execute(
                    """
                    SELECT member_ordinal, claim_id
                    FROM groundloop_m4_claim_registry_member
                    WHERE claim_registry_snapshot_id = %s
                    ORDER BY member_ordinal
                    """,
                    (event.claim_registry_snapshot_id,),
                ).fetchall()
            )
            if stored != expected:
                raise EventConflictError("claim registry snapshot content changed")

    def _write_structural_rows(
        self, cursor: Cursor[Any], epoch_id: int, payload: StructuralPayload
    ) -> None:
        old_id = payload.deactivated_document_version_id
        if old_id is not None:
            active = cursor.execute(
                """
                SELECT 1 FROM groundloop_document_version AS version
                JOIN groundloop_m4_update AS update_row ON update_row.epoch_id = %s
                JOIN groundloop_epoch AS creator
                  ON creator.epoch_id = version.valid_from_epoch
                 AND creator.semantic_status = 'sealed'
                WHERE version.document_version_id = %s
                  AND version.valid_from_epoch <=
                      update_row.previous_published_epoch_id
                  AND (
                      version.valid_to_epoch IS NULL
                      OR update_row.previous_published_epoch_id <
                         version.valid_to_epoch
                  )
                """,
                (epoch_id, old_id),
            ).fetchone()
            if active is None:
                raise InvalidEventError("deactivated document version is not active")
            cursor.execute(
                """
                INSERT INTO groundloop_m4_structural_deactivation (
                    epoch_id, document_version_id
                ) VALUES (%s, %s)
                """,
                (epoch_id, old_id),
            )
        inserted = payload.inserted
        if inserted is None:
            return
        previous_row = cursor.execute(
            """
            SELECT previous_published_epoch_id FROM groundloop_m4_update
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if previous_row is None or previous_row[0] is None:
            raise ValidationError("M4 insertion has no published structural base")
        prior_versions = cursor.execute(
            """
            SELECT version.document_version_id
            FROM groundloop_document_version AS version
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = version.valid_from_epoch
             AND creator.semantic_status = 'sealed'
            WHERE version.document_id = %s
              AND version.valid_from_epoch <= %s
              AND (version.valid_to_epoch IS NULL OR %s < version.valid_to_epoch)
            ORDER BY version.document_version_id
            """,
            (
                inserted.version.document_id,
                int(previous_row[0]),
                int(previous_row[0]),
            ),
        ).fetchall()
        expected_prior = (
            ()
            if payload.deactivated_document_version_id is None
            else ((payload.deactivated_document_version_id,),)
        )
        if tuple(prior_versions) != expected_prior:
            raise InvalidEventError(
                "inserted document identity has an undeclared published version"
            )
        cursor.execute(
            """
            INSERT INTO groundloop_document(document_id, source_uri, authority_class)
            VALUES (%s, NULL, 'unclassified')
            ON CONFLICT (document_id) DO NOTHING
            """,
            (inserted.version.document_id,),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m4_document_metadata_overlay (
                epoch_id, document_id, source_uri, authority_class
            ) VALUES (%s, %s, %s, %s)
            """,
            (
                epoch_id,
                inserted.version.document_id,
                inserted.source_uri,
                inserted.authority_class,
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_document_version (
                document_version_id, document_id, content_hash,
                valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, %s, NULL)
            """,
            (
                inserted.version.document_version_id,
                inserted.version.document_id,
                inserted.version.content_hash,
                epoch_id,
            ),
        )
        for chunk in inserted.chunks:
            cursor.execute(
                """
                INSERT INTO groundloop_chunk_version (
                    chunk_version_id, document_version_id, chunk_index, text,
                    text_hash, chunker_version, valid_from_epoch, valid_to_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                """,
                (
                    chunk.chunk_version_id,
                    chunk.document_version_id,
                    chunk.chunk_index,
                    chunk.text,
                    chunk.text_hash,
                    inserted.chunker_version,
                    epoch_id,
                ),
            )
            if inserted.chunker_artifact_id is not None:
                cursor.execute(
                    """
                    INSERT INTO groundloop_chunk_provenance (
                        chunk_version_id, chunker_artifact_id, input_hash
                    ) VALUES (%s, %s, %s)
                    """,
                    (
                        chunk.chunk_version_id,
                        inserted.chunker_artifact_id,
                        inserted.chunker_input_hash,
                    ),
                )

    def _write_withdrawal_overlay(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        withdrawal: StructuralWithdrawal,
    ) -> None:
        for observation_id in withdrawal.plan.observation_ids:
            row = cursor.execute(
                """
                SELECT subject_kind, subject_id, chunk_version_id, task_type
                FROM groundloop_semantic_observation
                WHERE observation_id = %s AND subject_kind = 'claim'
                """,
                (observation_id,),
            ).fetchone()
            if row is None:
                raise InvalidEventError("withdrawal observation is missing")
            cursor.execute(
                """
                INSERT INTO groundloop_working_observation_delta (
                    epoch_id, subject_kind, subject_id, chunk_version_id,
                    task_type, base_observation_id, working_observation_id,
                    installed_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, NULL, 1)
                """,
                (epoch_id, row[0], row[1], row[2], row[3], observation_id),
            )

    def _write_initial_evaluation(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        event: DynamicEventPlan,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...],
    ) -> None:
        head = event.update.previous_published_epoch_id
        claim_rows = cursor.execute(
            """
            SELECT claim_id FROM groundloop_m4_claim_registry_member
            WHERE claim_registry_snapshot_id = %s ORDER BY member_ordinal
            """,
            (event.claim_registry_snapshot_id,),
        ).fetchall()
        pending: set[str] = set()
        open_jobs_by_claim: dict[str, int] = {}
        scope_by_claim: dict[str, bool] = {}
        for (claim_id,) in claim_rows:
            claim = str(claim_id)
            open_count = sum(1 for job in root_jobs if job.target_claim_id == claim)
            scope_open = any(scope.contains(claim) for scope in discovery_scopes)
            state = "pending" if open_count or scope_open else "complete"
            if state == "pending":
                pending.add(claim)
            open_jobs_by_claim[claim] = open_count
            scope_by_claim[claim] = scope_open
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES
                    (%s, 'claim', %s, %s, %s, %s, %s, 1)
                """,
                (
                    epoch_id,
                    claim_id,
                    state,
                    head,
                    open_count,
                    scope_open,
                ),
            )
        answer_rows = cursor.execute(
            """
            SELECT DISTINCT claim.answer_version_id
            FROM groundloop_m4_claim_registry_member AS member
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE member.claim_registry_snapshot_id = %s
            ORDER BY claim.answer_version_id
            """,
            (event.claim_registry_snapshot_id,),
        ).fetchall()
        for (answer_id,) in answer_rows:
            required_rows = cursor.execute(
                """
                SELECT claim_id FROM groundloop_claim
                WHERE answer_version_id = %s AND required
                  AND claim_id = ANY(%s)
                ORDER BY claim_id
                """,
                (answer_id, list(event.registered_claim_ids)),
            ).fetchall()
            required = tuple(str(row[0]) for row in required_rows)
            open_count = sum(open_jobs_by_claim[claim_id] for claim_id in required)
            scope_open = any(scope_by_claim[claim_id] for claim_id in required)
            is_pending = any(claim_id in pending for claim_id in required)
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES
                    (%s, 'answer', %s, %s, %s, %s, %s, 1)
                """,
                (
                    epoch_id,
                    answer_id,
                    "pending" if is_pending else "complete",
                    head,
                    open_count,
                    scope_open,
                ),
            )

    def _write_initial_compact_evaluation(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        event: DynamicEventPlan,
        root_jobs: tuple[LogicalJobSpec, ...],
        discovery_scopes: tuple[DiscoveryScope, ...],
    ) -> None:
        self._write_compact_evaluation(
            cursor,
            epoch_id=epoch_id,
            revision=1,
            confirmed_as_of_epoch=event.update.previous_published_epoch_id,
            open_jobs=root_jobs,
            discovery_scope_open=bool(discovery_scopes),
            failed=False,
        )

    def _write_compact_evaluation(
        self,
        cursor: Cursor[Any],
        *,
        epoch_id: int,
        revision: int,
        confirmed_as_of_epoch: int | None,
        open_jobs: tuple[LogicalJobSpec, ...],
        discovery_scope_open: bool,
        failed: bool,
    ) -> None:
        default_state = (
            "failed" if failed else "pending" if discovery_scope_open else "complete"
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m4_evaluation_default VALUES (
                %s, %s, %s, %s, %s
            ) ON CONFLICT (epoch_id) DO UPDATE SET
                evaluation_state = EXCLUDED.evaluation_state,
                confirmed_as_of_epoch = EXCLUDED.confirmed_as_of_epoch,
                discovery_scope_open = EXCLUDED.discovery_scope_open,
                updated_revision = EXCLUDED.updated_revision
            """,
            (
                epoch_id,
                default_state,
                confirmed_as_of_epoch,
                discovery_scope_open,
                revision,
            ),
        )
        self._account(cursor, epoch_id, "evaluation_default_rows_written")
        cursor.execute(
            "DELETE FROM groundloop_object_evaluation WHERE epoch_id = %s",
            (epoch_id,),
        )
        if failed:
            return
        open_by_claim: dict[str, int] = {}
        for job in open_jobs:
            claim_id = (
                job.pair.claim_id if job.pair is not None else job.target_claim_id
            )
            if claim_id is not None:
                open_by_claim[claim_id] = open_by_claim.get(claim_id, 0) + 1
        if not open_by_claim:
            return
        answer_by_claim = {
            str(row[0]): (str(row[1]), bool(row[2]))
            for row in cursor.execute(
                """
                SELECT claim_id, answer_version_id, required
                FROM groundloop_claim
                WHERE claim_id = ANY(%s)
                """,
                (list(sorted(open_by_claim)),),
            ).fetchall()
        }
        for claim_id, open_count in sorted(open_by_claim.items()):
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES (
                    %s, 'claim', %s, 'pending', %s, %s, %s, %s
                )
                """,
                (
                    epoch_id,
                    claim_id,
                    confirmed_as_of_epoch,
                    open_count,
                    discovery_scope_open,
                    revision,
                ),
            )
        answer_counts: dict[str, int] = {}
        for claim_id, open_count in open_by_claim.items():
            answer = answer_by_claim.get(claim_id)
            if answer is not None and answer[1]:
                answer_id = answer[0]
                answer_counts[answer_id] = answer_counts.get(answer_id, 0) + open_count
        for answer_id, open_count in sorted(answer_counts.items()):
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES (
                    %s, 'answer', %s, 'pending', %s, %s, %s, %s
                )
                """,
                (
                    epoch_id,
                    answer_id,
                    confirmed_as_of_epoch,
                    open_count,
                    discovery_scope_open,
                    revision,
                ),
            )
        self._account(
            cursor,
            epoch_id,
            "evaluation_override_rows_written",
            len(open_by_claim) + len(answer_counts),
        )

    def chunk_is_active(self, chunk_version_id: str) -> bool:
        epoch_id = self._active_epoch_id
        if epoch_id is None:
            epoch_id = self._publication_head()
            row = self.connection.execute(
                """
                SELECT 1 FROM groundloop_chunk_version AS chunk
                JOIN groundloop_document_version AS version
                  ON version.document_version_id = chunk.document_version_id
                JOIN groundloop_epoch AS creator
                  ON creator.epoch_id = version.valid_from_epoch
                 AND creator.semantic_status = 'sealed'
                WHERE chunk_version_id = %s
                  AND chunk.valid_from_epoch <= %s
                  AND (
                      chunk.valid_to_epoch IS NULL OR %s < chunk.valid_to_epoch
                  )
                """,
                (chunk_version_id, epoch_id, epoch_id),
            ).fetchone()
            return row is not None
        state = (
            self.runtime_store.read_epoch_header_point(epoch_id).state
            if self._measured
            else self.runtime_store.read_epoch(epoch_id).state
        )
        if state is RuntimeEpochState.FAILED:
            return False
        row = self.connection.execute(
            """
            SELECT 1 FROM groundloop_m4_effective_chunk_version
            WHERE epoch_id = %s AND chunk_version_id = %s
            """,
            (epoch_id, chunk_version_id),
        ).fetchone()
        if self._measured:
            with self.connection.cursor() as cursor:
                self._account(cursor, epoch_id, "active_chunk_rows_examined", 1)
        return row is not None

    def pending_claim_ids(self, epoch_id: int) -> tuple[str, ...]:
        epoch = self.runtime_store.read_epoch(epoch_id)
        claim_ids = tuple(
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT member.claim_id
                FROM groundloop_m4_update AS update_row
                JOIN groundloop_m4_claim_registry_member AS member
                  ON member.claim_registry_snapshot_id =
                     update_row.registry_snapshot_id
                WHERE update_row.epoch_id = %s
                ORDER BY member.member_ordinal
                """,
                (epoch_id,),
            ).fetchall()
        )
        return tuple(
            claim_id
            for claim_id in claim_ids
            if claim_evaluation_state(epoch, claim_id).value == "pending"
        )

    @staticmethod
    def _completion_lease_binding(lease: JobLease, job_id: str) -> tuple[str, str, int]:
        if lease.job_id != job_id:
            raise EventConflictError("completion lease belongs to another job")
        if not lease.should_execute or lease.already_completed:
            raise EventConflictError("completion requires an executable job lease")
        if (
            lease.attempt_id is None
            or lease.lease_token_hash is None
            or lease.expected_revision is None
        ):
            raise ValidationError("executable lease binding is incomplete")
        return (
            lease.attempt_id,
            lease.lease_token_hash,
            lease.expected_revision,
        )

    def acquire_job(self, epoch_id: int, spec: LogicalJobSpec) -> JobLease:
        if self._measured:
            header = self.runtime_store.read_epoch_header_point(epoch_id)
            job = self.runtime_store.read_job_point(epoch_id, spec.job_id)
            if job.spec != spec:
                raise EventConflictError("requested job differs from persisted job")
            if job.state in {
                JobState.COMPLETED_ACTIVE,
                JobState.COMPLETED_INACTIVE,
            }:
                return JobLease(spec.job_id, False, True)
            if job.state in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
                first_attempt = job.state is JobState.DECLARED
                ordinal = (
                    1
                    if job.latest_attempt is None
                    else job.latest_attempt.attempt.attempt_ordinal + 1
                )
                attempt = JobAttempt(
                    attempt_id=stable_m4_digest(
                        "m4-job-attempt-v1", spec.job_id, str(ordinal)
                    ),
                    job_id=spec.job_id,
                    execution_spec_hash=spec.execution_spec_hash,
                    attempt_ordinal=ordinal,
                    lease_token_hash=stable_m4_digest(
                        "m4-lease-token-v1", spec.job_id, str(ordinal)
                    ),
                )
                with self.connection.transaction():
                    started = self.runtime_store.start_attempt_point(
                        epoch_id, header.revision, attempt
                    )
                    if not started.replayed:
                        self.evaluation_store.apply_transition(
                            epoch_id,
                            EvaluationTransition(
                                transition_id=attempt.attempt_id,
                                expected_revision=header.revision,
                                claim_job_deltas=(
                                    (
                                        ClaimJobDelta(
                                            job.spec.target_claim_id,
                                            1,
                                        ),
                                    )
                                    if first_attempt
                                    and job.spec.kind is JobKind.FRONTIER_RETRIEVE
                                    and job.spec.target_claim_id is not None
                                    else ()
                                ),
                            ),
                        )
                header = started.header
            elif job.state is JobState.RUNNING:
                if job.latest_attempt is None:
                    raise ValidationError("running job has no persisted attempt")
                attempt = job.latest_attempt.attempt
            else:
                raise InvalidEventError("job is not executable")
            return JobLease(
                spec.job_id,
                True,
                False,
                attempt_id=attempt.attempt_id,
                lease_token_hash=attempt.lease_token_hash,
                expected_revision=header.revision,
            )
        epoch = self.runtime_store.read_epoch(epoch_id)
        runtime_job = next(
            (item for item in epoch.jobs if item.spec.job_id == spec.job_id),
            None,
        )
        if runtime_job is None or runtime_job.spec != spec:
            raise EventConflictError("requested job differs from persisted job")
        if runtime_job.completion is not None:
            return JobLease(spec.job_id, False, True)
        if runtime_job.state in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
            ordinal = len(runtime_job.attempts) + 1
            legacy_attempt = JobAttempt(
                attempt_id=stable_m4_digest(
                    "m4-job-attempt-v1", spec.job_id, str(ordinal)
                ),
                job_id=spec.job_id,
                execution_spec_hash=spec.execution_spec_hash,
                attempt_ordinal=ordinal,
                lease_token_hash=stable_m4_digest(
                    "m4-lease-token-v1", spec.job_id, str(ordinal)
                ),
            )
            started_transition = self.runtime_store.start_attempt(
                epoch_id, legacy_attempt
            )
            epoch = next(
                item
                for item in started_transition.book.epochs
                if item.epoch_id == epoch_id
            )
        elif runtime_job.state is not JobState.RUNNING:
            raise InvalidEventError("job is not executable")
        else:
            if not runtime_job.attempts:
                raise ValidationError("running job has no persisted attempt")
            legacy_attempt = runtime_job.attempts[-1]
        return JobLease(
            spec.job_id,
            True,
            False,
            attempt_id=legacy_attempt.attempt_id,
            lease_token_hash=legacy_attempt.lease_token_hash,
            expected_revision=epoch.revision,
        )

    def _acquire_direct_job_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        spec: LogicalJobSpec,
        lease_token_hash: str,
        *,
        lease_expires_at: datetime | None = None,
    ) -> JobLease:
        """Persist deterministic direct dispatch in the caller's transaction."""
        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        header = self.runtime_store.read_epoch_header_point(epoch_id, cursor=cursor)
        if header.revision != expected_revision:
            raise EventConflictError("stale epoch revision")
        job = self.runtime_store.read_job_point(epoch_id, spec.job_id, cursor=cursor)
        if job.spec != spec:
            raise EventConflictError("requested job differs from persisted job")
        if job.state in {
            JobState.COMPLETED_ACTIVE,
            JobState.COMPLETED_INACTIVE,
        }:
            return JobLease(spec.job_id, False, True)
        if job.state in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
            first_attempt = job.state is JobState.DECLARED
            ordinal = (
                1
                if job.latest_attempt is None
                else job.latest_attempt.attempt.attempt_ordinal + 1
            )
            expected_token = stable_m4_digest(
                "m4-lease-token-v1", spec.job_id, str(ordinal)
            )
            if lease_token_hash != expected_token:
                raise EventConflictError(
                    "direct acquisition lease token is not deterministic"
                )
            attempt = JobAttempt(
                attempt_id=stable_m4_digest(
                    "m4-job-attempt-v1", spec.job_id, str(ordinal)
                ),
                job_id=spec.job_id,
                execution_spec_hash=spec.execution_spec_hash,
                attempt_ordinal=ordinal,
                lease_token_hash=lease_token_hash,
            )
            if lease_expires_at is None:
                started = self.runtime_store.start_attempt_point_local(
                    cursor, epoch_id, expected_revision, attempt
                )
            else:
                started = self.runtime_store.start_recovery_attempt_point_local(
                    cursor,
                    epoch_id,
                    expected_revision,
                    attempt,
                    lease_expires_at=lease_expires_at,
                )
            if not started.replayed:
                self.evaluation_store.apply_transition_local(
                    cursor,
                    epoch_id,
                    EvaluationTransition(
                        transition_id=attempt.attempt_id,
                        expected_revision=expected_revision,
                        claim_job_deltas=(
                            (ClaimJobDelta(job.spec.target_claim_id, 1),)
                            if first_attempt
                            and job.spec.kind is JobKind.FRONTIER_RETRIEVE
                            and job.spec.target_claim_id is not None
                            else ()
                        ),
                    ),
                )
            header = started.header
        elif job.state is JobState.RUNNING:
            if job.latest_attempt is None:
                raise ValidationError("running job has no persisted attempt")
            attempt = job.latest_attempt.attempt
            if lease_token_hash != attempt.lease_token_hash:
                raise EventConflictError(
                    "direct acquisition lease token differs from active attempt"
                )
        else:
            raise InvalidEventError("job is not executable")
        return JobLease(
            spec.job_id,
            True,
            False,
            attempt_id=attempt.attempt_id,
            lease_token_hash=attempt.lease_token_hash,
            expected_revision=header.revision,
        )

    def _takeover_direct_job_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        spec: LogicalJobSpec,
        expired_attempt_id: str,
        successor: JobAttempt,
        *,
        decision_time: datetime,
        lease_expires_at: datetime,
    ) -> JobLease:
        """Stage the frozen dense successor for one M5-D24 direct takeover."""

        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        if successor.job_id != spec.job_id:
            raise EventConflictError("typed-direct successor belongs to another job")
        result = self.runtime_store.takeover_attempt_point_local(
            cursor,
            epoch_id,
            expected_revision,
            expired_attempt_id,
            successor,
            decision_time=decision_time,
            lease_expires_at=lease_expires_at,
        )
        self.evaluation_store.apply_transition_local(
            cursor,
            epoch_id,
            EvaluationTransition(
                transition_id=successor.attempt_id,
                expected_revision=expected_revision,
            ),
        )
        return JobLease(
            spec.job_id,
            True,
            False,
            attempt_id=successor.attempt_id,
            lease_token_hash=successor.lease_token_hash,
            expected_revision=result.header.revision,
        )

    @staticmethod
    def _typed_failure_xact_identity(cursor: Cursor[Any]) -> str:
        row = cursor.execute("SELECT pg_current_xact_id()::text").fetchone()
        if row is None or type(row[0]) is not str or not row[0]:
            raise ValidationError("typed failure requires a live PostgreSQL xact")
        return row[0]

    @staticmethod
    def _validate_typed_failure_attempt_image(
        image: PointAttemptRecord,
        *,
        job: LogicalJobSpec,
    ) -> None:
        if type(image) is not PointAttemptRecord:
            raise ValidationError("typed failure latest attempt has another type")
        attempt = image.attempt
        if type(attempt) is not JobAttempt:
            raise ValidationError("typed failure attempt has another type")
        if (
            attempt.job_id != job.job_id
            or attempt.execution_spec_hash != job.execution_spec_hash
            or attempt.attempt_id
            != stable_m4_digest(
                "m4-job-attempt-v1", job.job_id, str(attempt.attempt_ordinal)
            )
            or attempt.lease_token_hash
            != stable_m4_digest(
                "m4-lease-token-v1", job.job_id, str(attempt.attempt_ordinal)
            )
            or type(image.state) is not str
            or image.state not in {"leased", "completed", "failed", "expired"}
            or type(image.lease_expires_at) is not datetime
        ):
            raise ValidationError("typed failure attempt image is not exact M4-v1")
        _canonical_typed_failure_authority_value(image)

    @classmethod
    def _validate_typed_failure_job_locks_value(
        cls,
        locks: _TypedDirectEpochFailureJobLocks,
    ) -> None:
        if type(locks) is not _TypedDirectEpochFailureJobLocks:
            raise ValidationError("typed failure job locks have another type")
        if (
            type(locks.epoch_id) is not int
            or locks.epoch_id < 1
            or type(locks.expected_revision) is not int
            or locks.expected_revision < 1
            or type(locks.jobs) is not tuple
        ):
            raise ValidationError("typed failure job locks are malformed")
        job_ids: list[str] = []
        for image in locks.jobs:
            if type(image) is not _TypedDirectEpochFailureJobImage:
                raise ValidationError("typed failure job image has another type")
            if type(image.job) is not LogicalJobSpec:
                raise ValidationError("typed failure job spec has another type")
            _canonical_typed_failure_authority_value(image.job)
            if type(image.state) is not JobState:
                raise ValidationError("typed failure job state has another type")
            if image.completed_revision is not None and (
                type(image.completed_revision) is not int
                or image.completed_revision < 1
            ):
                raise ValidationError(
                    "typed failure job completion revision is malformed"
                )
            if image.completion_digest is not None and (
                type(image.completion_digest) is not str
                or len(image.completion_digest) != 64
            ):
                raise ValidationError(
                    "typed failure job completion digest is malformed"
                )
            if image.latest_attempt is not None:
                cls._validate_typed_failure_attempt_image(
                    image.latest_attempt, job=image.job
                )
            job_ids.append(image.job.job_id)
        if tuple(job_ids) != tuple(sorted(set(job_ids))):
            raise ValidationError(
                "typed failure jobs must be C-byte ordered and unique"
            )

    @classmethod
    def _typed_failure_job_locks_snapshot(
        cls,
        locks: _TypedDirectEpochFailureJobLocks,
    ) -> tuple[object, ...]:
        cls._validate_typed_failure_job_locks_value(locks)
        snapshot = _canonical_typed_failure_authority_value(locks)
        if type(snapshot) is not tuple:
            raise AssertionError("typed failure lock snapshot must be a tuple")
        return snapshot

    @classmethod
    def _typed_failure_plan_snapshot(
        cls,
        plan: _TypedDirectEpochFailureLockedPlan,
    ) -> tuple[object, ...]:
        if type(plan) is not _TypedDirectEpochFailureLockedPlan:
            raise ValidationError("typed failure locked plan has another type")
        cls._validate_typed_failure_job_locks_value(plan.job_locks)
        if type(plan.cancelled_jobs) is not tuple:
            raise ValidationError("typed failure cancellations must be an exact tuple")
        for job in plan.cancelled_jobs:
            if type(job) is not LogicalJobSpec:
                raise ValidationError("typed failure cancellation has another type")
        if (plan.target_job is None) != (plan.target_attempt is None):
            raise ValidationError(
                "typed failure target job and attempt must be jointly present"
            )
        if plan.target_job is not None:
            if (
                type(plan.target_job) is not LogicalJobSpec
                or type(plan.target_attempt) is not JobAttempt
            ):
                raise ValidationError("typed failure target has another type")
        snapshot = _canonical_typed_failure_authority_value(plan)
        if type(snapshot) is not tuple:
            raise AssertionError("typed failure plan snapshot must be a tuple")
        return snapshot

    def _lock_typed_direct_epoch_failure_jobs_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
    ) -> _TypedDirectEpochFailureJobLocks:
        """Tier-9 read-only lock of the complete M4 direct-job set."""

        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        if type(epoch_id) is not int or epoch_id < 1:
            raise ValidationError("typed failure epoch_id must be positive")
        if type(expected_revision) is not int or expected_revision < 1:
            raise ValidationError("typed failure expected revision must be positive")
        stored = self.runtime_store.lock_typed_failure_jobs_point_local(
            cursor, epoch_id
        )
        images: list[_TypedDirectEpochFailureJobImage] = []
        for item in stored:
            if type(item) is not _TypedFailurePointJobImage:
                raise ValidationError("typed failure store returned another image")
            images.append(
                _TypedDirectEpochFailureJobImage(
                    job=item.job,
                    state=item.state,
                    completed_revision=item.completed_revision,
                    completion_digest=item.completion_digest,
                    latest_attempt=item.latest_attempt,
                )
            )
        locks = _TypedDirectEpochFailureJobLocks(
            epoch_id=epoch_id,
            expected_revision=expected_revision,
            jobs=tuple(images),
        )
        self._validate_typed_failure_job_locks_value(locks)
        xact = self._typed_failure_xact_identity(cursor)
        self._typed_failure_job_lock_authority[id(locks)] = (
            id(cursor),
            xact,
            locks,
            self._typed_failure_job_locks_snapshot(locks),
        )
        return locks

    def _lock_typed_direct_epoch_failure_details_local(
        self,
        cursor: Cursor[Any],
        job_locks: _TypedDirectEpochFailureJobLocks,
        target_lease: JobLease | None,
    ) -> _TypedDirectEpochFailureLockedPlan:
        """Lock M4 attempts/details and derive the immutable write plan."""

        xact = self._typed_failure_xact_identity(cursor)
        authority = self._typed_failure_job_lock_authority.pop(id(job_locks), None)
        if (
            authority is None
            or authority[0] != id(cursor)
            or authority[1] != xact
            or authority[2] is not job_locks
            or authority[3] != self._typed_failure_job_locks_snapshot(job_locks)
        ):
            raise EventConflictError(
                "typed failure job locks are mutated, copied, stale, or cross-cursor"
            )

        attempts = self.runtime_store.lock_typed_failure_attempts_point_local(
            cursor, job_locks.epoch_id
        )
        attempts_by_job: dict[str, list[PointAttemptRecord]] = {
            image.job.job_id: [] for image in job_locks.jobs
        }
        for attempt_image in attempts:
            job_attempts = attempts_by_job.get(attempt_image.attempt.job_id)
            if job_attempts is None:
                raise ValidationError("typed failure attempt has no locked job")
            job_attempts.append(attempt_image)
        for image in job_locks.jobs:
            job_attempts = attempts_by_job[image.job.job_id]
            for ordinal, attempt_image in enumerate(job_attempts, start=1):
                self._validate_typed_failure_attempt_image(attempt_image, job=image.job)
                if attempt_image.attempt.attempt_ordinal != ordinal:
                    raise ValidationError(
                        "typed failure attempt ordinals are not dense"
                    )
            latest = None if not job_attempts else job_attempts[-1]
            if latest != image.latest_attempt:
                raise EventConflictError(
                    "typed failure latest attempt changed after job locking"
                )

        # These are later detail-tier locks. Staging subsequently reselects
        # the already-held rows through unchanged helpers but acquires no
        # earlier job/attempt authority.
        update_row = cursor.execute(
            """
            SELECT epoch_id, manifest
            FROM groundloop_m4_update
            WHERE epoch_id = %s
            FOR UPDATE
            """,
            (job_locks.epoch_id,),
        ).fetchone()
        evaluation_row = cursor.execute(
            """
            SELECT lifecycle_state, revision
            FROM groundloop_m4_evaluation_epoch_counter
            WHERE epoch_id = %s
            FOR UPDATE
            """,
            (job_locks.epoch_id,),
        ).fetchone()
        if update_row is None:
            raise InvalidEventError("typed failure lacks its M4 update")
        if type(update_row[0]) is not int or update_row[0] != job_locks.epoch_id:
            raise ValidationError("typed failure M4 update identity is malformed")
        manifest = update_row[1]
        if type(manifest) is not dict:
            raise ValidationError("typed failure M4 update manifest is malformed")
        runtime_metadata = manifest.get(_RUNTIME_MANIFEST_KEY)
        if (
            type(runtime_metadata) is not dict
            or "failure_reason" not in runtime_metadata
        ):
            raise ValidationError(
                "typed failure M4 update lacks exact runtime failure metadata"
            )
        locked_failure_reason = runtime_metadata["failure_reason"]
        if locked_failure_reason is not None and (
            type(locked_failure_reason) is not str or not locked_failure_reason.strip()
        ):
            raise ValidationError("typed failure M4 projection reason is malformed")
        evaluation_image = None if evaluation_row is None else tuple(evaluation_row)
        if evaluation_image not in {
            ("active", job_locks.expected_revision),
            ("failed", job_locks.expected_revision + 1),
        }:
            raise EventConflictError(
                "typed failure evaluation image differs from locked revision"
            )
        if evaluation_image == ("active", job_locks.expected_revision):
            if locked_failure_reason is not None:
                raise EventConflictError(
                    "active typed failure plan already has an M4 failure reason"
                )
        elif type(locked_failure_reason) is not str:
            raise EventConflictError(
                "terminal typed failure plan lacks its M4 failure reason"
            )

        target_job: LogicalJobSpec | None = None
        target_attempt: JobAttempt | None = None
        target_id: str | None = None
        if target_lease is not None:
            if type(target_lease) is not JobLease:
                raise ValidationError("typed failure target lease has another type")
            if (
                target_lease.should_execute is not True
                or target_lease.already_completed is not False
                or type(target_lease.attempt_id) is not str
                or type(target_lease.lease_token_hash) is not str
                or type(target_lease.expected_revision) is not int
                or target_lease.expected_revision != job_locks.expected_revision
            ):
                raise ValidationError(
                    "typed failure target requires an exact executable M4 lease"
                )
            target_id = target_lease.job_id
            target_image = next(
                (
                    image
                    for image in job_locks.jobs
                    if image.job.job_id == target_lease.job_id
                ),
                None,
            )
            if target_image is None or target_image.latest_attempt is None:
                raise EventConflictError("typed failure target is not in the job set")
            latest = target_image.latest_attempt
            if (
                latest.attempt.attempt_id != target_lease.attempt_id
                or latest.attempt.lease_token_hash != target_lease.lease_token_hash
                or target_image.state
                not in {JobState.RUNNING, JobState.TERMINAL_FAILED, JobState.CANCELLED}
                or (target_image.state is JobState.RUNNING and latest.state != "leased")
                or (
                    target_image.state is JobState.TERMINAL_FAILED
                    and latest.state != "failed"
                )
                or (
                    target_image.state is JobState.CANCELLED
                    and latest.state != "leased"
                )
            ):
                raise EventConflictError(
                    "typed failure target lease differs from its locked attempt"
                )
            target_job = target_image.job
            target_attempt = latest.attempt

        open_states = {
            JobState.DECLARED,
            JobState.RUNNING,
            JobState.RETRYABLE_FAILED,
        }
        cancelled_jobs = tuple(
            image.job
            for image in job_locks.jobs
            if image.job.job_id != target_id and image.state in open_states
        )
        plan = _TypedDirectEpochFailureLockedPlan(
            job_locks=job_locks,
            target_job=target_job,
            target_attempt=target_attempt,
            cancelled_jobs=cancelled_jobs,
        )
        self._typed_failure_plan_authority[id(plan)] = (
            id(cursor),
            xact,
            plan,
            self._typed_failure_plan_snapshot(plan),
            locked_failure_reason,
        )
        return plan

    def _stage_typed_direct_epoch_failure_local(
        self,
        cursor: Cursor[Any],
        locked_plan: _TypedDirectEpochFailureLockedPlan,
        failure_reason: str,
        target_terminal_reason: str | None,
    ) -> _TypedDirectEpochFailureStage:
        """Write only the fully locked M4 target/cancellation/failure plan."""

        xact = self._typed_failure_xact_identity(cursor)
        authority = self._typed_failure_plan_authority.pop(id(locked_plan), None)
        if (
            authority is None
            or authority[0] != id(cursor)
            or authority[1] != xact
            or authority[2] is not locked_plan
            or authority[3] != self._typed_failure_plan_snapshot(locked_plan)
        ):
            raise EventConflictError(
                "typed failure locked plan is mutated, copied, stale, or cross-cursor"
            )
        if authority[4] is not None:
            raise EventConflictError(
                "typed failure first-write plan already records an M4 failure reason"
            )
        if type(failure_reason) is not str or not failure_reason.strip():
            raise ValidationError("typed M4 failure reason must be exact nonempty text")
        target_present = locked_plan.target_job is not None
        if target_present != (
            locked_plan.target_attempt is not None
        ) or target_present != (target_terminal_reason is not None):
            raise ValidationError(
                "typed failure target job, attempt, and reason are jointly present"
            )
        if target_terminal_reason is not None and (
            type(target_terminal_reason) is not str
            or not target_terminal_reason.strip()
        ):
            raise ValidationError(
                "typed direct terminal reason must be exact nonempty text"
            )

        image_by_id = {image.job.job_id: image for image in locked_plan.job_locks.jobs}
        target_job_id: str | None = None
        target_attempt_id: str | None = None
        if locked_plan.target_job is not None:
            assert locked_plan.target_attempt is not None
            target_job_id = locked_plan.target_job.job_id
            target_attempt_id = locked_plan.target_attempt.attempt_id
            target_image = image_by_id.get(target_job_id)
            if (
                target_image is None
                or target_image.state is not JobState.RUNNING
                or target_image.latest_attempt is None
                or target_image.latest_attempt.state != "leased"
                or target_image.latest_attempt.attempt != locked_plan.target_attempt
            ):
                raise EventConflictError(
                    "typed failure target is not the locked executable attempt"
                )

        open_states = {
            JobState.DECLARED,
            JobState.RUNNING,
            JobState.RETRYABLE_FAILED,
        }
        expected_cancelled = tuple(
            image.job
            for image in locked_plan.job_locks.jobs
            if image.job.job_id != target_job_id and image.state in open_states
        )
        if locked_plan.cancelled_jobs != expected_cancelled:
            raise EventConflictError("typed failure cancellation plan is incomplete")
        cancelled_states = tuple(
            (job.job_id, image_by_id[job.job_id].state)
            for job in locked_plan.cancelled_jobs
        )
        self.runtime_store.apply_typed_failure_jobs_point_local(
            cursor,
            epoch_id=locked_plan.job_locks.epoch_id,
            expected_revision=locked_plan.job_locks.expected_revision,
            target_job_id=target_job_id,
            target_attempt_id=target_attempt_id,
            cancelled_job_states=cancelled_states,
        )
        transition = EvaluationTransition(
            transition_id=stable_m4_digest(
                "m4-evaluation-failure-v1",
                str(locked_plan.job_locks.epoch_id),
                failure_reason,
            ),
            expected_revision=locked_plan.job_locks.expected_revision,
            kind=EvaluationTransitionKind.FAIL,
        )
        receipt = self.evaluation_store.apply_transition_local(
            cursor, locked_plan.job_locks.epoch_id, transition
        )
        if receipt.replayed:
            raise EventConflictError(
                "typed failure first-write plan encountered an M4 replay"
            )
        header = self.runtime_store.stage_typed_failure_projection_point_local(
            cursor,
            epoch_id=locked_plan.job_locks.epoch_id,
            expected_revision=locked_plan.job_locks.expected_revision,
            reason=failure_reason,
        )
        return _TypedDirectEpochFailureStage(
            target_job=locked_plan.target_job,
            target_attempt=locked_plan.target_attempt,
            cancelled_jobs=locked_plan.cancelled_jobs,
            resulting_revision=header.revision,
        )

    def _discard_typed_direct_epoch_failure_plan_local(
        self,
        cursor: Cursor[Any],
        locked_plan: _TypedDirectEpochFailureLockedPlan,
        terminal_failure_reason: str,
    ) -> None:
        """Consume a terminal plan after matching its durable M4 reason."""

        authority = self._typed_failure_plan_authority.pop(id(locked_plan), None)
        if (
            authority is None
            or authority[0] != id(cursor)
            or authority[1] != self._typed_failure_xact_identity(cursor)
            or authority[2] is not locked_plan
            or authority[3] != self._typed_failure_plan_snapshot(locked_plan)
        ):
            raise EventConflictError(
                "typed failure terminal plan is mutated, copied, stale, or cross-cursor"
            )
        if locked_plan.cancelled_jobs:
            raise EventConflictError(
                "typed failure terminal plan still has open direct jobs"
            )
        if (
            type(terminal_failure_reason) is not str
            or not terminal_failure_reason.strip()
        ):
            raise ValidationError(
                "typed failure canonical terminal reason must be exact nonempty text"
            )
        if authority[4] != terminal_failure_reason:
            raise EventConflictError(
                "typed failure M4 projection records another reason"
            )

    def mark_retryable_failure(self, epoch_id: int, lease: JobLease) -> None:
        """Atomically retain one failed attempt as retryable semantic work.

        Retry changes the coordination revision but not the number of open
        semantic jobs.  Measured mode therefore advances the signed evaluation
        overlay with a zero-delta transition in the same database transaction.
        """
        attempt_id, _lease_token_hash, lease_revision = self._completion_lease_binding(
            lease, lease.job_id
        )
        if self._measured:
            transition_id = stable_m4_digest(
                "m4-evaluation-retryable-failure-v1",
                str(epoch_id),
                lease.job_id,
                attempt_id,
            )
            with self.connection.transaction():
                failed = self.runtime_store.mark_retryable_failure_point(
                    epoch_id,
                    lease_revision,
                    lease.job_id,
                    attempt_id,
                )
                self._inject("retryable_runtime_failed")
                if not failed.replayed:
                    self.evaluation_store.apply_transition(
                        epoch_id,
                        EvaluationTransition(
                            transition_id=transition_id,
                            expected_revision=lease_revision,
                        ),
                    )
            return

        transition = self.runtime_store.mark_retryable_failure(
            epoch_id, lease.job_id, attempt_id
        )
        if not transition.replayed:
            with self.connection.transaction():
                with self.connection.cursor() as cursor:
                    self._sync_evaluation(cursor, epoch_id)

    def _mark_direct_retryable_failure_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: JobLease,
    ) -> None:
        """Stage one retryable direct failure without owning the transaction."""

        attempt_id, _token, _lease_revision = self._completion_lease_binding(
            lease, lease.job_id
        )
        transition_id = stable_m4_digest(
            "m4-evaluation-retryable-failure-v1",
            str(epoch_id),
            lease.job_id,
            attempt_id,
        )
        result = self.runtime_store.mark_retryable_failure_point_local(
            cursor,
            epoch_id,
            expected_revision,
            lease.job_id,
            attempt_id,
        )
        if not result.replayed:
            self.evaluation_store.apply_transition_local(
                cursor,
                epoch_id,
                EvaluationTransition(
                    transition_id=transition_id,
                    expected_revision=expected_revision,
                ),
            )

    def _mark_direct_terminal_failure_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: JobLease,
    ) -> None:
        """Stage the M4 half of terminal direct failure under an outer owner."""

        attempt_id, _token, _lease_revision = self._completion_lease_binding(
            lease, lease.job_id
        )
        result = self.runtime_store.mark_terminal_failure_point_local(
            cursor,
            epoch_id,
            expected_revision,
            lease.job_id,
            attempt_id,
        )
        if not result.replayed:
            self.evaluation_store.apply_transition_local(
                cursor,
                epoch_id,
                EvaluationTransition(
                    transition_id=stable_m4_digest(
                        "m4-evaluation-terminal-failure-v1",
                        str(epoch_id),
                        lease.job_id,
                        attempt_id,
                    ),
                    expected_revision=expected_revision,
                ),
            )

    def complete_expansion(
        self,
        epoch_id: int,
        lease: JobLease,
        discovery: DiscoveryResult,
        completion: JobCompletion,
        child_jobs: tuple[LogicalJobSpec, ...],
    ) -> None:
        attempt_id, lease_token_hash, lease_revision = self._completion_lease_binding(
            lease, completion.job_id
        )
        if self._measured:
            with self.connection.transaction():
                with self.connection.cursor() as cursor:
                    self._persist_discovery_result(
                        cursor, epoch_id, completion.job_id, discovery
                    )
                self._inject("expansion_discovery_persisted")
                header = self.runtime_store.read_epoch_header_point(epoch_id)
                parent = self.runtime_store.read_job_point(epoch_id, completion.job_id)
                point_transition = self.runtime_store.complete_point(
                    CompletionPlan(
                        epoch_id,
                        header.revision,
                        completion,
                        child_jobs,
                    ),
                    attempt_id=attempt_id,
                    lease_token_hash=lease_token_hash,
                    lease_expected_revision=lease_revision,
                )
                self._inject("expansion_runtime_completed")
                if not point_transition.replayed:
                    deltas = tuple(
                        ClaimJobDelta(child.pair.claim_id, 1)
                        for child in child_jobs
                        if child.pair is not None
                    )
                    if (
                        parent.spec.kind is JobKind.FRONTIER_RETRIEVE
                        and parent.spec.target_claim_id is not None
                    ):
                        deltas += (ClaimJobDelta(parent.spec.target_claim_id, -1),)
                    self.evaluation_store.apply_transition(
                        epoch_id,
                        EvaluationTransition(
                            transition_id=completion.completion_digest,
                            expected_revision=header.revision,
                            scope_delta=(
                                -1
                                if parent.spec.kind is JobKind.IMPACT_DISCOVERY
                                else 0
                            ),
                            claim_job_deltas=deltas,
                        ),
                    )
                    self._inject("expansion_evaluation_synced")
            return
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                self._persist_discovery_result(
                    cursor, epoch_id, completion.job_id, discovery
                )
            self._inject("expansion_discovery_persisted")
            epoch = self.runtime_store.read_epoch(epoch_id)
            completed_job = next(
                job for job in epoch.jobs if job.spec.job_id == completion.job_id
            )
            relevant_chunks = tuple(
                chunk_id
                for chunk_id in (
                    completed_job.spec.target_chunk_version_id,
                    completed_job.spec.pair.chunk_version_id
                    if completed_job.spec.pair is not None
                    else None,
                )
                if chunk_id is not None
            )
            transition = self.runtime_store.complete(
                CompletionPlan(epoch_id, epoch.revision, completion, child_jobs),
                active_chunk_ids=self._active_chunk_ids(
                    epoch_id, relevant_chunks if self._measured else None
                ),
                attempt_id=attempt_id,
                lease_token_hash=lease_token_hash,
                lease_expected_revision=lease_revision,
            )
            self._inject("expansion_runtime_completed")
            if not transition.replayed:
                with self.connection.cursor() as cursor:
                    self._sync_evaluation(cursor, epoch_id)
                self._inject("expansion_evaluation_synced")

    def _stage_direct_expansion_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: JobLease,
        discovery: DiscoveryResult,
        completion: JobCompletion,
        child_jobs: tuple[LogicalJobSpec, ...],
    ) -> None:
        """Stage one direct discovery completion without transaction ownership."""
        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        attempt_id, lease_token_hash, lease_revision = self._completion_lease_binding(
            lease, completion.job_id
        )
        header = self.runtime_store.read_epoch_header_point(epoch_id, cursor=cursor)
        if header.revision != expected_revision:
            raise EventConflictError("stale epoch revision")
        parent = self.runtime_store.read_job_point(
            epoch_id, completion.job_id, cursor=cursor
        )
        self._persist_discovery_result(cursor, epoch_id, completion.job_id, discovery)
        self._inject("expansion_discovery_persisted")
        point_transition = self.runtime_store.complete_point_local(
            cursor,
            CompletionPlan(
                epoch_id,
                expected_revision,
                completion,
                child_jobs,
            ),
            attempt_id=attempt_id,
            lease_token_hash=lease_token_hash,
            lease_expected_revision=lease_revision,
        )
        self._inject("expansion_runtime_completed")
        if point_transition.replayed:
            return
        deltas = tuple(
            ClaimJobDelta(child.pair.claim_id, 1)
            for child in child_jobs
            if child.pair is not None
        )
        if (
            parent.spec.kind is JobKind.FRONTIER_RETRIEVE
            and parent.spec.target_claim_id is not None
        ):
            deltas += (ClaimJobDelta(parent.spec.target_claim_id, -1),)
        self.evaluation_store.apply_transition_local(
            cursor,
            epoch_id,
            EvaluationTransition(
                transition_id=completion.completion_digest,
                expected_revision=expected_revision,
                scope_delta=(-1 if parent.spec.kind is JobKind.IMPACT_DISCOVERY else 0),
                claim_job_deltas=deltas,
            ),
        )
        self._inject("expansion_evaluation_synced")

    @staticmethod
    def _admitted_pair_id(admitted: AdmittedPair) -> str:
        return stable_m4_digest(
            "m4-admitted-pair-v1",
            str(admitted.epoch_id),
            admitted.pair.claim_id,
            admitted.pair.chunk_version_id,
            admitted.candidate_policy_id,
        )

    @staticmethod
    def _channel_set_hash(hits: tuple[ChannelHit, ...]) -> str:
        identities = tuple(
            sorted(
                stable_m4_digest(
                    "m4-discovery-channel-v1",
                    str(hit.epoch_id),
                    hit.pair.claim_id,
                    hit.pair.chunk_version_id,
                    hit.candidate_policy_id,
                    hit.channel.value,
                    str(hit.rank),
                    "" if hit.score is None else format(hit.score, ".17g"),
                    hit.channel_artifact_hash,
                )
                for hit in hits
            )
        )
        return stable_m4_digest("m4-discovery-channel-set-v1", *identities)

    @classmethod
    def _admitted_pair_set_hash(cls, admitted_pairs: tuple[AdmittedPair, ...]) -> str:
        identities = tuple(
            sorted(cls._admitted_pair_id(item) for item in admitted_pairs)
        )
        return stable_m4_digest("m4-discovery-admitted-set-v1", *identities)

    def _persist_discovery_result(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        root_job_id: str,
        discovery: DiscoveryResult,
    ) -> None:
        if discovery.root_job_id != root_job_id:
            raise EventConflictError("discovery result belongs to another root")
        for hit in discovery.channel_hits:
            self._persist_channel_hit(cursor, epoch_id, hit)
        for admitted in discovery.admitted_pairs:
            self._persist_admitted_pair(cursor, epoch_id, admitted, discovery)
        channel_hash = self._channel_set_hash(discovery.channel_hits)
        pair_hash = self._admitted_pair_set_hash(discovery.admitted_pairs)
        cursor.execute(
            """
            INSERT INTO groundloop_m4_discovery_result (
                root_job_id, epoch_id, result_artifact_id,
                result_artifact_hash, fallback_satisfied,
                channel_hit_count, admitted_pair_count,
                channel_set_hash, admitted_pair_set_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (root_job_id) DO NOTHING
            """,
            (
                root_job_id,
                epoch_id,
                discovery.result_artifact_id,
                discovery.result_artifact_hash,
                discovery.fallback_satisfied,
                len(discovery.channel_hits),
                len(discovery.admitted_pairs),
                channel_hash,
                pair_hash,
            ),
        )
        row = cursor.execute(
            """
            SELECT epoch_id, result_artifact_id, result_artifact_hash,
                   fallback_satisfied, channel_hit_count,
                   admitted_pair_count, channel_set_hash,
                   admitted_pair_set_hash
            FROM groundloop_m4_discovery_result WHERE root_job_id = %s
            """,
            (root_job_id,),
        ).fetchone()
        expected = (
            epoch_id,
            discovery.result_artifact_id,
            discovery.result_artifact_hash,
            discovery.fallback_satisfied,
            len(discovery.channel_hits),
            len(discovery.admitted_pairs),
            channel_hash,
            pair_hash,
        )
        actual = (
            None
            if row is None
            else tuple(
                str(value).strip() if index in {2, 6, 7} else value
                for index, value in enumerate(row)
            )
        )
        if actual != expected:
            raise EventConflictError(
                "discovery-result identity was reused with different content"
            )

    def _persist_channel_hit(
        self, cursor: Cursor[Any], epoch_id: int, hit: ChannelHit
    ) -> None:
        if hit.epoch_id != epoch_id:
            raise EventConflictError("channel hit belongs to another epoch")
        cursor.execute(
            """
            INSERT INTO groundloop_impact_channel_hit (
                epoch_id, chunk_version_id, claim_id,
                candidate_policy_id, channel, rank, score,
                channel_artifact_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                hit.epoch_id,
                hit.pair.chunk_version_id,
                hit.pair.claim_id,
                hit.candidate_policy_id,
                hit.channel.value,
                hit.rank,
                hit.score,
                hit.channel_artifact_hash,
            ),
        )
        row = cursor.execute(
            """
            SELECT rank, score, channel_artifact_hash
            FROM groundloop_impact_channel_hit
            WHERE epoch_id = %s AND chunk_version_id = %s
              AND claim_id = %s AND candidate_policy_id = %s
              AND channel = %s
            """,
            (
                hit.epoch_id,
                hit.pair.chunk_version_id,
                hit.pair.claim_id,
                hit.candidate_policy_id,
                hit.channel.value,
            ),
        ).fetchone()
        expected = (hit.rank, hit.score, hit.channel_artifact_hash)
        actual = (
            None
            if row is None
            else (
                int(row[0]),
                None if row[1] is None else float(row[1]),
                str(row[2]).strip(),
            )
        )
        if actual != expected:
            raise EventConflictError(
                "channel-hit identity was reused with different content"
            )

    def _persist_admitted_pair(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        admitted: AdmittedPair,
        discovery: DiscoveryResult,
    ) -> None:
        if admitted.epoch_id != epoch_id:
            raise EventConflictError("admitted pair belongs to another epoch")
        admitted_id = self._admitted_pair_id(admitted)
        cursor.execute(
            """
            INSERT INTO groundloop_admitted_pair (
                admitted_pair_id, epoch_id, chunk_version_id, claim_id,
                candidate_policy_id, fused_rank, reasons,
                mandatory_lineage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (admitted_pair_id) DO NOTHING
            """,
            (
                admitted_id,
                admitted.epoch_id,
                admitted.pair.chunk_version_id,
                admitted.pair.claim_id,
                admitted.candidate_policy_id,
                admitted.fused_rank,
                [reason.value for reason in admitted.reasons],
                admitted.mandatory_lineage,
            ),
        )
        row = cursor.execute(
            """
            SELECT epoch_id, chunk_version_id, claim_id,
                   candidate_policy_id, fused_rank, reasons,
                   mandatory_lineage
            FROM groundloop_admitted_pair WHERE admitted_pair_id = %s
            """,
            (admitted_id,),
        ).fetchone()
        expected = (
            admitted.epoch_id,
            admitted.pair.chunk_version_id,
            admitted.pair.claim_id,
            admitted.candidate_policy_id,
            admitted.fused_rank,
            [reason.value for reason in admitted.reasons],
            admitted.mandatory_lineage,
        )
        if row is None or tuple(row) != expected:
            raise EventConflictError(
                "admitted-pair identity was reused with different content"
            )
        scores = tuple(
            hit.score
            for hit in discovery.channel_hits
            if hit.pair == admitted.pair and hit.score is not None
        )
        score = max(scores, default=0.0)
        artifact_hash = stable_m4_digest(
            "m4-frontier-candidate-v1", admitted_id, format(score, ".17g")
        )
        current = cursor.execute(
            """
            SELECT frontier.valid_from_epoch
            FROM groundloop_m4_effective_candidate_frontier AS frontier
            WHERE frontier.epoch_id = %s AND frontier.claim_id = %s
              AND frontier.chunk_version_id = %s
              AND frontier.candidate_policy_id = %s
            """,
            (
                admitted.epoch_id,
                admitted.pair.claim_id,
                admitted.pair.chunk_version_id,
                admitted.candidate_policy_id,
            ),
        ).fetchone()
        if current is None or int(current[0]) != admitted.epoch_id:
            cursor.execute(
                """
                INSERT INTO groundloop_candidate_frontier (
                    claim_id, chunk_version_id, candidate_policy_id,
                    frontier_state, rank, retrieval_score,
                    candidate_artifact_hash, valid_from_epoch,
                    valid_to_epoch
                ) VALUES (%s, %s, %s, 'queued', %s, %s, %s, %s, NULL)
                """,
                (
                    admitted.pair.claim_id,
                    admitted.pair.chunk_version_id,
                    admitted.candidate_policy_id,
                    admitted.fused_rank,
                    score,
                    artifact_hash,
                    admitted.epoch_id,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE groundloop_candidate_frontier SET frontier_state = 'queued'
                WHERE claim_id = %s AND chunk_version_id = %s
                  AND candidate_policy_id = %s AND valid_from_epoch = %s
                """,
                (
                    admitted.pair.claim_id,
                    admitted.pair.chunk_version_id,
                    admitted.candidate_policy_id,
                    current[0],
                ),
            )

    def children_of(
        self, epoch_id: int, root_job_id: str
    ) -> tuple[LogicalJobSpec, ...]:
        if self._measured:
            return self.runtime_store.read_children_point(epoch_id, root_job_id)
        epoch = self.runtime_store.read_epoch(epoch_id)
        return tuple(
            job.spec for job in epoch.jobs if job.spec.parent_job_id == root_job_id
        )

    def mark_fallback_blocked(self, epoch_id: int, root_job_id: str) -> None:
        if self._measured:
            job = self.runtime_store.read_job_point(epoch_id, root_job_id)
            if job.state in {
                JobState.COMPLETED_ACTIVE,
                JobState.COMPLETED_INACTIVE,
            }:
                raise InvalidEventError("fallback root is not open")
            self._fallback_blocked.add(root_job_id)
            return
        epoch = self.runtime_store.read_epoch(epoch_id)
        if not any(job.spec.job_id == root_job_id and job.open for job in epoch.jobs):
            raise InvalidEventError("fallback root is not open")
        self._fallback_blocked.add(root_job_id)

    def fail_epoch(self, epoch_id: int, reason: str) -> None:
        if self._measured:
            header = self.runtime_store.read_epoch_header_point(epoch_id)
            if header.state is RuntimeEpochState.FAILED:
                return
            transition_id = stable_m4_digest(
                "m4-evaluation-failure-v1", str(epoch_id), reason
            )
            with self.connection.transaction():
                failed = self.runtime_store.fail_epoch_point(
                    epoch_id, header.revision, reason
                )
                if not failed.replayed:
                    self.evaluation_store.apply_transition(
                        epoch_id,
                        EvaluationTransition(
                            transition_id=transition_id,
                            expected_revision=header.revision,
                            kind=EvaluationTransitionKind.FAIL,
                        ),
                    )
            # Discard a provisional in-memory overlay. Recovery hydration is
            # explicitly outside the successful event-time bound.
            head = self._publication_head()
            self._published_repository, self._published_engine = (
                _load_published_repository(self.connection, head)
            )
            self._working_repository = self._published_repository
            self._working_engine = self._published_engine
            self._active_epoch_id = None
            return
        epoch = self.runtime_store.read_epoch(epoch_id)
        if epoch.state is RuntimeEpochState.FAILED:
            return
        with self.connection.transaction():
            self.runtime_store.fail_epoch(epoch_id, epoch.revision, reason)
            with self.connection.cursor() as cursor:
                self._sync_evaluation(cursor, epoch_id)

    def _stage_direct_failure_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        reason: str,
    ) -> None:
        """Stage M4 failure metadata/evaluation without owning base authority."""
        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        projected = self.runtime_store.stage_failure_projection_local(
            cursor,
            epoch_id,
            expected_revision,
            reason,
            failure_injector=(
                lambda point: self._inject(
                    "failure_store_" + point.removeprefix("point_")
                )
            ),
        )
        transition = EvaluationTransition(
            transition_id=stable_m4_digest(
                "m4-evaluation-failure-v1", str(epoch_id), reason
            ),
            expected_revision=expected_revision,
            kind=EvaluationTransitionKind.FAIL,
        )
        receipt = self.evaluation_store.apply_transition_local(
            cursor, epoch_id, transition
        )
        if projected.replayed != receipt.replayed:
            raise EventConflictError(
                "direct failure projections disagree on replay state"
            )

    def sealing_snapshot(self, epoch_id: int) -> SealingSnapshot:
        if self._measured:
            header = self.runtime_store.read_epoch_header_point(epoch_id)
            blocked = tuple(sorted(self._fallback_blocked))
            ready = header.seal_ready and not blocked
            return SealingSnapshot(
                epoch_id=epoch_id,
                revision=header.revision,
                ready=ready,
                failed=header.state is RuntimeEpochState.FAILED,
                fallback_blocked_root_ids=blocked,
            )
        epoch = self.runtime_store.read_epoch(epoch_id)
        open_jobs = tuple(sorted(job.spec.job_id for job in epoch.jobs if job.open))
        open_scopes = tuple(
            sorted(
                scope.root_job_id
                for scope in epoch.discovery_scopes
                if not scope.closed
            )
        )
        blocked = tuple(sorted(self._fallback_blocked & set(open_jobs)))
        return SealingSnapshot(
            epoch_id=epoch_id,
            revision=epoch.revision,
            ready=epoch.state is RuntimeEpochState.SEMANTIC_COMPLETE,
            failed=epoch.state is RuntimeEpochState.FAILED,
            open_job_ids=open_jobs,
            open_discovery_root_ids=open_scopes,
            fallback_blocked_root_ids=blocked,
        )

    def complete_verifier_atomically(
        self,
        epoch_id: int,
        lease: JobLease,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
        *,
        make_effective: bool,
    ) -> ObservationCompletionReceipt:
        if completion.job_id != verifier_job.job_id:
            raise EventConflictError("completion belongs to another verifier job")
        attempt_id, lease_token_hash, lease_revision = self._completion_lease_binding(
            lease, completion.job_id
        )
        if observation.key != (
            SubjectKind.CLAIM,
            verifier_job.pair.claim_id if verifier_job.pair else "",
            verifier_job.pair.chunk_version_id if verifier_job.pair else "",
            observation.task_type,
        ):
            raise EventConflictError("observation does not match verifier job")
        expected_effective = completion.terminal_state is JobState.COMPLETED_ACTIVE
        if make_effective != expected_effective:
            raise EventConflictError("observation activity and completion disagree")

        if self._measured:
            header = self.runtime_store.read_epoch_header_point(epoch_id)
            point_job = self.runtime_store.read_job_point(epoch_id, completion.job_id)
            if point_job.spec != verifier_job:
                raise EventConflictError(
                    "verifier completion differs from persisted job"
                )
            already_completed = point_job.state in {
                JobState.COMPLETED_ACTIVE,
                JobState.COMPLETED_INACTIVE,
            }
            patch: IncrementalStatePatch | None = None
            if make_effective and not already_completed:
                patch = self._working_engine.prepare_committed_event_patch(
                    ObserveEvent(
                        stable_m4_digest(
                            "m4-working-observe-event-v1",
                            str(epoch_id),
                            observation.observation_id,
                        ),
                        observation,
                    ),
                    self._working_repository,
                    self._working_repository,
                )
            patch_applied = False
            inserted = False
            replayed = False
            try:
                with self.connection.transaction():
                    point_transition = self.runtime_store.complete_point(
                        CompletionPlan(
                            epoch_id,
                            header.revision,
                            completion,
                        ),
                        attempt_id=attempt_id,
                        lease_token_hash=lease_token_hash,
                        lease_expected_revision=lease_revision,
                    )
                    replayed = point_transition.replayed
                    self._inject("verifier_runtime_completed")
                    if replayed:
                        self._validate_observation(observation, epoch_id, completion)
                    else:
                        inserted = self._insert_observation(
                            observation, epoch_id, completion
                        )
                    self._inject("verifier_observation_archived")
                    if not replayed and verifier_job.pair is not None:
                        self.connection.execute(
                            """
                            UPDATE groundloop_candidate_frontier
                            SET frontier_state = %s
                            WHERE claim_id = %s AND chunk_version_id = %s
                              AND candidate_policy_id = %s
                              AND valid_from_epoch = %s
                            """,
                            (
                                "verified_current" if make_effective else "inactive",
                                verifier_job.pair.claim_id,
                                verifier_job.pair.chunk_version_id,
                                verifier_job.candidate_policy_id,
                                epoch_id,
                            ),
                        )
                    if self._verification_writer is not None:
                        with self.connection.cursor() as cursor:
                            self._verification_writer(
                                cursor,
                                epoch_id,
                                verifier_job,
                                completion,
                                observation,
                            )
                    if replayed:
                        with self.connection.cursor() as cursor:
                            self._validate_verifier_completion_replay(
                                cursor,
                                epoch_id,
                                completion.job_id,
                                observation,
                                make_effective=make_effective,
                            )
                    elif make_effective:
                        assert patch is not None
                        self._working_engine.apply_state_patch(patch)
                        patch_applied = True
                        self._insert_working_observation_delta(
                            epoch_id, point_transition.header.revision, observation
                        )
                        self._inject("verifier_overlay_written")
                        with self.connection.cursor() as cursor:
                            touched_claim_ids = (observation.subject_id,)
                            self._persist_working_states(
                                cursor,
                                epoch_id,
                                point_transition.header.revision,
                                self._working_engine,
                                causative_digest=completion.completion_digest,
                                claim_ids=touched_claim_ids,
                                answer_ids=self._answer_ids_for_claims(
                                    touched_claim_ids
                                ),
                            )
                        self._inject("verifier_state_written")
                    if (
                        not replayed
                        and point_transition.header.state
                        is not RuntimeEpochState.FAILED
                    ):
                        claim_id = (
                            verifier_job.pair.claim_id
                            if verifier_job.pair is not None
                            else observation.subject_id
                        )
                        self.evaluation_store.apply_transition(
                            epoch_id,
                            EvaluationTransition(
                                transition_id=completion.completion_digest,
                                expected_revision=header.revision,
                                claim_job_deltas=(ClaimJobDelta(claim_id, -1),),
                            ),
                        )
            except Exception:
                if patch_applied:
                    self._working_repository, self._working_engine = (
                        _load_working_repository(
                            self.connection,
                            epoch_id,
                            self._publication_head(),
                        )
                    )
                raise
            if replayed:
                return ObservationCompletionReceipt(False, False)
            if make_effective:
                try:
                    self._working_repository.register_observation(observation)
                except Exception:
                    self._working_repository, self._working_engine = (
                        _load_working_repository(
                            self.connection,
                            epoch_id,
                            self._publication_head(),
                        )
                    )
                    raise
            return ObservationCompletionReceipt(inserted, make_effective)

        known_epoch = self.runtime_store.read_epoch(epoch_id)
        known_job = next(
            (job for job in known_epoch.jobs if job.spec.job_id == completion.job_id),
            None,
        )
        if known_job is None:
            raise InvalidEventError("verifier completion names an unknown job")
        known_completion = known_job.completion is not None
        staged_repository = deepcopy(self._working_repository)
        staged_engine = deepcopy(self._working_engine)
        if make_effective and not known_completion:
            before = deepcopy(staged_repository)
            staged_repository.register_observation(observation)
            staged_engine.apply_committed_event(
                ObserveEvent(
                    stable_m4_digest(
                        "m4-working-observe-event-v1",
                        str(epoch_id),
                        observation.observation_id,
                    ),
                    observation,
                ),
                before,
                staged_repository,
            )

        inserted = False
        replayed = False
        with self.connection.transaction():
            epoch = self.runtime_store.read_epoch(epoch_id)
            transition = self.runtime_store.complete(
                CompletionPlan(epoch_id, epoch.revision, completion),
                active_chunk_ids=self._active_chunk_ids(
                    epoch_id,
                    (
                        (verifier_job.pair.chunk_version_id,)
                        if self._measured and verifier_job.pair is not None
                        else None
                    ),
                ),
                attempt_id=attempt_id,
                lease_token_hash=lease_token_hash,
                lease_expected_revision=lease_revision,
            )
            replayed = transition.replayed
            self._inject("verifier_runtime_completed")
            if replayed:
                self._validate_observation(observation, epoch_id, completion)
            else:
                inserted = self._insert_observation(observation, epoch_id, completion)
            self._inject("verifier_observation_archived")
            if not replayed and verifier_job.pair is not None:
                self.connection.execute(
                    """
                    UPDATE groundloop_candidate_frontier
                    SET frontier_state = %s
                    WHERE claim_id = %s AND chunk_version_id = %s
                      AND candidate_policy_id = %s
                      AND valid_from_epoch = %s
                    """,
                    (
                        "verified_current" if make_effective else "inactive",
                        verifier_job.pair.claim_id,
                        verifier_job.pair.chunk_version_id,
                        verifier_job.candidate_policy_id,
                        epoch_id,
                    ),
                )
            if self._verification_writer is not None:
                with self.connection.cursor() as cursor:
                    self._verification_writer(
                        cursor, epoch_id, verifier_job, completion, observation
                    )
            if replayed:
                with self.connection.cursor() as cursor:
                    self._validate_verifier_completion_replay(
                        cursor,
                        epoch_id,
                        completion.job_id,
                        observation,
                        make_effective=make_effective,
                    )
            else:
                completed_epoch = self.runtime_store.read_epoch(epoch_id)
            if make_effective and not replayed:
                self._insert_working_observation_delta(
                    epoch_id, completed_epoch.revision, observation
                )
                self._inject("verifier_overlay_written")
                with self.connection.cursor() as cursor:
                    touched_claim_ids = (observation.subject_id,)
                    self._persist_working_states(
                        cursor,
                        epoch_id,
                        completed_epoch.revision,
                        staged_engine,
                        causative_digest=completion.completion_digest,
                        claim_ids=(touched_claim_ids if self._measured else None),
                        answer_ids=(
                            self._answer_ids_for_claims(touched_claim_ids)
                            if self._measured
                            else None
                        ),
                    )
                    if not self._measured:
                        self._assert_grounding_equality(
                            cursor, epoch_id, staged_repository, staged_engine
                        )
                self._inject("verifier_state_written")
            if not replayed:
                with self.connection.cursor() as cursor:
                    self._sync_evaluation(cursor, epoch_id)
        if replayed:
            return ObservationCompletionReceipt(False, False)
        if make_effective:
            self._working_repository = staged_repository
            self._working_engine = staged_engine
        return ObservationCompletionReceipt(inserted, make_effective)

    def _stage_direct_verifier_completion_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        lease: JobLease,
        verifier_job: LogicalJobSpec,
        completion: JobCompletion,
        observation: SemanticObservation,
        *,
        make_effective: bool,
    ) -> tuple[
        ObservationCompletionReceipt,
        InMemoryRepository | None,
        IncrementalMaintenanceEngine | None,
    ]:
        """Stage one direct verifier completion in the caller's transaction."""
        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        if completion.job_id != verifier_job.job_id:
            raise EventConflictError("completion belongs to another verifier job")
        attempt_id, lease_token_hash, lease_revision = self._completion_lease_binding(
            lease, completion.job_id
        )
        if observation.key != (
            SubjectKind.CLAIM,
            verifier_job.pair.claim_id if verifier_job.pair else "",
            verifier_job.pair.chunk_version_id if verifier_job.pair else "",
            observation.task_type,
        ):
            raise EventConflictError("observation does not match verifier job")
        expected_effective = completion.terminal_state is JobState.COMPLETED_ACTIVE
        if make_effective != expected_effective:
            raise EventConflictError("observation activity and completion disagree")
        header = self.runtime_store.read_epoch_header_point(epoch_id, cursor=cursor)
        if header.revision != expected_revision:
            raise EventConflictError("stale epoch revision")
        point_job = self.runtime_store.read_job_point(
            epoch_id, completion.job_id, cursor=cursor
        )
        if point_job.spec != verifier_job:
            raise EventConflictError("verifier completion differs from persisted job")
        already_completed = point_job.state in {
            JobState.COMPLETED_ACTIVE,
            JobState.COMPLETED_INACTIVE,
        }
        staged_repository: InMemoryRepository | None = None
        staged_engine: IncrementalMaintenanceEngine | None = None
        if make_effective and not already_completed:
            staged_repository = deepcopy(self._working_repository)
            staged_engine = deepcopy(self._working_engine)
            patch = staged_engine.prepare_committed_event_patch(
                ObserveEvent(
                    stable_m4_digest(
                        "m4-working-observe-event-v1",
                        str(epoch_id),
                        observation.observation_id,
                    ),
                    observation,
                ),
                staged_repository,
                staged_repository,
            )
            staged_engine.apply_state_patch(patch)
            staged_repository.register_observation(observation)

        point_transition = self.runtime_store.complete_point_local(
            cursor,
            CompletionPlan(epoch_id, expected_revision, completion),
            attempt_id=attempt_id,
            lease_token_hash=lease_token_hash,
            lease_expected_revision=lease_revision,
        )
        replayed = point_transition.replayed
        self._inject("verifier_runtime_completed")
        if replayed:
            self._validate_observation(observation, epoch_id, completion, cursor=cursor)
            inserted = False
        else:
            inserted = self._insert_observation(
                observation, epoch_id, completion, cursor=cursor
            )
        self._inject("verifier_observation_archived")
        if not replayed and verifier_job.pair is not None:
            cursor.execute(
                """
                UPDATE groundloop_candidate_frontier
                SET frontier_state = %s
                WHERE claim_id = %s AND chunk_version_id = %s
                  AND candidate_policy_id = %s AND valid_from_epoch = %s
                """,
                (
                    "verified_current" if make_effective else "inactive",
                    verifier_job.pair.claim_id,
                    verifier_job.pair.chunk_version_id,
                    verifier_job.candidate_policy_id,
                    epoch_id,
                ),
            )
        if self._verification_writer is not None:
            self._verification_writer(
                cursor,
                epoch_id,
                verifier_job,
                completion,
                observation,
            )
        if replayed:
            self._validate_verifier_completion_replay(
                cursor,
                epoch_id,
                completion.job_id,
                observation,
                make_effective=make_effective,
            )
        elif make_effective:
            assert staged_engine is not None
            self._insert_working_observation_delta(
                epoch_id,
                point_transition.header.revision,
                observation,
                cursor=cursor,
            )
            self._inject("verifier_overlay_written")
            touched_claim_ids = (observation.subject_id,)
            self._persist_working_states(
                cursor,
                epoch_id,
                point_transition.header.revision,
                staged_engine,
                causative_digest=completion.completion_digest,
                claim_ids=touched_claim_ids,
                answer_ids=self._answer_ids_for_claims(touched_claim_ids),
            )
            self._inject("verifier_state_written")
        if (
            not replayed
            and point_transition.header.state is not RuntimeEpochState.FAILED
        ):
            claim_id = (
                verifier_job.pair.claim_id
                if verifier_job.pair is not None
                else observation.subject_id
            )
            self.evaluation_store.apply_transition_local(
                cursor,
                epoch_id,
                EvaluationTransition(
                    transition_id=completion.completion_digest,
                    expected_revision=expected_revision,
                    claim_job_deltas=(ClaimJobDelta(claim_id, -1),),
                ),
            )
        if replayed:
            return ObservationCompletionReceipt(False, False), None, None
        return (
            ObservationCompletionReceipt(inserted, make_effective),
            staged_repository,
            staged_engine,
        )

    def _insert_observation(
        self,
        observation: SemanticObservation,
        epoch_id: int,
        completion: JobCompletion,
        *,
        cursor: SqlExecutor | None = None,
    ) -> bool:
        executor = self.connection if cursor is None else cursor
        inserted = executor.execute(
            """
            INSERT INTO groundloop_semantic_observation (
                observation_id, subject_kind, subject_id, chunk_version_id,
                task_type, support_score, refute_score, neutral_score,
                model_id, model_version, prompt_version, input_hash,
                produced_epoch, raw_output_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (observation_id) DO NOTHING RETURNING observation_id
            """,
            (
                observation.observation_id,
                observation.subject_kind.value,
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
                observation.support_score,
                observation.refute_score,
                observation.neutral_score,
                observation.producer.model_id,
                observation.producer.model_version,
                observation.producer.prompt_version,
                observation.input_hash,
                epoch_id,
                (
                    self._verification_writer.raw_output_hash(
                        completion.result_artifact_id
                    )
                    if self._verification_writer is not None
                    else completion.result_artifact_hash
                ),
            ),
        ).fetchone()
        self._validate_observation(observation, epoch_id, completion, cursor=cursor)
        return inserted is not None

    def _validate_observation(
        self,
        observation: SemanticObservation,
        epoch_id: int,
        completion: JobCompletion,
        *,
        cursor: SqlExecutor | None = None,
    ) -> None:
        raw_output_hash = (
            self._verification_writer.raw_output_hash(completion.result_artifact_id)
            if self._verification_writer is not None
            else completion.result_artifact_hash
        )
        executor = self.connection if cursor is None else cursor
        row = executor.execute(
            """
            SELECT subject_kind, subject_id, chunk_version_id, task_type,
                   support_score, refute_score, neutral_score, model_id,
                   model_version, prompt_version, input_hash, produced_epoch,
                   raw_output_hash
            FROM groundloop_semantic_observation
            WHERE observation_id = %s AND subject_kind = 'claim'
            """,
            (observation.observation_id,),
        ).fetchone()
        expected = (
            observation.subject_kind.value,
            observation.subject_id,
            observation.chunk_version_id,
            observation.task_type,
            observation.support_score,
            observation.refute_score,
            observation.neutral_score,
            observation.producer.model_id,
            observation.producer.model_version,
            observation.producer.prompt_version,
            observation.input_hash,
            epoch_id,
            raw_output_hash,
        )
        if row is None or tuple(row) != expected:
            raise EventConflictError(
                "observation identity was reused with different content"
            )

    def _validate_verifier_completion_replay(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        job_id: str,
        observation: SemanticObservation,
        *,
        make_effective: bool,
    ) -> None:
        completed = cursor.execute(
            """
            SELECT completed_revision FROM groundloop_semantic_job
            WHERE epoch_id = %s AND job_id = %s
            """,
            (epoch_id, job_id),
        ).fetchone()
        if completed is None or completed[0] is None:
            raise EventConflictError("replayed verifier job lacks completion revision")
        delta = cursor.execute(
            """
            SELECT base_observation_id, working_observation_id,
                   installed_revision
            FROM groundloop_working_observation_delta
            WHERE epoch_id = %s AND subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
            """,
            (
                epoch_id,
                observation.subject_kind.value,
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
            ),
        ).fetchone()
        if not make_effective:
            if delta is not None and delta[1] == observation.observation_id:
                raise EventConflictError(
                    "inactive completion replay has an effective observation delta"
                )
            return
        previous = cursor.execute(
            """
            SELECT previous_published_epoch_id FROM groundloop_m4_update
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if previous is None:
            raise EventConflictError("replayed verifier epoch lacks update metadata")
        base = None
        if previous[0] is not None:
            base_row = cursor.execute(
                """
                SELECT observation_id
                FROM groundloop_published_observation_currency
                WHERE subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                  AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    observation.subject_kind.value,
                    observation.subject_id,
                    observation.chunk_version_id,
                    observation.task_type,
                    previous[0],
                    previous[0],
                ),
            ).fetchone()
            base = None if base_row is None else base_row[0]
        expected = (base, observation.observation_id, int(completed[0]))
        if delta is None or tuple(delta) != expected:
            raise EventConflictError(
                "replayed verifier completion differs from its working delta"
            )
        if not self._measured:
            self._assert_grounding_equality(
                cursor,
                epoch_id,
                self._working_repository,
                self._working_engine,
            )

    def _insert_working_observation_delta(
        self,
        epoch_id: int,
        revision: int,
        observation: SemanticObservation,
        *,
        cursor: SqlExecutor | None = None,
    ) -> None:
        executor = self.connection if cursor is None else cursor
        base = executor.execute(
            """
            SELECT observation_id
            FROM groundloop_published_observation_currency
            WHERE subject_kind = %s AND subject_id = %s
              AND chunk_version_id = %s AND task_type = %s
              AND valid_to_epoch IS NULL
            """,
            (
                observation.subject_kind.value,
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
            ),
        ).fetchone()
        executor.execute(
            """
            INSERT INTO groundloop_working_observation_delta (
                epoch_id, subject_kind, subject_id, chunk_version_id,
                task_type, base_observation_id, working_observation_id,
                installed_revision
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                epoch_id,
                observation.subject_kind.value,
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
                None if base is None else base[0],
                observation.observation_id,
                revision,
            ),
        )

    def _active_chunk_ids(
        self,
        epoch_id: int,
        relevant_chunk_ids: tuple[str, ...] | None = None,
    ) -> frozenset[str]:
        state = self.runtime_store.read_epoch(epoch_id).state
        if state is RuntimeEpochState.FAILED:
            return frozenset()
        if state is not RuntimeEpochState.SEALED and self._active_epoch_id != epoch_id:
            raise InvalidEventError("completion does not target the working epoch")
        canonical = (
            None
            if relevant_chunk_ids is None
            else tuple(sorted(set(relevant_chunk_ids)))
        )
        if canonical is None:
            rows = self.connection.execute(
                """
                SELECT chunk_version_id
                FROM groundloop_m4_effective_chunk_version
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """
                SELECT chunk_version_id
                FROM groundloop_m4_effective_chunk_version
                WHERE epoch_id = %s AND chunk_version_id = ANY(%s)
                """,
                (epoch_id, list(canonical)),
            ).fetchall()
        with self.connection.cursor() as cursor:
            self._account(
                cursor,
                epoch_id,
                "active_chunk_rows_examined",
                len(rows) if canonical is None else len(canonical),
            )
        return frozenset(str(row[0]) for row in rows)

    def _persist_working_states(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        revision: int,
        engine: IncrementalMaintenanceEngine,
        *,
        causative_digest: str | None,
        claim_ids: tuple[str, ...] | None = None,
        answer_ids: tuple[str, ...] | None = None,
    ) -> None:
        selected_claim_ids = (
            tuple(sorted(engine.claim_states))
            if claim_ids is None
            else tuple(sorted(set(claim_ids)))
        )
        selected_answer_ids = (
            tuple(sorted(engine.answer_states))
            if answer_ids is None
            else tuple(sorted(set(answer_ids)))
        )
        old_claims = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT claim_id, status
                FROM groundloop_m4_effective_working_claim_state
                WHERE epoch_id = %s AND claim_id = ANY(%s)
                """,
                (epoch_id, list(selected_claim_ids)),
            ).fetchall()
        }
        old_answers = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT answer_version_id, status
                FROM groundloop_m4_effective_working_answer_state
                WHERE epoch_id = %s AND answer_version_id = ANY(%s)
                """,
                (epoch_id, list(selected_answer_ids)),
            ).fetchall()
        }
        for claim_id in selected_claim_ids:
            try:
                claim_state = engine.claim_state(claim_id)
            except KeyError as error:
                raise ValidationError(
                    "working claim selection is outside the engine"
                ) from error
            cursor.execute(
                """
                INSERT INTO groundloop_m4_working_claim_state VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (epoch_id, claim_id) DO UPDATE SET
                    support_count = EXCLUDED.support_count,
                    refute_count = EXCLUDED.refute_count,
                    best_support_score = EXCLUDED.best_support_score,
                    best_refute_score = EXCLUDED.best_refute_score,
                    supporting_observation_ids = EXCLUDED.supporting_observation_ids,
                    refuting_observation_ids = EXCLUDED.refuting_observation_ids,
                    status = EXCLUDED.status,
                    certificate_digest = EXCLUDED.certificate_digest,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    epoch_id,
                    claim_state.claim_id,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.status.value,
                    _certificate_digest(claim_state.claim_id, claim_state),
                    revision,
                ),
            )
            old = old_claims.get(claim_state.claim_id)
            if old is not None and old != claim_state.status.value:
                cursor.execute(
                    """
                    INSERT INTO groundloop_working_transition (
                        epoch_id, revision, object_type, object_id, old_status,
                        new_status, causative_completion_digest
                    ) VALUES (%s, %s, 'claim', %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        epoch_id,
                        revision,
                        claim_state.claim_id,
                        old,
                        claim_state.status.value,
                        causative_digest,
                    ),
                )
        for answer_id in selected_answer_ids:
            try:
                answer_state = engine.answer_state(answer_id)
            except KeyError as error:
                raise ValidationError(
                    "working answer selection is outside the engine"
                ) from error
            cursor.execute(
                """
                INSERT INTO groundloop_m4_working_answer_state VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (epoch_id, answer_version_id) DO UPDATE SET
                    required_claim_count = EXCLUDED.required_claim_count,
                    supported_count = EXCLUDED.supported_count,
                    unsupported_count = EXCLUDED.unsupported_count,
                    refuted_count = EXCLUDED.refuted_count,
                    conflicted_count = EXCLUDED.conflicted_count,
                    status = EXCLUDED.status,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    epoch_id,
                    answer_state.answer_version_id,
                    answer_state.required_claim_count,
                    answer_state.supported_count,
                    answer_state.unsupported_count,
                    answer_state.refuted_count,
                    answer_state.conflicted_count,
                    answer_state.status.value,
                    revision,
                ),
            )
            old = old_answers.get(answer_state.answer_version_id)
            if old is not None and old != answer_state.status.value:
                cursor.execute(
                    """
                    INSERT INTO groundloop_working_transition (
                        epoch_id, revision, object_type, object_id, old_status,
                        new_status, causative_completion_digest
                    ) VALUES (%s, %s, 'answer', %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        epoch_id,
                        revision,
                        answer_state.answer_version_id,
                        old,
                        answer_state.status.value,
                        causative_digest,
                    ),
                )
        self._account(
            cursor,
            epoch_id,
            "working_claim_rows_written",
            len(selected_claim_ids),
        )
        self._account(
            cursor,
            epoch_id,
            "working_answer_rows_written",
            len(selected_answer_ids),
        )

    def _sync_evaluation(self, cursor: Cursor[Any], epoch_id: int) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        if self._measured:
            self._write_compact_evaluation(
                cursor,
                epoch_id=epoch_id,
                revision=epoch.revision,
                confirmed_as_of_epoch=self._publication_head(),
                open_jobs=tuple(job.spec for job in epoch.jobs if job.open),
                discovery_scope_open=any(
                    not scope.closed for scope in epoch.discovery_scopes
                ),
                failed=epoch.state is RuntimeEpochState.FAILED,
            )
            return
        head = self._publication_head()
        claims = cursor.execute(
            """
            SELECT claim.claim_id, claim.answer_version_id, claim.required
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_m4_claim_registry_member AS member
              ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE update_row.epoch_id = %s ORDER BY member.member_ordinal
            """,
            (epoch_id,),
        ).fetchall()
        by_answer: dict[str, list[str]] = {}
        for claim_id, answer_id, required in claims:
            if bool(required):
                by_answer.setdefault(str(answer_id), []).append(str(claim_id))
            state = claim_evaluation_state(epoch, str(claim_id))
            open_count = sum(
                1
                for job in epoch.jobs
                if job.open
                and (
                    (job.spec.pair is not None and job.spec.pair.claim_id == claim_id)
                    or job.spec.target_claim_id == claim_id
                )
            )
            scope_open = any(
                not scope.closed and scope.contains(str(claim_id))
                for scope in epoch.discovery_scopes
            )
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES
                    (%s, 'claim', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (epoch_id, object_type, object_id) DO UPDATE SET
                    evaluation_state = EXCLUDED.evaluation_state,
                    confirmed_as_of_epoch = EXCLUDED.confirmed_as_of_epoch,
                    open_required_job_count = EXCLUDED.open_required_job_count,
                    discovery_scope_open = EXCLUDED.discovery_scope_open,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    epoch_id,
                    claim_id,
                    state.value,
                    head,
                    open_count,
                    scope_open,
                    epoch.revision,
                ),
            )
        answer_rows = cursor.execute(
            """
            SELECT DISTINCT claim.answer_version_id
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_m4_claim_registry_member AS member
              ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE update_row.epoch_id = %s ORDER BY claim.answer_version_id
            """,
            (epoch_id,),
        ).fetchall()
        for (answer_id,) in answer_rows:
            required = tuple(sorted(by_answer.get(str(answer_id), ())))
            state = answer_evaluation_state(epoch, required)
            open_count = sum(
                1
                for job in epoch.jobs
                if job.open
                and (
                    (job.spec.pair is not None and job.spec.pair.claim_id in required)
                    or job.spec.target_claim_id in required
                )
            )
            scope_open = any(
                not scope.closed
                and any(scope.contains(claim_id) for claim_id in required)
                for scope in epoch.discovery_scopes
            )
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES
                    (%s, 'answer', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (epoch_id, object_type, object_id) DO UPDATE SET
                    evaluation_state = EXCLUDED.evaluation_state,
                    confirmed_as_of_epoch = EXCLUDED.confirmed_as_of_epoch,
                    open_required_job_count = EXCLUDED.open_required_job_count,
                    discovery_scope_open = EXCLUDED.discovery_scope_open,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    epoch_id,
                    answer_id,
                    state.value,
                    head,
                    open_count,
                    scope_open,
                    epoch.revision,
                ),
            )

    def _read_sql_oracle(
        self, cursor: Cursor[Any], epoch_id: int
    ) -> tuple[dict[str, ClaimState], dict[str, AnswerState]]:
        claims: dict[str, ClaimState] = {}
        rows = cursor.execute(
            """
            SELECT claim_id, support_count, refute_count, best_support_score,
                   best_refute_score, supporting_observation_ids,
                   refuting_observation_ids, status
            FROM groundloop_m4_claim_state_oracle
            WHERE epoch_id = %s ORDER BY claim_id
            """,
            (epoch_id,),
        ).fetchall()
        for row in rows:
            claim_state = ClaimState(
                str(row[0]),
                int(row[1]),
                int(row[2]),
                None if row[3] is None else float(row[3]),
                None if row[4] is None else float(row[4]),
                tuple(str(item) for item in row[5]),
                tuple(str(item) for item in row[6]),
                ClaimStatus(str(row[7])),
            )
            claims[claim_state.claim_id] = claim_state
        answers: dict[str, AnswerState] = {}
        rows = cursor.execute(
            """
            SELECT answer_version_id, required_claim_count, supported_count,
                   unsupported_count, refuted_count, conflicted_count, status
            FROM groundloop_m4_answer_state_oracle
            WHERE epoch_id = %s ORDER BY answer_version_id
            """,
            (epoch_id,),
        ).fetchall()
        for row in rows:
            answer_state = AnswerState(
                str(row[0]),
                int(row[1]),
                int(row[2]),
                int(row[3]),
                int(row[4]),
                int(row[5]),
                AnswerStatus(str(row[6])),
            )
            answers[answer_state.answer_version_id] = answer_state
        return claims, answers

    def _read_working_states(
        self, cursor: Cursor[Any], epoch_id: int
    ) -> tuple[dict[str, ClaimState], dict[str, AnswerState]]:
        claims: dict[str, ClaimState] = {}
        for row in cursor.execute(
            """
            SELECT claim_id, support_count, refute_count, best_support_score,
                   best_refute_score, supporting_observation_ids,
                   refuting_observation_ids, status
            FROM groundloop_m4_effective_working_claim_state
            WHERE epoch_id = %s ORDER BY claim_id
            """,
            (epoch_id,),
        ).fetchall():
            claim_state = ClaimState(
                str(row[0]),
                int(row[1]),
                int(row[2]),
                None if row[3] is None else float(row[3]),
                None if row[4] is None else float(row[4]),
                tuple(str(item) for item in row[5]),
                tuple(str(item) for item in row[6]),
                ClaimStatus(str(row[7])),
            )
            claims[claim_state.claim_id] = claim_state
        answers: dict[str, AnswerState] = {}
        for row in cursor.execute(
            """
            SELECT answer_version_id, required_claim_count, supported_count,
                   unsupported_count, refuted_count, conflicted_count, status
            FROM groundloop_m4_effective_working_answer_state
            WHERE epoch_id = %s ORDER BY answer_version_id
            """,
            (epoch_id,),
        ).fetchall():
            answer_state = AnswerState(
                str(row[0]),
                int(row[1]),
                int(row[2]),
                int(row[3]),
                int(row[4]),
                int(row[5]),
                AnswerStatus(str(row[6])),
            )
            answers[answer_state.answer_version_id] = answer_state
        return claims, answers

    def _assert_grounding_equality(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        repository: InMemoryRepository,
        engine: IncrementalMaintenanceEngine,
        *,
        count_inline: bool = True,
    ) -> None:
        if count_inline:
            self._account(cursor, epoch_id, "inline_grounding_oracle_calls")
        engine.validate_certificates()
        reference = compute_all_states(repository)
        sql_oracle = self._read_sql_oracle(cursor, epoch_id)
        materialized = self._read_working_states(cursor, epoch_id)
        incremental = (engine.claim_states, engine.answer_states)
        if not (incremental == reference == sql_oracle == materialized):
            raise ValidationError(
                "M4 grounding surfaces differ: incremental, Python, SQL, materialized"
            )

    def check_grounding(self, epoch_id: int) -> None:
        if self._measured:
            if self._active_epoch_id != epoch_id:
                raise InvalidEventError("grounding check does not target working state")
            return
        with self.connection.cursor() as cursor:
            self._assert_grounding_equality(
                cursor,
                epoch_id,
                self._working_repository,
                self._working_engine,
            )

    def audit_grounding_exactness(self, epoch_id: int) -> None:
        """Run all exact grounding oracles outside the measured event path."""
        with self.connection.cursor() as cursor:
            self._assert_grounding_equality(
                cursor,
                epoch_id,
                self._working_repository,
                self._working_engine,
                count_inline=False,
            )

    def check_coordination(self, epoch_id: int) -> None:
        if self._measured:
            header = self.runtime_store.read_epoch_header_point(epoch_id)
            if header.state is not RuntimeEpochState.SEMANTIC_COMPLETE:
                raise ValidationError("coordination surface is not complete")
            if header.open_job_count or header.open_scope_count:
                raise ValidationError("coordination counters retain open work")
            return
        epoch = self.runtime_store.read_epoch(epoch_id)
        if not self._measured:
            book = self.runtime_store.read_book()
            if not any(item == epoch for item in book.epochs):
                raise ValidationError("runtime book omits the requested epoch")
        if epoch.state is not RuntimeEpochState.SEMANTIC_COMPLETE:
            raise ValidationError("coordination surface is not complete")

    def check_evaluation(self, epoch_id: int) -> None:
        with self.connection.cursor() as cursor:
            if self._measured:
                self._assert_incremental_evaluation(
                    cursor, epoch_id, require_complete=True
                )
            else:
                self._assert_evaluation_surface(cursor, epoch_id, require_complete=True)

    def _assert_incremental_evaluation(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        *,
        require_complete: bool,
    ) -> None:
        header = self.runtime_store.read_epoch_header_point(epoch_id)
        default = self.evaluation_store.read_default(epoch_id)
        if default.revision != header.revision:
            raise ValidationError(
                "evaluation and runtime revisions are not synchronized"
            )
        override = cursor.execute(
            """
            SELECT 1 FROM groundloop_m4_evaluation_override_counter
            WHERE epoch_id = %s LIMIT 1
            """,
            (epoch_id,),
        ).fetchone()
        if require_complete and (
            default.lifecycle
            not in {EvaluationLifecycle.ACTIVE, EvaluationLifecycle.SEALED}
            or default.default_state.value != "complete"
            or default.open_discovery_scope_count != 0
            or override is not None
            or (
                default.lifecycle is EvaluationLifecycle.SEALED
                and default.confirmed_as_of_epoch != epoch_id
            )
        ):
            raise ValidationError("incremental evaluation surface is not sealable")

    def _assert_compact_evaluation(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        *,
        require_complete: bool,
    ) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        scope_open = any(not scope.closed for scope in epoch.discovery_scopes)
        failed = epoch.state is RuntimeEpochState.FAILED
        default_state = "failed" if failed else "pending" if scope_open else "complete"
        default = cursor.execute(
            """
            SELECT evaluation_state, confirmed_as_of_epoch,
                   discovery_scope_open, updated_revision
            FROM groundloop_m4_evaluation_default WHERE epoch_id = %s
            FOR SHARE
            """,
            (epoch_id,),
        ).fetchone()
        expected_default = (
            default_state,
            self._publication_head(),
            scope_open,
            epoch.revision,
        )
        if default is None or tuple(default) != expected_default:
            raise ValidationError("compact evaluation default differs from runtime")
        open_by_claim: dict[str, int] = {}
        if not failed:
            for job in epoch.jobs:
                if not job.open:
                    continue
                claim_id = (
                    job.spec.pair.claim_id
                    if job.spec.pair is not None
                    else job.spec.target_claim_id
                )
                if claim_id is not None:
                    open_by_claim[claim_id] = open_by_claim.get(claim_id, 0) + 1
        answer_by_claim: dict[str, tuple[str, bool]] = (
            {}
            if not open_by_claim
            else {
                str(row[0]): (str(row[1]), bool(row[2]))
                for row in cursor.execute(
                    """
                    SELECT claim_id, answer_version_id, required
                    FROM groundloop_claim
                    WHERE claim_id = ANY(%s)
                    """,
                    (list(sorted(open_by_claim)),),
                ).fetchall()
            }
        )
        expected_overrides: dict[tuple[str, str], tuple[str, int, int, bool, int]] = {}
        answer_counts: dict[str, int] = {}
        for claim_id, count in open_by_claim.items():
            expected_overrides[("claim", claim_id)] = (
                "pending",
                self._publication_head(),
                count,
                scope_open,
                epoch.revision,
            )
            answer = answer_by_claim.get(claim_id)
            if answer is not None and answer[1]:
                answer_id = answer[0]
                answer_counts[answer_id] = answer_counts.get(answer_id, 0) + count
        for answer_id, count in answer_counts.items():
            expected_overrides[("answer", answer_id)] = (
                "pending",
                self._publication_head(),
                count,
                scope_open,
                epoch.revision,
            )
        actual_overrides = {
            (str(row[0]), str(row[1])): (
                str(row[2]),
                int(row[3]),
                int(row[4]),
                bool(row[5]),
                int(row[6]),
            )
            for row in cursor.execute(
                """
                SELECT object_type, object_id, evaluation_state,
                       confirmed_as_of_epoch, open_required_job_count,
                       discovery_scope_open, updated_revision
                FROM groundloop_object_evaluation WHERE epoch_id = %s
                FOR SHARE
                """,
                (epoch_id,),
            ).fetchall()
        }
        if actual_overrides != expected_overrides:
            raise ValidationError("compact evaluation overrides differ from runtime")
        if require_complete and (default_state != "complete" or expected_overrides):
            raise ValidationError("compact evaluation surface is not sealable")

    def _assert_evaluation_surface(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        *,
        require_complete: bool,
    ) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        head = self._publication_head()
        expected: dict[tuple[str, str], tuple[str, int, int, bool, int]] = {}
        claim_rows = cursor.execute(
            """
            SELECT claim.claim_id, claim.answer_version_id, claim.required
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_m4_claim_registry_member AS member
              ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE update_row.epoch_id = %s ORDER BY member.member_ordinal
            """,
            (epoch_id,),
        ).fetchall()
        by_answer: dict[str, list[str]] = {}
        for claim_id, answer_id, required in claim_rows:
            claim = str(claim_id)
            state = claim_evaluation_state(epoch, claim).value
            open_count = sum(
                1
                for job in epoch.jobs
                if job.open
                and (
                    (job.spec.pair is not None and job.spec.pair.claim_id == claim)
                    or job.spec.target_claim_id == claim
                )
            )
            scope_open = any(
                not scope.closed and scope.contains(claim)
                for scope in epoch.discovery_scopes
            )
            expected[("claim", claim)] = (
                state,
                head,
                open_count,
                scope_open,
                epoch.revision,
            )
            if bool(required):
                by_answer.setdefault(str(answer_id), []).append(claim)
        answer_rows = cursor.execute(
            """
            SELECT DISTINCT claim.answer_version_id
            FROM groundloop_m4_update AS update_row
            JOIN groundloop_m4_claim_registry_member AS member
              ON member.claim_registry_snapshot_id = update_row.registry_snapshot_id
            JOIN groundloop_claim AS claim USING (claim_id)
            WHERE update_row.epoch_id = %s ORDER BY claim.answer_version_id
            """,
            (epoch_id,),
        ).fetchall()
        for (raw_answer_id,) in answer_rows:
            answer_id = str(raw_answer_id)
            claims = tuple(sorted(by_answer.get(answer_id, ())))
            state = answer_evaluation_state(epoch, claims).value
            open_count = sum(
                1
                for job in epoch.jobs
                if job.open
                and (
                    (job.spec.pair is not None and job.spec.pair.claim_id in claims)
                    or job.spec.target_claim_id in claims
                )
            )
            scope_open = any(
                not scope.closed
                and any(scope.contains(claim_id) for claim_id in claims)
                for scope in epoch.discovery_scopes
            )
            expected[("answer", answer_id)] = (
                state,
                head,
                open_count,
                scope_open,
                epoch.revision,
            )
        actual = {
            (str(row[0]), str(row[1])): (
                str(row[2]),
                int(row[3]),
                int(row[4]),
                bool(row[5]),
                int(row[6]),
            )
            for row in cursor.execute(
                """
                SELECT object_type, object_id, evaluation_state,
                       confirmed_as_of_epoch, open_required_job_count,
                       discovery_scope_open, updated_revision
                FROM groundloop_object_evaluation WHERE epoch_id = %s
                FOR SHARE
                """,
                (epoch_id,),
            ).fetchall()
        }
        if actual != expected:
            raise ValidationError("persisted evaluation surface differs from runtime")
        if require_complete and any(
            value[0] != "complete"
            or value[2] != 0
            or value[3]
            or value[4] != epoch.revision
            for value in actual.values()
        ):
            raise ValidationError("evaluation surface is not sealable")

    def request_seal(
        self,
        epoch_id: int,
        expected_revision: int,
        update: CorpusUpdateIdentity,
    ) -> PublicationReceipt:
        if self._measured:
            header = self.runtime_store.read_epoch_header_point(epoch_id)
            update_row = self.connection.execute(
                """
                SELECT epoch.event_id, epoch.payload_hash,
                       update_row.update_kind,
                       update_row.previous_published_epoch_id,
                       update_row.candidate_policy_id
                FROM groundloop_epoch AS epoch
                JOIN groundloop_m4_update AS update_row USING (epoch_id)
                WHERE epoch.epoch_id = %s
                """,
                (epoch_id,),
            ).fetchone()
            persisted_update = (
                None
                if update_row is None
                else CorpusUpdateIdentity(
                    event_id=str(update_row[0]),
                    payload_hash=str(update_row[1]).strip(),
                    update_kind=UpdateKind(str(update_row[2])),
                    previous_published_epoch_id=(
                        None if update_row[3] is None else int(update_row[3])
                    ),
                    candidate_policy_id=str(update_row[4]),
                )
            )
            if persisted_update != update:
                raise EventConflictError(
                    "publication update differs from runtime epoch"
                )

            def publish_point(cursor: Cursor[Any], published_epoch_id: int) -> None:
                self._assert_incremental_evaluation(
                    cursor, published_epoch_id, require_complete=True
                )
                self._promote_structural_overlay(cursor, published_epoch_id)
                self._inject("publication_structure_promoted")
                self._promote_observation_currency(cursor, published_epoch_id)
                self._inject("publication_currency_promoted")
                self._publish_grounding_states(
                    cursor, published_epoch_id, expected_revision + 1, update
                )
                self._inject("publication_states_written")
                cursor.execute(
                    """
                    INSERT INTO groundloop_m4_publication_head(singleton, epoch_id)
                    VALUES (true, %s)
                    ON CONFLICT (singleton) DO UPDATE SET
                        epoch_id = EXCLUDED.epoch_id, updated_at = now()
                    """,
                    (published_epoch_id,),
                )
                self._inject("publication_head_advanced")
                self.evaluation_store.apply_transition(
                    published_epoch_id,
                    EvaluationTransition(
                        transition_id=stable_m4_digest(
                            "m4-evaluation-seal-v1", str(published_epoch_id)
                        ),
                        expected_revision=header.revision,
                        kind=EvaluationTransitionKind.SEAL,
                    ),
                )
                self._inject("publication_evaluation_promoted")

            self.runtime_store.seal_epoch_point(
                epoch_id,
                expected_revision,
                publication_action=publish_point,
                failure_injector=(
                    lambda point: self._inject(
                        "publication_store_" + point.removeprefix("point_")
                    )
                ),
            )
            self._published_repository = self._working_repository
            self._published_engine = self._working_engine
            self._active_epoch_id = None
            publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
            return PublicationReceipt(epoch_id, publication_id)

        epoch = self.runtime_store.read_epoch(epoch_id)
        if epoch.update != update:
            raise EventConflictError("publication update differs from runtime epoch")

        def publish(cursor: Cursor[Any], published_epoch_id: int) -> None:
            if self._measured:
                self._assert_compact_evaluation(
                    cursor, published_epoch_id, require_complete=True
                )
            else:
                self._assert_evaluation_surface(
                    cursor, published_epoch_id, require_complete=True
                )
                self._assert_grounding_equality(
                    cursor,
                    published_epoch_id,
                    self._working_repository,
                    self._working_engine,
                )
            self._promote_structural_overlay(cursor, published_epoch_id)
            self._inject("publication_structure_promoted")
            self._promote_observation_currency(cursor, published_epoch_id)
            self._inject("publication_currency_promoted")
            self._publish_grounding_states(
                cursor, published_epoch_id, expected_revision + 1, update
            )
            self._inject("publication_states_written")
            cursor.execute(
                """
                INSERT INTO groundloop_m4_publication_head(singleton, epoch_id)
                VALUES (true, %s)
                ON CONFLICT (singleton) DO UPDATE SET
                    epoch_id = EXCLUDED.epoch_id, updated_at = now()
                """,
                (published_epoch_id,),
            )
            self._inject("publication_head_advanced")
            self._promote_evaluation_surface(
                cursor, published_epoch_id, expected_revision + 1
            )
            self._inject("publication_evaluation_promoted")
            self._assert_sealed_evaluation(published_epoch_id, expected_revision + 1)

        self.runtime_store.seal_epoch(
            epoch_id,
            expected_revision,
            publication_action=publish,
            failure_injector=(lambda point: self._inject(f"publication_store_{point}")),
        )
        self._published_repository = deepcopy(self._working_repository)
        self._published_engine = deepcopy(self._working_engine)
        self._active_epoch_id = None
        publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
        return PublicationReceipt(epoch_id, publication_id)

    def _stage_direct_seal_local(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        expected_revision: int,
        update: CorpusUpdateIdentity,
    ) -> PublicationReceipt:
        """Stage direct publication while leaving heads/base state to M5."""
        if not self._measured:
            raise ValidationError(
                "typed cursor-local M4 composition requires measured mode"
            )
        update_row = cursor.execute(
            """
            SELECT epoch.event_id, epoch.payload_hash, update_row.update_kind,
                   update_row.previous_published_epoch_id,
                   update_row.candidate_policy_id
            FROM groundloop_epoch AS epoch
            JOIN groundloop_m4_update AS update_row USING (epoch_id)
            WHERE epoch.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        persisted_update = (
            None
            if update_row is None
            else CorpusUpdateIdentity(
                event_id=str(update_row[0]),
                payload_hash=str(update_row[1]).strip(),
                update_kind=UpdateKind(str(update_row[2])),
                previous_published_epoch_id=(
                    None if update_row[3] is None else int(update_row[3])
                ),
                candidate_policy_id=str(update_row[4]),
            )
        )
        if persisted_update != update:
            raise EventConflictError("publication update differs from runtime epoch")
        checked = self.runtime_store.validate_seal_point_local(
            cursor,
            epoch_id,
            expected_revision,
            failure_injector=(
                lambda point: self._inject(
                    "publication_store_" + point.removeprefix("point_")
                )
            ),
        )
        publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
        if checked.replayed:
            return PublicationReceipt(epoch_id, publication_id)
        if self._active_epoch_id != epoch_id:
            raise InvalidEventError("direct seal requires the adopted working cache")
        self._assert_incremental_evaluation(cursor, epoch_id, require_complete=True)
        self._promote_structural_overlay(cursor, epoch_id)
        self._inject("publication_structure_promoted")
        self._promote_observation_currency(cursor, epoch_id)
        self._inject("publication_currency_promoted")
        self._publish_grounding_states(
            cursor,
            epoch_id,
            expected_revision + 1,
            update,
            emit_public_deltas=False,
        )
        self._inject("publication_states_written")
        self.evaluation_store.apply_transition_local(
            cursor,
            epoch_id,
            EvaluationTransition(
                transition_id=stable_m4_digest("m4-evaluation-seal-v1", str(epoch_id)),
                expected_revision=expected_revision,
                kind=EvaluationTransitionKind.SEAL,
            ),
        )
        self._inject("publication_evaluation_promoted")
        return PublicationReceipt(epoch_id, publication_id)

    def _promote_evaluation_surface(
        self, cursor: Cursor[Any], epoch_id: int, final_revision: int
    ) -> None:
        if self._measured:
            changed = cursor.execute(
                """
                UPDATE groundloop_m4_evaluation_default
                SET confirmed_as_of_epoch = %s, updated_revision = %s
                WHERE epoch_id = %s AND evaluation_state = 'complete'
                  AND NOT discovery_scope_open
                """,
                (epoch_id, final_revision, epoch_id),
            ).rowcount
            if changed != 1:
                raise ValidationError(
                    "seal could not promote the compact evaluation default"
                )
            return
        changed = cursor.execute(
            """
            UPDATE groundloop_object_evaluation
            SET confirmed_as_of_epoch = %s, updated_revision = %s
            WHERE epoch_id = %s AND evaluation_state = 'complete'
              AND open_required_job_count = 0
              AND NOT discovery_scope_open
            """,
            (epoch_id, final_revision, epoch_id),
        ).rowcount
        total = cursor.execute(
            """
            SELECT count(*) FROM groundloop_object_evaluation
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if total is None or changed != int(total[0]):
            raise ValidationError(
                "seal could not promote the complete evaluation surface"
            )

    def _assert_sealed_evaluation(self, epoch_id: int, final_revision: int) -> None:
        if self._measured:
            row = self.connection.execute(
                """
                SELECT evaluation_state, confirmed_as_of_epoch,
                       discovery_scope_open, updated_revision,
                       (SELECT count(*) FROM groundloop_object_evaluation
                        WHERE epoch_id = %s)
                FROM groundloop_m4_evaluation_default WHERE epoch_id = %s
                """,
                (epoch_id, epoch_id),
            ).fetchone()
            if row is None or tuple(row) != (
                "complete",
                epoch_id,
                False,
                final_revision,
                0,
            ):
                raise ValidationError(
                    "sealed compact evaluation surface differs from runtime"
                )
            return
        rows = self.connection.execute(
            """
            SELECT evaluation_state, confirmed_as_of_epoch,
                   open_required_job_count, discovery_scope_open,
                   updated_revision
            FROM groundloop_object_evaluation WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchall()
        if not rows or any(
            (
                str(row[0]),
                int(row[1]),
                int(row[2]),
                bool(row[3]),
                int(row[4]),
            )
            != ("complete", epoch_id, 0, False, final_revision)
            for row in rows
        ):
            raise ValidationError("sealed evaluation surface differs from runtime")

    def _promote_structural_overlay(self, cursor: Cursor[Any], epoch_id: int) -> None:
        self._promote_document_metadata(cursor, epoch_id)
        previous_row = cursor.execute(
            """
            SELECT previous_published_epoch_id FROM groundloop_m4_update
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        if previous_row is None or previous_row[0] is None:
            raise ValidationError("M4 structural promotion has no published base")
        previous_epoch = int(previous_row[0])
        ambiguous = cursor.execute(
            """
            SELECT current.claim_id, current.chunk_version_id,
                   current.candidate_policy_id, count(*)
            FROM groundloop_candidate_frontier AS current
            JOIN groundloop_candidate_frontier AS predecessor
              ON predecessor.claim_id = current.claim_id
             AND predecessor.chunk_version_id = current.chunk_version_id
             AND predecessor.candidate_policy_id = current.candidate_policy_id
             AND predecessor.valid_from_epoch <= %s
             AND (
                 predecessor.valid_to_epoch IS NULL
                 OR %s < predecessor.valid_to_epoch
             )
            JOIN groundloop_epoch AS creator
              ON creator.epoch_id = predecessor.valid_from_epoch
             AND creator.semantic_status = 'sealed'
            WHERE current.valid_from_epoch = %s
            GROUP BY current.claim_id, current.chunk_version_id,
                     current.candidate_policy_id
            HAVING count(*) > 1
            LIMIT 1
            """,
            (previous_epoch, previous_epoch, epoch_id),
        ).fetchone()
        if ambiguous is not None:
            raise EventConflictError("frontier key has multiple published predecessors")
        cursor.execute(
            """
            UPDATE groundloop_candidate_frontier AS predecessor
            SET valid_to_epoch = %s
            FROM groundloop_candidate_frontier AS current,
                 groundloop_epoch AS creator
            WHERE current.valid_from_epoch = %s
              AND predecessor.claim_id = current.claim_id
              AND predecessor.chunk_version_id = current.chunk_version_id
              AND predecessor.candidate_policy_id = current.candidate_policy_id
              AND predecessor.valid_from_epoch <= %s
              AND predecessor.valid_from_epoch <> %s
              AND (
                  predecessor.valid_to_epoch IS NULL
                  OR %s < predecessor.valid_to_epoch
              )
              AND creator.epoch_id = predecessor.valid_from_epoch
              AND creator.semantic_status = 'sealed'
            """,
            (epoch_id, epoch_id, previous_epoch, epoch_id, previous_epoch),
        )
        old_rows = cursor.execute(
            """
            SELECT document_version_id
            FROM groundloop_m4_structural_deactivation
            WHERE epoch_id = %s ORDER BY document_version_id
            """,
            (epoch_id,),
        ).fetchall()
        for (document_version_id,) in old_rows:
            changed = cursor.execute(
                """
                UPDATE groundloop_document_version SET valid_to_epoch = %s
                WHERE document_version_id = %s
                  AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    epoch_id,
                    document_version_id,
                    previous_epoch,
                    previous_epoch,
                ),
            ).rowcount
            if changed != 1:
                raise EventConflictError(
                    "published document version changed before structural promotion"
                )
            cursor.execute(
                """
                UPDATE groundloop_chunk_version SET valid_to_epoch = %s
                WHERE document_version_id = %s
                  AND valid_from_epoch <= %s
                  AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
                """,
                (
                    epoch_id,
                    document_version_id,
                    previous_epoch,
                    previous_epoch,
                ),
            )
            cursor.execute(
                """
                UPDATE groundloop_candidate_frontier AS frontier
                SET valid_to_epoch = %s
                FROM groundloop_chunk_version AS chunk,
                     groundloop_epoch AS creator
                WHERE chunk.document_version_id = %s
                  AND frontier.chunk_version_id = chunk.chunk_version_id
                  AND creator.epoch_id = frontier.valid_from_epoch
                  AND creator.semantic_status = 'sealed'
                  AND frontier.valid_from_epoch <= %s
                  AND (
                      frontier.valid_to_epoch IS NULL
                      OR %s < frontier.valid_to_epoch
                  )
                """,
                (epoch_id, document_version_id, previous_epoch, previous_epoch),
            )

    def _promote_document_metadata(self, cursor: Cursor[Any], epoch_id: int) -> None:
        rows = cursor.execute(
            """
            SELECT overlay.document_id, overlay.source_uri,
                   overlay.authority_class,
                   EXISTS (
                       SELECT 1 FROM groundloop_document_version AS version
                       JOIN groundloop_epoch AS creator
                         ON creator.epoch_id = version.valid_from_epoch
                        AND creator.semantic_status = 'sealed'
                       WHERE version.document_id = overlay.document_id
                   ) AS has_published_version
            FROM groundloop_m4_document_metadata_overlay AS overlay
            WHERE overlay.epoch_id = %s ORDER BY overlay.document_id
            """,
            (epoch_id,),
        ).fetchall()
        for document_id, source_uri, authority_class, has_published in rows:
            if bool(has_published):
                stored = cursor.execute(
                    """
                    SELECT source_uri, authority_class FROM groundloop_document
                    WHERE document_id = %s
                    """,
                    (document_id,),
                ).fetchone()
                if stored is None or tuple(stored) != (source_uri, authority_class):
                    raise EventConflictError(
                        "published document metadata differs from replacement"
                    )
                continue
            changed = cursor.execute(
                """
                UPDATE groundloop_document
                SET source_uri = %s, authority_class = %s
                WHERE document_id = %s
                  AND source_uri IS NULL
                  AND authority_class = 'unclassified'
                """,
                (source_uri, authority_class, document_id),
            ).rowcount
            if changed != 1:
                raise EventConflictError(
                    "unpublished document placeholder changed before sealing"
                )

    def _promote_observation_currency(self, cursor: Cursor[Any], epoch_id: int) -> None:
        rows = cursor.execute(
            """
            SELECT subject_kind, subject_id, chunk_version_id, task_type,
                   base_observation_id, working_observation_id
            FROM groundloop_working_observation_delta
            WHERE epoch_id = %s AND subject_kind = 'claim'
            ORDER BY subject_kind, subject_id, chunk_version_id, task_type
            """,
            (epoch_id,),
        ).fetchall()
        for row in rows:
            key = tuple(row[:4])
            cursor.execute(
                """
                UPDATE groundloop_published_observation_currency
                SET valid_to_epoch = %s
                WHERE subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                  AND valid_to_epoch IS NULL
                """,
                (epoch_id, *key),
            )
            cursor.execute(
                """
                DELETE FROM groundloop_observation_currency
                WHERE subject_kind = %s AND subject_id = %s
                  AND chunk_version_id = %s AND task_type = %s
                """,
                key,
            )
            working_id = row[5]
            if working_id is None:
                continue
            cursor.execute(
                """
                INSERT INTO groundloop_published_observation_currency (
                    subject_kind, subject_id, chunk_version_id, task_type,
                    observation_id, valid_from_epoch, valid_to_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, NULL)
                """,
                (*key, working_id, epoch_id),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_observation_currency (
                    subject_kind, subject_id, chunk_version_id, task_type,
                    observation_id, installed_revision
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (*key, working_id, epoch_id),
            )

    def _publish_grounding_states(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        revision: int,
        update: CorpusUpdateIdentity,
        *,
        emit_public_deltas: bool = True,
    ) -> None:
        claim_ids = (
            tuple(sorted(self._working_engine.claim_states))
            if not self._measured
            else tuple(
                str(row[0])
                for row in cursor.execute(
                    """
                    SELECT claim_id FROM groundloop_m4_working_claim_state
                    WHERE epoch_id = %s ORDER BY claim_id
                    """,
                    (epoch_id,),
                ).fetchall()
            )
        )
        answer_ids = (
            tuple(sorted(self._working_engine.answer_states))
            if not self._measured
            else tuple(
                str(row[0])
                for row in cursor.execute(
                    """
                    SELECT answer_version_id
                    FROM groundloop_m4_working_answer_state
                    WHERE epoch_id = %s ORDER BY answer_version_id
                    """,
                    (epoch_id,),
                ).fetchall()
            )
        )
        old_claims = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT claim_id, status FROM groundloop_published_claim_state
                WHERE valid_to_epoch IS NULL AND claim_id = ANY(%s)
                """,
                (list(claim_ids),),
            ).fetchall()
        }
        old_answers = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT answer_version_id, status
                FROM groundloop_published_answer_state
                WHERE valid_to_epoch IS NULL AND answer_version_id = ANY(%s)
                """,
                (list(answer_ids),),
            ).fetchall()
        }
        cursor.execute(
            """
            UPDATE groundloop_published_claim_state SET valid_to_epoch = %s
            WHERE valid_to_epoch IS NULL AND claim_id = ANY(%s)
            """,
            (epoch_id, list(claim_ids)),
        )
        cursor.execute(
            """
            UPDATE groundloop_published_answer_state SET valid_to_epoch = %s
            WHERE valid_to_epoch IS NULL AND answer_version_id = ANY(%s)
            """,
            (epoch_id, list(answer_ids)),
        )
        for claim_id in claim_ids:
            claim_state = self._working_engine.claim_state(claim_id)
            digest = _certificate_digest(claim_state.claim_id, claim_state)
            cursor.execute(
                """
                INSERT INTO groundloop_published_claim_state VALUES (
                    %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    claim_state.claim_id,
                    epoch_id,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.status.value,
                    digest,
                ),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_claim_state_materialized VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (claim_id) DO UPDATE SET
                    support_count = EXCLUDED.support_count,
                    refute_count = EXCLUDED.refute_count,
                    best_support_score = EXCLUDED.best_support_score,
                    best_refute_score = EXCLUDED.best_refute_score,
                    supporting_observation_ids = EXCLUDED.supporting_observation_ids,
                    refuting_observation_ids = EXCLUDED.refuting_observation_ids,
                    status = EXCLUDED.status,
                    updated_epoch = EXCLUDED.updated_epoch,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    claim_state.claim_id,
                    claim_state.support_count,
                    claim_state.refute_count,
                    claim_state.best_support_score,
                    claim_state.best_refute_score,
                    list(claim_state.supporting_observation_ids),
                    list(claim_state.refuting_observation_ids),
                    claim_state.status.value,
                    epoch_id,
                    revision,
                ),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_claim_certificate VALUES
                    (%s, %s, %s, %s, %s)
                ON CONFLICT (claim_id) DO UPDATE SET
                    support_observation_id = EXCLUDED.support_observation_id,
                    refute_observation_id = EXCLUDED.refute_observation_id,
                    repaired_epoch = EXCLUDED.repaired_epoch,
                    repaired_revision = EXCLUDED.repaired_revision
                """,
                (
                    claim_state.claim_id,
                    claim_state.supporting_observation_ids[0]
                    if claim_state.supporting_observation_ids
                    else None,
                    claim_state.refuting_observation_ids[0]
                    if claim_state.refuting_observation_ids
                    else None,
                    epoch_id,
                    revision,
                ),
            )
            old = old_claims.get(claim_state.claim_id)
            if (
                emit_public_deltas
                and old is not None
                and old != claim_state.status.value
            ):
                self._insert_public_delta(
                    cursor,
                    update,
                    epoch_id,
                    revision,
                    "claim",
                    claim_state.claim_id,
                    old,
                    claim_state.status.value,
                )
        for answer_id in answer_ids:
            answer_state = self._working_engine.answer_state(answer_id)
            cursor.execute(
                """
                INSERT INTO groundloop_published_answer_state VALUES (
                    %s, %s, NULL, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    answer_state.answer_version_id,
                    epoch_id,
                    answer_state.required_claim_count,
                    answer_state.supported_count,
                    answer_state.unsupported_count,
                    answer_state.refuted_count,
                    answer_state.conflicted_count,
                    answer_state.status.value,
                ),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_answer_state_materialized VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (answer_version_id) DO UPDATE SET
                    required_claim_count = EXCLUDED.required_claim_count,
                    supported_count = EXCLUDED.supported_count,
                    unsupported_count = EXCLUDED.unsupported_count,
                    refuted_count = EXCLUDED.refuted_count,
                    conflicted_count = EXCLUDED.conflicted_count,
                    status = EXCLUDED.status,
                    updated_epoch = EXCLUDED.updated_epoch,
                    updated_revision = EXCLUDED.updated_revision
                """,
                (
                    answer_state.answer_version_id,
                    answer_state.required_claim_count,
                    answer_state.supported_count,
                    answer_state.unsupported_count,
                    answer_state.refuted_count,
                    answer_state.conflicted_count,
                    answer_state.status.value,
                    epoch_id,
                    revision,
                ),
            )
            old = old_answers.get(answer_state.answer_version_id)
            if (
                emit_public_deltas
                and old is not None
                and old != answer_state.status.value
            ):
                self._insert_public_delta(
                    cursor,
                    update,
                    epoch_id,
                    revision,
                    "answer",
                    answer_state.answer_version_id,
                    old,
                    answer_state.status.value,
                )
        self._account(
            cursor,
            epoch_id,
            "published_claim_versions_written",
            len(claim_ids),
        )
        self._account(
            cursor,
            epoch_id,
            "published_answer_versions_written",
            len(answer_ids),
        )

    @staticmethod
    def _insert_public_delta(
        cursor: Cursor[Any],
        update: CorpusUpdateIdentity,
        epoch_id: int,
        revision: int,
        object_type: str,
        object_id: str,
        old_status: str,
        new_status: str,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO groundloop_status_delta (
                event_id, epoch_id, revision, object_type, object_id,
                old_status, new_status, reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                update.event_id,
                epoch_id,
                revision,
                object_type,
                object_id,
                old_status,
                new_status,
                f"event={update.event_id} op={update.update_kind.value}",
            ),
        )
