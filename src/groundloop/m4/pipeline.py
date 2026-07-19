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
from dataclasses import dataclass
from typing import Any, Protocol

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
from groundloop.incremental import IncrementalMaintenanceEngine
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
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
    JobCompletion,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.models.contracts import PairVerificationInput
from groundloop.m4.models.ports import M4VerificationApplicationPort
from groundloop.m4.persistence import PostgresM4RuntimeStore
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


def _certificate_digest(claim_id: str, state: ClaimState) -> str:
    return stable_m4_digest(
        "m4-claim-certificate-v1",
        claim_id,
        state.supporting_observation_ids[0]
        if state.supporting_observation_ids
        else "",
        state.refuting_observation_ids[0]
        if state.refuting_observation_ids
        else "",
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
        return () if self.inserted is None else tuple(
            chunk.chunk_version_id for chunk in self.inserted.chunks
        )

    @property
    def manifest(self) -> dict[str, object]:
        return {
            "schema": "groundloop-m4-structural-payload-v1",
            "deactivated_document_version_id": (
                self.deactivated_document_version_id
            ),
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
            "chunker_artifact_id": (
                None
                if self.inserted is None
                else self.inserted.chunker_artifact_id
            ),
            "chunker_input_hash": (
                None if self.inserted is None else self.inserted.chunker_input_hash
            ),
        }


class AdmissionDelegate(Protocol):
    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult: ...


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
        calibration_hash = (
            self.verifier_port.adapter.spec.calibration_artifact_sha256
        )
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
    """Archive deterministic admitted pairs returned by an injected strategy."""

    connection: Connection[Any]
    delegate: AdmissionDelegate

    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult:
        result = self.delegate.discover(epoch_id, root_job)
        with self.connection.transaction():
            for admitted in result.admitted_pairs:
                admitted_id = stable_m4_digest(
                    "m4-admitted-pair-v1",
                    str(admitted.epoch_id),
                    admitted.pair.claim_id,
                    admitted.pair.chunk_version_id,
                    admitted.candidate_policy_id,
                )
                self.connection.execute(
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
                row = self.connection.execute(
                    """
                    SELECT epoch_id, chunk_version_id, claim_id,
                           candidate_policy_id, fused_rank, reasons,
                           mandatory_lineage
                    FROM groundloop_admitted_pair
                    WHERE admitted_pair_id = %s
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
        return result


def _load_published_repository(
    connection: Connection[Any], epoch_id: int
) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
    """Rebuild process-local IVM state after startup, never during an event."""
    repository = InMemoryRepository()
    repository.advance_epoch()
    question_rows = connection.execute(
        "SELECT question_id, text FROM groundloop_question ORDER BY question_id"
    ).fetchall()
    for question_id, text in question_rows:
        repository.register_question(Question(str(question_id), str(text)))

    answer_rows = connection.execute(
        """
        SELECT answer_version_id, question_id, text, generator_model_id,
               generator_model_version, prompt_version
        FROM groundloop_answer_version ORDER BY answer_version_id
        """
    ).fetchall()
    claims_by_answer: dict[str, list[Claim]] = {}
    claim_rows = connection.execute(
        """
        SELECT claim_id, answer_version_id, text, extractor_model_id,
               extractor_model_version, extractor_prompt_version, required
        FROM groundloop_claim ORDER BY answer_version_id, claim_id
        """
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
        repository.register_answer(
            answer, tuple(claims_by_answer.get(answer.answer_version_id, ()))
        )

    policy_row = connection.execute(
        """
        SELECT policy_version, support_threshold, refute_threshold,
               tie_rule_version
        FROM groundloop_decision_policy
        WHERE valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY valid_from_epoch DESC LIMIT 1
        """,
        (epoch_id, epoch_id),
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

    version_rows = connection.execute(
        """
        SELECT document_version_id, document_id, content_hash
        FROM groundloop_document_version
        WHERE valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY document_version_id
        """,
        (epoch_id, epoch_id),
    ).fetchall()
    for version_row in version_rows:
        version_id = str(version_row[0])
        chunk_rows = connection.execute(
            """
            SELECT chunk_version_id, chunk_index, text, text_hash
            FROM groundloop_chunk_version
            WHERE document_version_id = %s
              AND valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY chunk_version_id
            """,
            (version_id, epoch_id, epoch_id),
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
        repository.register_document_version(
            version, chunks, repository.current_epoch
        )

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
        ORDER BY observation.observation_id
        """
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
    ) -> None:
        self.connection = connection
        self.runtime_store = PostgresM4RuntimeStore(connection)
        self._payloads = dict(structural_payloads)
        self._verification_writer = verification_provenance_writer
        self._failure_injector = failure_injector
        self._fallback_blocked: set[str] = set()
        head = self._publication_head()
        self._published_repository, self._published_engine = (
            _load_published_repository(connection, head)
        )
        self._working_repository = deepcopy(self._published_repository)
        self._working_engine = deepcopy(self._published_engine)
        self._active_epoch_id: int | None = None

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

    def plan_exact_withdrawal(
        self, event: DynamicEventPlan
    ) -> StructuralWithdrawal:
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
                ObservationDependency(
                    str(row[0]), PairKey(str(row[1]), str(row[2]))
                )
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
            WHERE observation.chunk_version_id = ANY(%s)
            ORDER BY observation.observation_id
            """,
            (list(deactivated_chunk_version_ids),),
        ).fetchall()
        candidate_rows = self.connection.execute(
            """
            SELECT claim_id, chunk_version_id, candidate_policy_id,
                   candidate_artifact_hash
            FROM groundloop_candidate_frontier
            WHERE valid_to_epoch IS NULL
              AND chunk_version_id = ANY(%s)
            ORDER BY chunk_version_id, claim_id, candidate_policy_id
            """,
            (list(deactivated_chunk_version_ids),),
        ).fetchall()
        observations = tuple(
            ObservationDependency(
                str(row[0]), PairKey(str(row[1]), str(row[2]))
            )
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
        if payload.inserted_chunk_ids != event.inserted_chunk_version_ids:
            raise EventConflictError("structural payload inserted chunks differ")
        expected_old = self._deactivated_chunk_ids(payload)
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
            payload.inserted is None
            or payload.deactivated_document_version_id is None
        ):
            raise ValidationError("REPLACE structural payload has the wrong shape")

    def _deactivated_chunk_ids(self, payload: StructuralPayload) -> tuple[str, ...]:
        if payload.deactivated_document_version_id is None:
            return ()
        rows = self.connection.execute(
            """
            SELECT chunk_version_id FROM groundloop_chunk_version
            WHERE document_version_id = %s AND valid_to_epoch IS NULL
            ORDER BY chunk_version_id
            """,
            (payload.deactivated_document_version_id,),
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def _stage_structural(
        self, event: DynamicEventPlan, payload: StructuralPayload
    ) -> tuple[InMemoryRepository, IncrementalMaintenanceEngine]:
        before = deepcopy(self._published_repository)
        after = deepcopy(before)
        engine = deepcopy(self._published_engine)
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
        engine.apply_committed_event(semantic_event, before, after)
        return after, engine

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
            opened = self.runtime_store.open_epoch(
                event.update,
                root_jobs,
                discovery_scopes,
                registry_snapshot_id=event.claim_registry_snapshot_id,
                structural_action=lambda _cursor, _epoch_id: None,
                event_manifest=payload.manifest,
            )
            sealed = str(existing[1]) == "sealed"
            return OpenEventReceipt(
                opened.epoch.epoch_id,
                replayed=True,
                already_sealed=sealed,
                publication_id=(
                    stable_m4_digest("m4-publication-v1", str(existing[0]))
                    if sealed
                    else None
                ),
            )

        self._validate_payload(event, payload)
        if withdrawal.plan.deactivated_chunk_ids != event.deactivated_chunk_version_ids:
            raise EventConflictError("withdrawal and event deactivation differ")
        staged_repository, staged_engine = self._stage_structural(event, payload)

        def structural_action(cursor: Cursor[Any], epoch_id: int) -> None:
            self._write_structural_rows(cursor, epoch_id, payload)
            self._write_withdrawal_overlay(cursor, epoch_id, withdrawal)
            self._persist_working_states(
                cursor, epoch_id, 1, staged_engine, causative_digest=None
            )
            self._write_initial_evaluation(cursor, epoch_id, event, withdrawal)
            self._assert_grounding_equality(
                cursor, epoch_id, staged_repository, staged_engine
            )

        opened = self.runtime_store.open_epoch(
            event.update,
            root_jobs,
            discovery_scopes,
            registry_snapshot_id=event.claim_registry_snapshot_id,
            structural_action=structural_action,
            event_manifest=payload.manifest,
        )
        self._working_repository = staged_repository
        self._working_engine = staged_engine
        self._active_epoch_id = opened.epoch.epoch_id
        self._fallback_blocked.clear()
        return OpenEventReceipt(opened.epoch.epoch_id, False, False)

    def _write_structural_rows(
        self, cursor: Cursor[Any], epoch_id: int, payload: StructuralPayload
    ) -> None:
        old_id = payload.deactivated_document_version_id
        if old_id is not None:
            changed = cursor.execute(
                """
                UPDATE groundloop_document_version SET valid_to_epoch = %s
                WHERE document_version_id = %s AND valid_to_epoch IS NULL
                """,
                (epoch_id, old_id),
            ).rowcount
            if changed != 1:
                raise InvalidEventError("deactivated document version is not active")
            cursor.execute(
                """
                UPDATE groundloop_chunk_version SET valid_to_epoch = %s
                WHERE document_version_id = %s AND valid_to_epoch IS NULL
                """,
                (epoch_id, old_id),
            )
            cursor.execute(
                """
                UPDATE groundloop_candidate_frontier SET valid_to_epoch = %s,
                    frontier_state = 'inactive'
                WHERE chunk_version_id IN (
                    SELECT chunk_version_id FROM groundloop_chunk_version
                    WHERE document_version_id = %s
                ) AND valid_to_epoch IS NULL
                """,
                (epoch_id, old_id),
            )
        inserted = payload.inserted
        if inserted is None:
            return
        cursor.execute(
            """
            INSERT INTO groundloop_document(document_id, source_uri, authority_class)
            VALUES (%s, %s, %s) ON CONFLICT (document_id) DO NOTHING
            """,
            (
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
                FROM groundloop_semantic_observation WHERE observation_id = %s
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
        withdrawal: StructuralWithdrawal,
    ) -> None:
        pending = set(withdrawal.fallback_claim_ids)
        if event.inserted_chunk_version_ids:
            pending.update(event.registered_claim_ids)
        head = event.update.previous_published_epoch_id
        claim_rows = cursor.execute(
            "SELECT claim_id FROM groundloop_claim ORDER BY claim_id"
        ).fetchall()
        for (claim_id,) in claim_rows:
            state = "pending" if str(claim_id) in pending else "complete"
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES
                    (%s, 'claim', %s, %s, %s, %s, %s, 1)
                """,
                (
                    epoch_id,
                    claim_id,
                    state,
                    head if state == "pending" else epoch_id,
                    1 if state == "pending" else 0,
                    state == "pending" and bool(event.inserted_chunk_version_ids),
                ),
            )
        answer_rows = cursor.execute(
            "SELECT answer_version_id FROM groundloop_answer_version ORDER BY 1"
        ).fetchall()
        for (answer_id,) in answer_rows:
            answer_pending = cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM groundloop_claim
                    WHERE answer_version_id = %s AND required
                      AND claim_id = ANY(%s)
                )
                """,
                (answer_id, list(pending)),
            ).fetchone()
            is_pending = bool(answer_pending and answer_pending[0])
            cursor.execute(
                """
                INSERT INTO groundloop_object_evaluation VALUES
                    (%s, 'answer', %s, %s, %s, %s, %s, 1)
                """,
                (
                    epoch_id,
                    answer_id,
                    "pending" if is_pending else "complete",
                    head if is_pending else epoch_id,
                    1 if is_pending else 0,
                    is_pending and bool(event.inserted_chunk_version_ids),
                ),
            )

    def chunk_is_active(self, chunk_version_id: str) -> bool:
        row = self.connection.execute(
            """
            SELECT valid_to_epoch IS NULL FROM groundloop_chunk_version
            WHERE chunk_version_id = %s
            """,
            (chunk_version_id,),
        ).fetchone()
        return row is not None and bool(row[0])

    def pending_claim_ids(self, epoch_id: int) -> tuple[str, ...]:
        epoch = self.runtime_store.read_epoch(epoch_id)
        claim_ids = tuple(
            str(row[0])
            for row in self.connection.execute(
                "SELECT claim_id FROM groundloop_claim ORDER BY claim_id"
            ).fetchall()
        )
        return tuple(
            claim_id
            for claim_id in claim_ids
            if claim_evaluation_state(epoch, claim_id).value == "pending"
        )

    def acquire_job(self, epoch_id: int, spec: LogicalJobSpec) -> JobLease:
        epoch = self.runtime_store.read_epoch(epoch_id)
        job = next(
            (item for item in epoch.jobs if item.spec.job_id == spec.job_id),
            None,
        )
        if job is None or job.spec != spec:
            raise EventConflictError("requested job differs from persisted job")
        if job.completion is not None:
            return JobLease(spec.job_id, False, True)
        if job.state in {JobState.DECLARED, JobState.RETRYABLE_FAILED}:
            ordinal = len(job.attempts) + 1
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
            self.runtime_store.start_attempt(epoch_id, attempt)
        elif job.state is not JobState.RUNNING:
            raise InvalidEventError("job is not executable")
        return JobLease(spec.job_id, True, False)

    def complete_expansion(
        self,
        epoch_id: int,
        lease: JobLease,
        completion: JobCompletion,
        child_jobs: tuple[LogicalJobSpec, ...],
    ) -> None:
        del lease
        with self.connection.transaction():
            epoch = self.runtime_store.read_epoch(epoch_id)
            self.runtime_store.complete(
                CompletionPlan(epoch_id, epoch.revision, completion, child_jobs),
                active_chunk_ids=self._active_chunk_ids(),
            )
            with self.connection.cursor() as cursor:
                self._sync_evaluation(cursor, epoch_id)

    def children_of(
        self, epoch_id: int, root_job_id: str
    ) -> tuple[LogicalJobSpec, ...]:
        epoch = self.runtime_store.read_epoch(epoch_id)
        return tuple(
            job.spec for job in epoch.jobs if job.spec.parent_job_id == root_job_id
        )

    def mark_fallback_blocked(self, epoch_id: int, root_job_id: str) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        if not any(job.spec.job_id == root_job_id and job.open for job in epoch.jobs):
            raise InvalidEventError("fallback root is not open")
        self._fallback_blocked.add(root_job_id)

    def fail_epoch(self, epoch_id: int, reason: str) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        if epoch.state is RuntimeEpochState.FAILED:
            return
        with self.connection.transaction():
            self.runtime_store.fail_epoch(epoch_id, epoch.revision, reason)
            with self.connection.cursor() as cursor:
                self._sync_evaluation(cursor, epoch_id)

    def sealing_snapshot(self, epoch_id: int) -> SealingSnapshot:
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
        del lease
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

        staged_repository = deepcopy(self._working_repository)
        staged_engine = deepcopy(self._working_engine)
        if make_effective:
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
        with self.connection.transaction():
            epoch = self.runtime_store.read_epoch(epoch_id)
            self.runtime_store.complete(
                CompletionPlan(epoch_id, epoch.revision, completion),
                active_chunk_ids=self._active_chunk_ids(),
            )
            self._inject("verifier_runtime_completed")
            inserted = self._insert_observation(observation, epoch_id, completion)
            self._inject("verifier_observation_archived")
            if verifier_job.pair is not None:
                self.connection.execute(
                    """
                    UPDATE groundloop_candidate_frontier
                    SET frontier_state = %s
                    WHERE claim_id = %s AND chunk_version_id = %s
                      AND candidate_policy_id = %s
                      AND valid_to_epoch IS NULL
                    """,
                    (
                        "verified_current" if make_effective else "inactive",
                        verifier_job.pair.claim_id,
                        verifier_job.pair.chunk_version_id,
                        verifier_job.candidate_policy_id,
                    ),
                )
            if self._verification_writer is not None:
                with self.connection.cursor() as cursor:
                    self._verification_writer(
                        cursor, epoch_id, verifier_job, completion, observation
                    )
            completed_epoch = self.runtime_store.read_epoch(epoch_id)
            if make_effective:
                self._insert_working_observation_delta(
                    epoch_id, completed_epoch.revision, observation
                )
                self._inject("verifier_overlay_written")
                with self.connection.cursor() as cursor:
                    self._persist_working_states(
                        cursor,
                        epoch_id,
                        completed_epoch.revision,
                        staged_engine,
                        causative_digest=completion.completion_digest,
                    )
                    self._assert_grounding_equality(
                        cursor, epoch_id, staged_repository, staged_engine
                    )
                self._inject("verifier_state_written")
            with self.connection.cursor() as cursor:
                self._sync_evaluation(cursor, epoch_id)
        if make_effective:
            self._working_repository = staged_repository
            self._working_engine = staged_engine
        return ObservationCompletionReceipt(inserted, make_effective)

    def _insert_observation(
        self,
        observation: SemanticObservation,
        epoch_id: int,
        completion: JobCompletion,
    ) -> bool:
        inserted = self.connection.execute(
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
        row = self.connection.execute(
            """
            SELECT subject_kind, subject_id, chunk_version_id, task_type,
                   support_score, refute_score, neutral_score, model_id,
                   model_version, prompt_version, input_hash
            FROM groundloop_semantic_observation WHERE observation_id = %s
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
        )
        if row is None or tuple(row) != expected:
            raise EventConflictError(
                "observation identity was reused with different content"
            )
        return inserted is not None

    def _insert_working_observation_delta(
        self, epoch_id: int, revision: int, observation: SemanticObservation
    ) -> None:
        base = self.connection.execute(
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
        self.connection.execute(
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

    def _active_chunk_ids(self) -> frozenset[str]:
        return frozenset(
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT chunk_version_id FROM groundloop_chunk_version
                WHERE valid_to_epoch IS NULL
                """
            ).fetchall()
        )

    def _persist_working_states(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        revision: int,
        engine: IncrementalMaintenanceEngine,
        *,
        causative_digest: str | None,
    ) -> None:
        old_claims = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT claim_id, status FROM groundloop_m4_working_claim_state
                WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchall()
        }
        if not old_claims:
            old_claims = {
                str(row[0]): str(row[1])
                for row in cursor.execute(
                    """
                    SELECT claim_id, status FROM groundloop_published_claim_state
                    WHERE valid_to_epoch IS NULL
                    """
                ).fetchall()
            }
        old_answers = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT answer_version_id, status
                FROM groundloop_m4_working_answer_state WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchall()
        }
        if not old_answers:
            old_answers = {
                str(row[0]): str(row[1])
                for row in cursor.execute(
                    """
                    SELECT answer_version_id, status
                    FROM groundloop_published_answer_state
                    WHERE valid_to_epoch IS NULL
                    """
                ).fetchall()
            }
        for claim_state in engine.claim_states.values():
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
        for answer_state in engine.answer_states.values():
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

    def _sync_evaluation(self, cursor: Cursor[Any], epoch_id: int) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        head = self._publication_head()
        claims = cursor.execute(
            "SELECT claim_id, answer_version_id, required FROM groundloop_claim"
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
                    epoch_id if state.value == "complete" else head,
                    open_count,
                    scope_open,
                    epoch.revision,
                ),
            )
        answer_rows = cursor.execute(
            "SELECT answer_version_id FROM groundloop_answer_version ORDER BY 1"
        ).fetchall()
        for (answer_id,) in answer_rows:
            required = tuple(sorted(by_answer.get(str(answer_id), ())))
            state = answer_evaluation_state(epoch, required)
            open_count = sum(
                1
                for claim_id in required
                if claim_evaluation_state(epoch, claim_id).value != "complete"
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
                    epoch_id if state.value == "complete" else head,
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
            FROM groundloop_m4_working_claim_state
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
            FROM groundloop_m4_working_answer_state
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
    ) -> None:
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
        with self.connection.cursor() as cursor:
            self._assert_grounding_equality(
                cursor,
                epoch_id,
                self._working_repository,
                self._working_engine,
            )

    def check_coordination(self, epoch_id: int) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        book = self.runtime_store.read_book()
        if not any(item == epoch for item in book.epochs):
            raise ValidationError("runtime book omits the requested epoch")
        if epoch.state is not RuntimeEpochState.SEMANTIC_COMPLETE:
            raise ValidationError("coordination surface is not complete")

    def check_evaluation(self, epoch_id: int) -> None:
        epoch = self.runtime_store.read_epoch(epoch_id)
        expected: dict[tuple[str, str], str] = {}
        claim_rows = self.connection.execute(
            "SELECT claim_id, answer_version_id, required FROM groundloop_claim"
        ).fetchall()
        by_answer: dict[str, list[str]] = {}
        for claim_id, answer_id, required in claim_rows:
            claim = str(claim_id)
            expected[("claim", claim)] = claim_evaluation_state(epoch, claim).value
            if bool(required):
                by_answer.setdefault(str(answer_id), []).append(claim)
        for answer_id, claims in by_answer.items():
            expected[("answer", answer_id)] = answer_evaluation_state(
                epoch, tuple(sorted(claims))
            ).value
        actual = {
            (str(row[0]), str(row[1])): str(row[2])
            for row in self.connection.execute(
                """
                SELECT object_type, object_id, evaluation_state
                FROM groundloop_object_evaluation WHERE epoch_id = %s
                """,
                (epoch_id,),
            ).fetchall()
        }
        if actual != expected or any(value != "complete" for value in actual.values()):
            raise ValidationError("persisted evaluation surface differs from runtime")

    def request_seal(
        self,
        epoch_id: int,
        expected_revision: int,
        update: CorpusUpdateIdentity,
    ) -> PublicationReceipt:
        epoch = self.runtime_store.read_epoch(epoch_id)
        if epoch.update != update:
            raise EventConflictError("publication update differs from runtime epoch")

        def publish(cursor: Cursor[Any], published_epoch_id: int) -> None:
            self._assert_grounding_equality(
                cursor,
                published_epoch_id,
                self._working_repository,
                self._working_engine,
            )
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

        self.runtime_store.seal_epoch(
            epoch_id,
            expected_revision,
            publication_action=publish,
            failure_injector=(
                lambda point: self._inject(f"publication_store_{point}")
            ),
        )
        self._published_repository = deepcopy(self._working_repository)
        self._published_engine = deepcopy(self._working_engine)
        self._active_epoch_id = None
        publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
        return PublicationReceipt(epoch_id, publication_id)

    def _promote_observation_currency(
        self, cursor: Cursor[Any], epoch_id: int
    ) -> None:
        rows = cursor.execute(
            """
            SELECT subject_kind, subject_id, chunk_version_id, task_type,
                   base_observation_id, working_observation_id
            FROM groundloop_working_observation_delta
            WHERE epoch_id = %s ORDER BY subject_kind, subject_id,
                                         chunk_version_id, task_type
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
                INSERT INTO groundloop_published_observation_currency VALUES
                    (%s, %s, %s, %s, %s, %s, NULL)
                """,
                (*key, working_id, epoch_id),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_observation_currency VALUES
                    (%s, %s, %s, %s, %s, %s)
                """,
                (*key, working_id, epoch_id),
            )

    def _publish_grounding_states(
        self,
        cursor: Cursor[Any],
        epoch_id: int,
        revision: int,
        update: CorpusUpdateIdentity,
    ) -> None:
        old_claims = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT claim_id, status FROM groundloop_published_claim_state
                WHERE valid_to_epoch IS NULL
                """
            ).fetchall()
        }
        old_answers = {
            str(row[0]): str(row[1])
            for row in cursor.execute(
                """
                SELECT answer_version_id, status
                FROM groundloop_published_answer_state
                WHERE valid_to_epoch IS NULL
                """
            ).fetchall()
        }
        cursor.execute(
            """
            UPDATE groundloop_published_claim_state SET valid_to_epoch = %s
            WHERE valid_to_epoch IS NULL
            """,
            (epoch_id,),
        )
        cursor.execute(
            """
            UPDATE groundloop_published_answer_state SET valid_to_epoch = %s
            WHERE valid_to_epoch IS NULL
            """,
            (epoch_id,),
        )
        for claim_state in self._working_engine.claim_states.values():
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
            if old is not None and old != claim_state.status.value:
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
        for answer_state in self._working_engine.answer_states.values():
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
            if old is not None and old != answer_state.status.value:
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
