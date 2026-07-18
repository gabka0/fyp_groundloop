"""Typed PostgreSQL snapshot serialization for GroundLoop M2.

The adapter persists one already-committed in-memory snapshot. It does not
implement either the Python oracle or the signed-delta algorithm. PostgreSQL
recomputes its independent oracle exclusively from the persisted base rows.

M1 does not record creation revisions for static questions/answers or
production revisions for observations. A snapshot import therefore assigns
those records to the first/current captured revision respectively. This is an
explicit serialization convention and does not affect the M2 snapshot
semantics compared by the three engines.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import Connection, sql

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
    Question,
    SemanticObservation,
    StatusDelta,
)
from groundloop.incremental import ClaimCertificate, IncrementalMaintenanceEngine
from groundloop.repository import InMemoryRepository, Interval

ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class EpochRow:
    epoch_id: int
    event_id: str
    payload_hash: str


@dataclass(frozen=True, slots=True)
class VersionedDocumentRow:
    value: DocumentVersion
    validity: Interval


@dataclass(frozen=True, slots=True)
class VersionedChunkRow:
    value: ChunkVersion
    validity: Interval


@dataclass(frozen=True, slots=True)
class VersionedPolicyRow:
    value: DecisionPolicy
    validity: Interval


@dataclass(frozen=True, slots=True)
class MaterializedClaimRow:
    state: ClaimState
    certificate: ClaimCertificate


@dataclass(frozen=True, slots=True)
class PostgresSnapshot:
    """Immutable, typed rows needed to reproduce one M2 base snapshot."""

    revision: int
    epochs: tuple[EpochRow, ...]
    questions: tuple[Question, ...]
    answers: tuple[AnswerVersion, ...]
    claims: tuple[Claim, ...]
    document_versions: tuple[VersionedDocumentRow, ...]
    chunks: tuple[VersionedChunkRow, ...]
    policies: tuple[VersionedPolicyRow, ...]
    observations: tuple[SemanticObservation, ...]
    current_observation_ids: tuple[str, ...]
    materialized_claims: tuple[MaterializedClaimRow, ...]
    materialized_answers: tuple[AnswerState, ...]
    status_deltas: tuple[StatusDelta, ...]

    @classmethod
    def capture(
        cls,
        repository: InMemoryRepository,
        engine: IncrementalMaintenanceEngine,
    ) -> PostgresSnapshot:
        """Capture base relations and independent incremental output.

        Private repository collections are read because M1 deliberately keeps
        its historical store encapsulated and exposes no bulk persistence
        contract. No collection is mutated. If that contract changes, this
        lane must request a coordinator-owned shared-contract update.
        """
        if repository.current_epoch <= 0:
            raise ValueError("a PostgreSQL snapshot requires at least one revision")
        processed = tuple(repository._processed_events.items())
        if len(processed) != repository.current_epoch:
            raise ValueError(
                "snapshot revisions cannot be mapped one-to-one to processed events"
            )
        epochs = tuple(
            EpochRow(epoch_id=index, event_id=event_id, payload_hash=record[0])
            for index, (event_id, record) in enumerate(processed, start=1)
        )
        claim_states = engine.claim_states
        certificates = engine.certificates
        answer_states = engine.answer_states
        if set(claim_states) != set(repository.all_claim_ids()):
            raise ValueError("incremental claim state is not synchronized to snapshot")
        if set(certificates) != set(claim_states):
            raise ValueError("incremental certificates are incomplete")
        if set(answer_states) != set(repository.all_answer_ids()):
            raise ValueError("incremental answer state is not synchronized to snapshot")
        return cls(
            revision=repository.current_epoch,
            epochs=epochs,
            questions=tuple(
                repository._questions[key] for key in sorted(repository._questions)
            ),
            answers=tuple(
                repository._answers[key] for key in sorted(repository._answers)
            ),
            claims=tuple(repository._claims[key] for key in sorted(repository._claims)),
            document_versions=tuple(
                VersionedDocumentRow(
                    repository._document_versions[key],
                    repository._document_version_validity[key],
                )
                for key in sorted(repository._document_versions)
            ),
            chunks=tuple(
                VersionedChunkRow(
                    repository._chunk_versions[key], repository._chunk_validity[key]
                )
                for key in sorted(repository._chunk_versions)
            ),
            policies=tuple(
                VersionedPolicyRow(
                    repository._policies[key], repository._policy_validity[key]
                )
                for key in sorted(repository._policies)
            ),
            observations=tuple(
                repository._observations[key]
                for key in sorted(repository._observations)
            ),
            current_observation_ids=tuple(
                sorted(repository._current_by_key.values())
            ),
            materialized_claims=tuple(
                MaterializedClaimRow(claim_states[key], certificates[key])
                for key in sorted(claim_states)
            ),
            materialized_answers=tuple(
                answer_states[key] for key in sorted(answer_states)
            ),
            status_deltas=tuple(repository.status_deltas),
        )


@dataclass(frozen=True, slots=True)
class OracleStates:
    claims: dict[str, ClaimState]
    answers: dict[str, AnswerState]


@dataclass(frozen=True, slots=True)
class MismatchCounts:
    claims: int
    answers: int
    invalid_certificates: int


@dataclass(frozen=True, slots=True)
class ServerMetadata:
    postgres_version: str
    postgres_version_num: int
    pgvector_available_version: str | None
    pgvector_installed_version: str | None


class PostgresEventConflictError(RuntimeError):
    """An event identifier was replayed with a different payload hash."""


def apply_m2_schema(connection: Connection[Any]) -> None:
    """Apply the owned migration and independent oracle in the search path."""
    migrations = sorted((ROOT / "migrations").glob("*.sql"))
    oracle = (ROOT / "sql/m2_full_recompute_oracle.sql").read_text()
    for migration in migrations:
        connection.execute(migration.read_text())
    connection.execute(oracle)


def record_epoch(
    connection: Connection[Any],
    *,
    event_id: str,
    payload_hash: str,
) -> tuple[int, bool]:
    """Idempotently record a received epoch, rejecting payload conflicts.

    Returns ``(epoch_id, created)``. Exact replay returns the existing epoch
    without changing its revision or state.
    """
    inserted = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, structural_status, semantic_status,
            evaluation_state, publication_mode
        ) VALUES (%s, %s, 'received', 'pending', 'pending', 'provisional')
        ON CONFLICT (event_id) DO NOTHING
        RETURNING epoch_id
        """,
        (event_id, payload_hash),
    ).fetchone()
    if inserted is not None:
        return inserted[0], True
    existing = connection.execute(
        """
        SELECT epoch_id, payload_hash FROM groundloop_epoch WHERE event_id = %s
        """,
        (event_id,),
    ).fetchone()
    assert existing is not None
    if existing[1].strip() != payload_hash:
        raise PostgresEventConflictError(
            f"event {event_id} was already recorded with a different payload"
        )
    return existing[0], False


@contextmanager
def temporary_m2_schema(
    connection: Connection[Any], schema: str | None = None
) -> Iterator[str]:
    """Create, initialize, then transactionally remove an isolated schema."""
    schema_name = schema or f"groundloop_m2_{uuid.uuid4().hex}"
    with connection.transaction():
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
        connection.execute(
            sql.SQL("SET LOCAL search_path TO {}, public").format(
                sql.Identifier(schema_name)
            )
        )
        apply_m2_schema(connection)
        try:
            yield schema_name
        finally:
            connection.execute("SET LOCAL search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _epoch_for_event(snapshot: PostgresSnapshot) -> dict[str, int]:
    return {row.event_id: row.epoch_id for row in snapshot.epochs}


def load_snapshot(
    connection: Connection[Any],
    snapshot: PostgresSnapshot,
    failure_injector: Callable[[str], None] | None = None,
) -> None:
    """Load a snapshot atomically into an empty initialized M2 schema."""
    first_epoch = snapshot.epochs[0].epoch_id
    current_epoch = snapshot.epochs[-1].epoch_id
    event_epochs = _epoch_for_event(snapshot)
    with connection.transaction():
        for epoch in snapshot.epochs:
            connection.execute(
                """
                INSERT INTO groundloop_epoch (
                    epoch_id, event_id, payload_hash, revision,
                    structural_status, semantic_status, evaluation_state,
                    publication_mode, sealed_at
                ) OVERRIDING SYSTEM VALUE VALUES (
                    %s, %s, %s, %s, 'committed', 'sealed', 'complete',
                    'provisional', now()
                )
                """,
                (epoch.epoch_id, epoch.event_id, epoch.payload_hash, epoch.epoch_id),
            )
        document_ids = sorted(
            {row.value.document_id for row in snapshot.document_versions}
        )
        for document_id in document_ids:
            connection.execute(
                """
                INSERT INTO groundloop_document
                    (document_id, source_uri, authority_class)
                VALUES (%s, %s, 'snapshot')
                """,
                (document_id, f"snapshot://{document_id}"),
            )
        for document_row in snapshot.document_versions:
            value = document_row.value
            connection.execute(
                """
                INSERT INTO groundloop_document_version (
                    document_version_id, document_id, content_hash,
                    valid_from_epoch, valid_to_epoch
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    value.document_version_id,
                    value.document_id,
                    value.content_hash,
                    document_row.validity[0],
                    document_row.validity[1],
                ),
            )
        for chunk_row in snapshot.chunks:
            chunk = chunk_row.value
            connection.execute(
                """
                INSERT INTO groundloop_chunk_version (
                    chunk_version_id, document_version_id, chunk_index, text,
                    text_hash, chunker_version, valid_from_epoch, valid_to_epoch
                ) VALUES (%s, %s, %s, %s, %s, 'snapshot-v1', %s, %s)
                """,
                (
                    chunk.chunk_version_id,
                    chunk.document_version_id,
                    chunk.chunk_index,
                    chunk.text,
                    chunk.text_hash,
                    chunk_row.validity[0],
                    chunk_row.validity[1],
                ),
            )
        for question in snapshot.questions:
            connection.execute(
                """
                INSERT INTO groundloop_question (question_id, text, created_epoch)
                VALUES (%s, %s, %s)
                """,
                (question.question_id, question.text, first_epoch),
            )
        for answer in snapshot.answers:
            connection.execute(
                """
                INSERT INTO groundloop_answer_version (
                    answer_version_id, question_id, text, generator_model_id,
                    generator_model_version, prompt_version, created_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    answer.answer_version_id,
                    answer.question_id,
                    answer.text,
                    answer.producer.model_id,
                    answer.producer.model_version,
                    answer.producer.prompt_version,
                    first_epoch,
                ),
            )
        for claim in snapshot.claims:
            connection.execute(
                """
                INSERT INTO groundloop_claim (
                    claim_id, answer_version_id, text, extractor_model_id,
                    extractor_model_version, extractor_prompt_version, required
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    claim.claim_id,
                    claim.answer_version_id,
                    claim.text,
                    claim.extractor.model_id,
                    claim.extractor.model_version,
                    claim.extractor.prompt_version,
                    claim.required,
                ),
            )
        for policy_row in snapshot.policies:
            policy = policy_row.value
            connection.execute(
                """
                INSERT INTO groundloop_decision_policy (
                    policy_version, support_threshold, refute_threshold,
                    tie_rule_version, valid_from_epoch, valid_to_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    policy.policy_version,
                    policy.support_threshold,
                    policy.refute_threshold,
                    policy.tie_rule_version,
                    policy_row.validity[0],
                    policy_row.validity[1],
                ),
            )
        if failure_injector is not None:
            failure_injector("base_rows_loaded")
        for observation in snapshot.observations:
            connection.execute(
                """
                INSERT INTO groundloop_semantic_observation (
                    observation_id, subject_kind, subject_id, chunk_version_id,
                    task_type, support_score, refute_score, neutral_score,
                    model_id, model_version, prompt_version, input_hash,
                    produced_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    current_epoch,
                ),
            )
        current_ids = set(snapshot.current_observation_ids)
        for observation in snapshot.observations:
            if observation.observation_id not in current_ids:
                continue
            connection.execute(
                """
                INSERT INTO groundloop_observation_currency (
                    subject_kind, subject_id, chunk_version_id, task_type,
                    observation_id, installed_revision
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    observation.subject_kind.value,
                    observation.subject_id,
                    observation.chunk_version_id,
                    observation.task_type,
                    observation.observation_id,
                    snapshot.revision,
                ),
            )
        if failure_injector is not None:
            failure_injector("observations_loaded")
        for claim_row in snapshot.materialized_claims:
            claim_state = claim_row.state
            connection.execute(
                """
                INSERT INTO groundloop_claim_state_materialized VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
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
                    current_epoch,
                    snapshot.revision,
                ),
            )
            certificate = claim_row.certificate
            connection.execute(
                """
                INSERT INTO groundloop_claim_certificate VALUES
                    (%s, %s, %s, %s, %s)
                """,
                (
                    certificate.claim_id,
                    certificate.support_observation_id,
                    certificate.refute_observation_id,
                    current_epoch,
                    snapshot.revision,
                ),
            )
        for state in snapshot.materialized_answers:
            connection.execute(
                """
                INSERT INTO groundloop_answer_state_materialized VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    state.answer_version_id,
                    state.required_claim_count,
                    state.supported_count,
                    state.unsupported_count,
                    state.refuted_count,
                    state.conflicted_count,
                    state.status.value,
                    current_epoch,
                    snapshot.revision,
                ),
            )
        for delta in snapshot.status_deltas:
            epoch_id = event_epochs[delta.event_id]
            connection.execute(
                """
                INSERT INTO groundloop_status_delta (
                    event_id, epoch_id, revision, object_type, object_id,
                    old_status, new_status, reason
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    delta.event_id,
                    epoch_id,
                    epoch_id,
                    delta.object_type,
                    delta.object_id,
                    delta.old_status,
                    delta.new_status,
                    delta.reason,
                ),
            )
        if failure_injector is not None:
            failure_injector("materialized_rows_loaded")
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")


def read_oracle_states(connection: Connection[Any]) -> OracleStates:
    """Read the independent SQL oracle into the shared typed state records."""
    claims: dict[str, ClaimState] = {}
    claim_rows = connection.execute(
        """
        SELECT claim_id, support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, status
        FROM groundloop_claim_state_oracle ORDER BY claim_id
        """
    ).fetchall()
    for row in claim_rows:
        state = ClaimState(
            claim_id=row[0],
            support_count=row[1],
            refute_count=row[2],
            best_support_score=row[3],
            best_refute_score=row[4],
            supporting_observation_ids=tuple(row[5]),
            refuting_observation_ids=tuple(row[6]),
            status=ClaimStatus(row[7]),
        )
        claims[state.claim_id] = state
    answers: dict[str, AnswerState] = {}
    answer_rows = connection.execute(
        """
        SELECT answer_version_id, required_claim_count, supported_count,
               unsupported_count, refuted_count, conflicted_count, status
        FROM groundloop_answer_state_oracle ORDER BY answer_version_id
        """
    ).fetchall()
    for row in answer_rows:
        answer_state = AnswerState(
            answer_version_id=row[0],
            required_claim_count=row[1],
            supported_count=row[2],
            unsupported_count=row[3],
            refuted_count=row[4],
            conflicted_count=row[5],
            status=AnswerStatus(row[6]),
        )
        answers[answer_state.answer_version_id] = answer_state
    return OracleStates(claims=claims, answers=answers)


def read_mismatch_counts(connection: Connection[Any]) -> MismatchCounts:
    claims = connection.execute(
        "SELECT count(*) FROM groundloop_claim_state_mismatches"
    ).fetchone()
    answers = connection.execute(
        "SELECT count(*) FROM groundloop_answer_state_mismatches"
    ).fetchone()
    invalid = connection.execute(
        """
        SELECT count(*) FROM groundloop_claim_certificate_validity_oracle
        WHERE NOT certificate_valid
        """
    ).fetchone()
    assert claims is not None and answers is not None and invalid is not None
    return MismatchCounts(claims[0], answers[0], invalid[0])


def read_server_metadata(connection: Connection[Any]) -> ServerMetadata:
    row = connection.execute(
        """
        SELECT current_setting('server_version'),
               current_setting('server_version_num')::integer,
               available.default_version,
               installed.extversion
        FROM pg_available_extensions AS available
        LEFT JOIN pg_extension AS installed ON installed.extname = available.name
        WHERE available.name = 'vector'
        """
    ).fetchone()
    if row is None:
        version = connection.execute(
            """
            SELECT current_setting('server_version'),
                   current_setting('server_version_num')::integer
            """
        ).fetchone()
        assert version is not None
        return ServerMetadata(version[0], version[1], None, None)
    return ServerMetadata(row[0], row[1], row[2], row[3])
