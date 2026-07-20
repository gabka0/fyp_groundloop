"""Black-box fixtures for the adversarial M4.7 physical-runtime gate.

The measured kernel is exercised only through ``M4Application`` and
``PostgresM4ApplicationPorts``.  Setup deliberately creates a sealed M4
history with unrelated jobs, but resets SQL tracing before the event under
test.  Full grounding and coordination reads happen only after the measured
kernel has returned.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

from psycopg import Connection, Cursor, sql
from psycopg.abc import Params, Query
from psycopg.types.json import Jsonb

from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    normalized_text_hash,
)
from groundloop.m4.application import DiscoveryResult, VerificationResult
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    JobAttempt,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.pipeline import InsertedDocument, bootstrap_m4_publication
from groundloop.postgres import apply_m2_schema, record_epoch

CHILD_COUNT = 3
CANDIDATE_POLICY_ID = "m4-physical-gate-policy-v1"
REGISTRY_SNAPSHOT_ID = "m4-physical-gate-registry-v1"
DECISION_POLICY_ID = "m4-physical-gate-decision-v1"
EMBEDDING_ARTIFACT_ID = "m4-physical-gate-embedder-v1"
INSERTED_CHUNK_ID = "m4-physical-gate-inserted-chunk"
IMPACT_EXECUTION_HASH = stable_m4_digest("m4-physical-gate-impact")
FRONTIER_EXECUTION_HASH = stable_m4_digest("m4-physical-gate-frontier")
VERIFIER_EXECUTION_HASH = stable_m4_digest("m4-physical-gate-verifier")


def digest(*values: str) -> str:
    return stable_m4_digest(*values)


def _query_text(query: Query, connection: Connection[Any]) -> str:
    if isinstance(query, str):
        rendered = query
    elif isinstance(query, bytes):
        rendered = query.decode()
    else:
        rendered = query.as_string(connection)
    return " ".join(rendered.split())


class CountingCursor(Cursor[Any]):
    """Count SQL executions without changing parameters or query results."""

    def execute(
        self,
        query: Query,
        params: Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None,
    ) -> CountingCursor:
        connection = cast(CountingConnection, self.connection)
        connection.statement_fingerprints.append(_query_text(query, connection))
        super().execute(query, params, prepare=prepare, binary=binary)
        return self

    def executemany(
        self,
        query: Query,
        params_seq: Iterable[Params],
        *,
        returning: bool = False,
    ) -> None:
        materialized = tuple(params_seq)
        connection = cast(CountingConnection, self.connection)
        fingerprint = _query_text(query, connection)
        connection.statement_fingerprints.extend(
            fingerprint for _item in materialized
        )
        super().executemany(query, materialized, returning=returning)


class CountingConnection(Connection[Any]):
    """Psycopg connection with a resettable client-side SQL trace."""

    statement_fingerprints: list[str]

    def reset_statement_trace(self) -> None:
        self.statement_fingerprints.clear()


@contextmanager
def isolated_connection(
    database_url: str, *, label: str
) -> Iterator[CountingConnection]:
    schema = f"groundloop_m4_physical_{label}_{uuid.uuid4().hex}"
    connection = CountingConnection.connect(
        database_url,
        autocommit=True,
        cursor_factory=CountingCursor,
    )
    connection.statement_fingerprints = []
    try:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(schema)
            )
        )
        with connection.transaction():
            apply_m2_schema(connection)
        yield connection
    finally:
        connection.execute("SET search_path TO public")
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
        connection.close()


def claim_ids(registry_size: int) -> tuple[str, ...]:
    return tuple(f"physical-claim-{index:04d}" for index in range(registry_size))


def answer_ids(registry_size: int) -> tuple[str, ...]:
    return tuple(f"physical-answer-{index:04d}" for index in range(registry_size))


def candidate_policy(registry_size: int) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id=CANDIDATE_POLICY_ID,
        embedding_model_artifact_id=EMBEDDING_ARTIFACT_ID,
        claim_role_template_hash=digest("physical-claim-role"),
        chunk_role_template_hash=digest("physical-chunk-role"),
        vector_method_version="exact-reverse-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=digest("physical-vector-build"),
        vector_search_config_hash=digest("physical-vector-search"),
        lexical_method_version="postgres-simple-v1",
        lexical_config_hash=digest("physical-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id=REGISTRY_SNAPSHOT_ID,
        claim_count=registry_size,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=CHILD_COUNT,
        frontier_depth=CHILD_COUNT,
        verifier_execution_spec_hash=VERIFIER_EXECUTION_HASH,
        decision_policy_version=DECISION_POLICY_ID,
    )


def seed_published_base(
    connection: Connection[Any], *, registry_size: int
) -> int:
    """Seed an unsupported published snapshot and one unrelated active chunk."""

    base_epoch, created = record_epoch(
        connection,
        event_id="m4-physical-gate-base",
        payload_hash=digest("m4-physical-gate-base"),
    )
    assert created
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET revision = 1, structural_status = 'committed',
            semantic_status = 'sealed', evaluation_state = 'complete',
            publication_mode = 'strict', sealed_at = now()
        WHERE epoch_id = %s
        """,
        (base_epoch,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES "
        "('physical-decoy-document', 'fixture://physical-decoy', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES "
        "('physical-decoy-version', 'physical-decoy-document', %s, %s, NULL)",
        (digest("physical-decoy-version"), base_epoch),
    )
    text = "An unrelated historical passage."
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES (
            'physical-decoy-chunk', 'physical-decoy-version', 0, %s, %s,
            'fixed-char-v1', %s, NULL
        )
        """,
        (text, normalized_text_hash(text), base_epoch),
    )
    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO groundloop_question VALUES (%s, %s, %s)",
            (
                (f"physical-question-{index:04d}", f"Question {index}?", base_epoch)
                for index in range(registry_size)
            ),
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_answer_version VALUES (
                %s, %s, %s, 'generator', 'v1', 'prompt-v1', %s
            )
            """,
            (
                (
                    f"physical-answer-{index:04d}",
                    f"physical-question-{index:04d}",
                    f"Answer {index}.",
                    base_epoch,
                )
                for index in range(registry_size)
            ),
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_claim VALUES (
                %s, %s, %s, 'extractor', 'v1', 'prompt-v1', true
            )
            """,
            (
                (
                    f"physical-claim-{index:04d}",
                    f"physical-answer-{index:04d}",
                    f"Claim {index}.",
                )
                for index in range(registry_size)
            ),
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_claim_state_materialized VALUES (
                %s, 0, 0, NULL, NULL, ARRAY[]::text[], ARRAY[]::text[],
                'unsupported', %s, 1
            )
            """,
            ((claim_id, base_epoch) for claim_id in claim_ids(registry_size)),
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_answer_state_materialized VALUES (
                %s, 1, 0, 1, 0, 0, 'unsupported', %s, 1
            )
            """,
            ((answer_id, base_epoch) for answer_id in answer_ids(registry_size)),
        )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy VALUES (
            %s, 0.8, 0.8, 'v1', NULL, NULL, %s, NULL
        )
        """,
        (DECISION_POLICY_ID, base_epoch),
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES (%s, 'embedding', 'test', 'embedder', 'v1', 'v1', 'MIT', %s)
        """,
        (EMBEDDING_ARTIFACT_ID, digest("physical-embedder-config")),
    )
    bootstrap_m4_publication(connection, sealed_epoch_id=base_epoch)
    return base_epoch


def seed_unrelated_runtime_jobs(
    connection: Connection[Any], *, base_epoch: int, job_count: int
) -> None:
    """Make the published predecessor a real M4 epoch with unrelated jobs."""

    connection.execute(
        """
        INSERT INTO groundloop_m4_update (
            epoch_id, update_kind, candidate_policy_id,
            previous_published_epoch_id, registry_snapshot_id, manifest
        ) VALUES (%s, 'insert', %s, NULL, %s, %s)
        """,
        (
            base_epoch,
            CANDIDATE_POLICY_ID,
            REGISTRY_SNAPSHOT_ID,
            Jsonb(
                {
                    "_groundloop_m4_runtime_v1": {
                        "event_manifest": {"fixture": "unrelated-runtime-history"},
                        "scope_claim_ids": {},
                        "failure_reason": None,
                    }
                }
            ),
        ),
    )
    job_rows: list[tuple[object, ...]] = []
    attempt_rows: list[tuple[object, ...]] = []
    for index in range(job_count):
        execution_hash = digest("unrelated-execution", str(index))
        payload_hash = digest("unrelated-payload", str(index))
        pair = PairKey("physical-claim-0000", "physical-decoy-chunk")
        job_id = LogicalJobSpec.derive_job_id(
            event_id="m4-physical-gate-base",
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id=CANDIDATE_POLICY_ID,
            execution_spec_hash=execution_hash,
            claim_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
        )
        spec = LogicalJobSpec(
            job_id=job_id,
            event_id="m4-physical-gate-base",
            kind=JobKind.VERIFY_PAIR,
            candidate_policy_id=CANDIDATE_POLICY_ID,
            payload_hash=payload_hash,
            execution_spec_hash=execution_hash,
            pair=pair,
        )
        result_hash = digest("unrelated-result", str(index))
        completion = JobCompletion.build(
            job_id=spec.job_id,
            payload_hash=spec.payload_hash,
            execution_spec_hash=spec.execution_spec_hash,
            result_artifact_id=f"unrelated-result-{index:04d}",
            result_artifact_hash=result_hash,
            terminal_state=JobState.COMPLETED_ACTIVE,
        )
        attempt_id = f"unrelated-attempt-{index:04d}"
        attempt = JobAttempt(
            attempt_id=attempt_id,
            job_id=spec.job_id,
            execution_spec_hash=spec.execution_spec_hash,
            attempt_ordinal=1,
            lease_token_hash=digest("unrelated-lease", str(index)),
        )
        job_rows.append(
            (
                spec.job_id,
                base_epoch,
                spec.kind.value,
                spec.candidate_policy_id,
                spec.payload_hash,
                spec.execution_spec_hash,
                pair.claim_id,
                pair.chunk_version_id,
                completion.terminal_state.value,
                completion.completion_digest,
                completion.result_artifact_id,
                completion.result_artifact_hash,
            )
        )
        attempt_rows.append(
            (
                attempt.attempt_id,
                attempt.job_id,
                attempt.execution_spec_hash,
                attempt.attempt_ordinal,
                attempt.lease_token_hash,
            )
        )
    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO groundloop_semantic_job (
                job_id, epoch_id, parent_job_id, job_kind,
                candidate_policy_id, payload_hash, execution_spec_hash,
                claim_id, chunk_version_id, expandable, job_state,
                child_closed, completion_digest, result_artifact_id,
                result_artifact_hash, created_revision, completed_revision,
                completed_at
            ) VALUES (
                %s, %s, NULL, %s, %s, %s, %s, %s, %s, false, %s,
                false, %s, %s, %s, 1, 1, now()
            )
            """,
            job_rows,
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_semantic_job_attempt (
                attempt_id, job_id, execution_spec_hash, attempt_ordinal,
                lease_token_hash, attempt_state, lease_expires_at, finished_at
            ) VALUES (%s, %s, %s, %s, %s, 'completed', now(), now())
            """,
            attempt_rows,
        )


def inserted_document() -> InsertedDocument:
    text = "The first three physical-gate claims are supported."
    return InsertedDocument(
        DocumentVersion(
            "m4-physical-gate-version",
            "m4-physical-gate-document",
            digest("m4-physical-gate-document-content"),
        ),
        (
            ChunkVersion(
                INSERTED_CHUNK_ID,
                "m4-physical-gate-version",
                0,
                text,
            ),
        ),
        source_uri="fixture://m4-physical-gate",
    )


@dataclass(slots=True)
class ThreePairAdmission:
    target_claim_ids: tuple[str, ...]
    calls: int = 0

    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult:
        self.calls += 1
        assert root_job.target_chunk_version_id == INSERTED_CHUNK_ID
        admitted: list[AdmittedPair] = []
        hits: list[ChannelHit] = []
        for rank, claim_id in enumerate(self.target_claim_ids, start=1):
            pair = PairKey(claim_id, INSERTED_CHUNK_ID)
            admitted.append(
                AdmittedPair(
                    epoch_id,
                    pair,
                    CANDIDATE_POLICY_ID,
                    rank,
                    (AdmissionChannel.VECTOR,),
                    False,
                )
            )
            hits.append(
                ChannelHit(
                    epoch_id,
                    pair,
                    CANDIDATE_POLICY_ID,
                    AdmissionChannel.VECTOR,
                    rank,
                    1.0 - (rank / 100.0),
                    digest("physical-hit", root_job.job_id, claim_id),
                )
            )
        result_hash = digest("physical-discovery", root_job.job_id)
        return DiscoveryResult(
            root_job.job_id,
            f"physical-discovery-{root_job.job_id}",
            result_hash,
            tuple(admitted),
            channel_hits=tuple(hits),
        )


@dataclass(slots=True)
class SupportingVerifier:
    calls: int = 0

    def verify(
        self, epoch_id: int, verifier_job: LogicalJobSpec
    ) -> VerificationResult:
        del epoch_id
        self.calls += 1
        pair = verifier_job.pair
        assert pair is not None
        observation = SemanticObservation(
            observation_id=digest("physical-observation", pair.claim_id),
            subject_kind=SubjectKind.CLAIM,
            subject_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
            task_type="claim-verification-v1",
            support_score=0.92,
            refute_score=0.03,
            neutral_score=0.05,
            producer=ModelStamp("scripted-physical-verifier", "v1", "prompt-v1"),
            input_hash=digest("physical-observation-input", pair.claim_id),
        )
        return VerificationResult(
            f"physical-verification-{pair.claim_id}",
            digest("physical-verification-result", pair.claim_id),
            observation,
        )


@dataclass(frozen=True, slots=True)
class ExecutionAccounting:
    execution_mode: str
    inline_grounding_oracle_calls: int
    working_claim_rows_written: int
    working_answer_rows_written: int
    evaluation_default_rows_written: int
    evaluation_override_rows_written: int
    active_chunk_rows_examined: int
    published_claim_versions_written: int
    published_answer_versions_written: int

    @classmethod
    def read(cls, connection: Connection[Any], epoch_id: int) -> ExecutionAccounting:
        row = connection.execute(
            """
            SELECT execution_mode, inline_grounding_oracle_calls,
                   working_claim_rows_written, working_answer_rows_written,
                   evaluation_default_rows_written,
                   evaluation_override_rows_written,
                   active_chunk_rows_examined,
                   published_claim_versions_written,
                   published_answer_versions_written
            FROM groundloop_m4_execution_accounting WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert row is not None
        return cls(str(row[0]), *(int(value) for value in row[1:]))
