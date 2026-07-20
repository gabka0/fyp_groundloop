"""Black-box fixture utilities for the M4 physical-incrementality gate.

This module deliberately knows only the public application composition and
the durable SQL accounting surface.  It does not import a full-recomputation
implementation, incremental-engine internals, or private pipeline helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import Connection

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
    LogicalJobSpec,
    PairKey,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.pipeline import InsertedDocument, bootstrap_m4_publication
from groundloop.postgres import record_epoch

REGISTRY_SIZE = 64
DECOY_CHUNK_COUNT = 128
TARGET_CLAIM_ID = "claim-000"
TARGET_ANSWER_ID = "answer-000"
CANDIDATE_POLICY_ID = "incrementality-candidate-v1"
REGISTRY_SNAPSHOT_ID = "incrementality-registry-v1"
DECISION_POLICY_ID = "incrementality-decision-v1"
IMPACT_EXECUTION_HASH = stable_m4_digest("incrementality-impact-execution")
FRONTIER_EXECUTION_HASH = stable_m4_digest("incrementality-frontier-execution")
VERIFIER_EXECUTION_HASH = stable_m4_digest("incrementality-verifier-execution")


def digest(value: str) -> str:
    return stable_m4_digest(value)


def claim_ids() -> tuple[str, ...]:
    return tuple(f"claim-{index:03d}" for index in range(REGISTRY_SIZE))


def answer_ids() -> tuple[str, ...]:
    return tuple(f"answer-{index:03d}" for index in range(REGISTRY_SIZE))


def candidate_policy() -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id=CANDIDATE_POLICY_ID,
        embedding_model_artifact_id="incrementality-embedder-v1",
        claim_role_template_hash=digest("incrementality-claim-role"),
        chunk_role_template_hash=digest("incrementality-chunk-role"),
        vector_method_version="exact-reverse-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=digest("incrementality-vector-build"),
        vector_search_config_hash=digest("incrementality-vector-search"),
        lexical_method_version="postgres-simple-v1",
        lexical_config_hash=digest("incrementality-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id=REGISTRY_SNAPSHOT_ID,
        claim_count=REGISTRY_SIZE,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=1,
        frontier_depth=1,
        verifier_execution_spec_hash=VERIFIER_EXECUTION_HASH,
        decision_policy_version=DECISION_POLICY_ID,
    )


def seed_large_unsupported_registry(connection: Connection[Any]) -> int:
    """Seed B0 with many untouched claims and many unrelated active chunks."""

    base_epoch, created = record_epoch(
        connection,
        event_id="incrementality-b0",
        payload_hash=digest("incrementality-b0"),
    )
    assert created
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET revision = 1,
            structural_status = 'committed',
            semantic_status = 'sealed',
            evaluation_state = 'complete',
            publication_mode = 'strict',
            sealed_at = now()
        WHERE epoch_id = %s
        """,
        (base_epoch,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES "
        "('decoy-document', 'fixture://decoys', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES "
        "('decoy-version', 'decoy-document', %s, %s, NULL)",
        (digest("decoy-version"), base_epoch),
    )
    with connection.transaction(), connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO groundloop_chunk_version VALUES (
                %s, 'decoy-version', %s, %s, %s,
                'fixed-char-v1', %s, NULL
            )
            """,
            (
                (
                    f"decoy-chunk-{index:03d}",
                    index,
                    f"Unrelated decoy passage {index}.",
                    normalized_text_hash(f"Unrelated decoy passage {index}."),
                    base_epoch,
                )
                for index in range(DECOY_CHUNK_COUNT)
            ),
        )
        cursor.executemany(
            "INSERT INTO groundloop_question VALUES (%s, %s, %s)",
            (
                (f"question-{index:03d}", f"Question {index}?", base_epoch)
                for index in range(REGISTRY_SIZE)
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
                    f"answer-{index:03d}",
                    f"question-{index:03d}",
                    f"Answer {index}.",
                    base_epoch,
                )
                for index in range(REGISTRY_SIZE)
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
                    f"claim-{index:03d}",
                    f"answer-{index:03d}",
                    f"Claim {index}.",
                )
                for index in range(REGISTRY_SIZE)
            ),
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_claim_state_materialized VALUES (
                %s, 0, 0, NULL, NULL, ARRAY[]::text[], ARRAY[]::text[],
                'unsupported', %s, 1
            )
            """,
            ((claim_id, base_epoch) for claim_id in claim_ids()),
        )
        cursor.executemany(
            """
            INSERT INTO groundloop_answer_state_materialized VALUES (
                %s, 1, 0, 1, 0, 0, 'unsupported', %s, 1
            )
            """,
            ((answer_id, base_epoch) for answer_id in answer_ids()),
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
        ) VALUES (
            'incrementality-embedder-v1', 'embedding', 'test', 'embedder',
            'v1', 'v1', 'MIT', %s
        )
        """,
        (digest("incrementality-embedder-config"),),
    )
    bootstrap_m4_publication(connection, sealed_epoch_id=base_epoch)
    return base_epoch


def inserted_support_document() -> InsertedDocument:
    text = "Claim zero is supported by this newly inserted passage."
    return InsertedDocument(
        DocumentVersion(
            "incrementality-version",
            "incrementality-document",
            digest("incrementality-document-content"),
        ),
        (
            ChunkVersion(
                "incrementality-chunk",
                "incrementality-version",
                0,
                text,
            ),
        ),
        source_uri="fixture://incrementality",
    )


@dataclass(slots=True)
class CompactPendingAdmission:
    """Assert the global discovery scope is compact at the worker boundary."""

    connection: Connection[Any]
    checked_pending: bool = False

    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult:
        default_count = self.connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m4_evaluation_epoch_counter
            WHERE epoch_id = %s AND default_evaluation_state = 'pending'
              AND open_discovery_scope_count = 1
            """,
            (epoch_id,),
        ).fetchone()
        override_count = self.connection.execute(
            """
            SELECT count(*) FROM groundloop_m4_evaluation_override_counter
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert default_count == (1,)
        assert override_count == (0,)
        self.checked_pending = True

        chunk_id = root_job.target_chunk_version_id
        assert chunk_id == "incrementality-chunk"
        pair = PairKey(TARGET_CLAIM_ID, chunk_id)
        admitted = AdmittedPair(
            epoch_id,
            pair,
            root_job.candidate_policy_id,
            1,
            (AdmissionChannel.VECTOR,),
            False,
        )
        hit = ChannelHit(
            epoch_id,
            pair,
            root_job.candidate_policy_id,
            AdmissionChannel.VECTOR,
            1,
            0.9,
            digest(f"incrementality-hit:{root_job.job_id}"),
        )
        return DiscoveryResult(
            root_job.job_id,
            f"incrementality-discovery:{root_job.job_id}",
            digest(f"incrementality-discovery:{root_job.job_id}"),
            (admitted,),
            channel_hits=(hit,),
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
        assert pair == PairKey(TARGET_CLAIM_ID, "incrementality-chunk")
        observation = SemanticObservation(
            observation_id=digest("incrementality-support-observation"),
            subject_kind=SubjectKind.CLAIM,
            subject_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
            task_type="claim-verification-v1",
            support_score=0.9,
            refute_score=0.05,
            neutral_score=0.05,
            producer=ModelStamp("scripted-verifier", "v1", "prompt-v1"),
            input_hash=digest("incrementality-support-input"),
        )
        return VerificationResult(
            "incrementality-verification-result",
            digest(repr(observation)),
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
            FROM groundloop_m4_execution_accounting
            WHERE epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert row is not None
        return cls(str(row[0]), *(int(value) for value in row[1:]))


def published_interval(
    connection: Connection[Any], table: str, key_column: str, key: str
) -> tuple[tuple[int, int | None], ...]:
    if (table, key_column) not in {
        ("groundloop_published_claim_state", "claim_id"),
        ("groundloop_published_answer_state", "answer_version_id"),
    }:
        raise ValueError("published interval helper received an unsafe table")
    rows = connection.execute(
        f"SELECT valid_from_epoch, valid_to_epoch FROM {table} "
        f"WHERE {key_column} = %s ORDER BY valid_from_epoch",
        (key,),
    ).fetchall()
    return tuple(
        (int(row[0]), None if row[1] is None else int(row[1])) for row in rows
    )
