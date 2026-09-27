from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import psycopg
from psycopg import Connection, sql

from groundloop.ai.application import M3Application, M3ApplicationConfig
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import DeterministicClaimExtractor
from groundloop.ai.contracts import (
    AtomicClaim,
    ChunkDraft,
    EmbeddingRecord,
    PipelineRunManifest,
    QueryKind,
    VerificationResult,
)
from groundloop.ai.embeddings import DeterministicFakeEmbedder
from groundloop.ai.embeddings.common import EmbeddedQuery
from groundloop.ai.generation import DeterministicAnswerGenerator
from groundloop.ai.persistence import PostgresArtifactStore
from groundloop.ai.verification import DeterministicFakeVerifier
from groundloop.domain import (
    AnswerStatus,
    ChunkVersion,
    ClaimStatus,
    DecisionPolicy,
    DocumentVersion,
    normalized_text_hash,
)
from groundloop.m4.application import DynamicEventPlan, EventRunResult, EventRunState
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobKind,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.m3_activation import (
    M3M4ActivationReceipt,
    M3M4RuntimeComposition,
    PublishedM3ActivationSource,
    activate_published_m3_run,
    compose_activated_m3_runtime,
    inspect_m3_m4_activation,
)
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import M4BgeRoleAdapter
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter
from groundloop.m4.pipeline import InsertedDocument, StructuralPayload
from groundloop.m4.runtime import RuntimeEpochState

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_HASH = hashlib.sha256(b"m3-m4-runtime-composition-test-v1").hexdigest()
CALIBRATION_ARTIFACT_SHA256 = "f" * 64

BASE_TEXT = "GroundLoop maintains claim grounding relative to versioned evidence."
AUXILIARY_TEXT = "Auxiliary replacement fallback document 1."
SUPPORT_TEXT = "Deterministic evidence candidate 7."
REFUTE_TEXT = "Deterministic evidence candidate 5."


@dataclass(frozen=True, slots=True)
class _PublishedActivation:
    manifest: PipelineRunManifest
    source: PublishedM3ActivationSource
    activation: M3M4ActivationReceipt
    embedding_spec: EmbeddingAdapterSpec
    verifier_spec: VerificationAdapterSpec


@dataclass(frozen=True, slots=True)
class _HistoryEvent:
    plan: DynamicEventPlan
    payload: StructuralPayload
    result: EventRunResult
    claim_status: ClaimStatus
    answer_status: AnswerStatus


class _FailOnCallEmbedder(DeterministicFakeEmbedder):
    def embed(self, chunks: tuple[ChunkDraft, ...]) -> tuple[EmbeddingRecord, ...]:
        del chunks
        raise AssertionError("exact replay called the passage embedder")

    def embed_query(self, text: str, query_kind: QueryKind) -> EmbeddedQuery:
        del text, query_kind
        raise AssertionError("exact replay called the query embedder")


class _FailOnCallVerifier(DeterministicFakeVerifier):
    def verify_batch(
        self,
        pairs: Sequence[tuple[AtomicClaim, ChunkDraft]],
        *,
        batch_size: int = 8,
    ) -> tuple[VerificationResult, ...]:
        del pairs, batch_size
        raise AssertionError("exact replay called the verifier backend")


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    assert value is not None
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _schema_name(connection: Connection[Any]) -> str:
    row = connection.execute("SELECT current_schema()").fetchone()
    assert row is not None and row[0] is not None
    return str(row[0])


def _connect(schema_name: str) -> Connection[Any]:
    connection = psycopg.connect(_database_url(), autocommit=True)
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    return connection


def _embedding_adapter(*, fail_on_call: bool = False) -> M4BgeRoleAdapter:
    backend = _FailOnCallEmbedder() if fail_on_call else DeterministicFakeEmbedder()
    return M4BgeRoleAdapter(
        backend,
        EmbeddingAdapterSpec(
            model_artifact=backend.model_artifact,
            dimension=backend.dimension,
        ),
    )


def _verifier_spec(
    backend: DeterministicFakeVerifier, policy: DecisionPolicy
) -> VerificationAdapterSpec:
    return VerificationAdapterSpec(
        model_artifact=backend.model_artifact,
        prompt_artifact=backend.prompt_artifact,
        calibration_version=backend.calibration_version,
        calibration_artifact_sha256=CALIBRATION_ARTIFACT_SHA256,
        temperature=backend.temperature,
        max_length=backend.max_length,
        decision_policy=policy,
    )


def _verifier_adapter(
    spec: VerificationAdapterSpec, *, fail_on_call: bool = False
) -> M4CalibratedVerifierAdapter:
    backend = _FailOnCallVerifier() if fail_on_call else DeterministicFakeVerifier()
    return M4CalibratedVerifierAdapter(backend, spec, batch_size=8)


def _publish_and_activate(
    connection: Connection[Any], tmp_path: Path
) -> _PublishedActivation:
    corpus = tmp_path / "runtime-composition-corpus"
    corpus.mkdir()
    (corpus / "groundloop.txt").write_text(BASE_TEXT + "\n", encoding="utf-8")
    (corpus / "auxiliary.txt").write_text(AUXILIARY_TEXT + "\n", encoding="utf-8")
    policy = DecisionPolicy("m3-policy-v1", 0.8, 0.8)
    m3_embedder = DeterministicFakeEmbedder()
    m3_verifier = DeterministicFakeVerifier()
    application = M3Application(
        store=PostgresArtifactStore(connection),
        chunker=FixedCharChunker(),
        embedder=m3_embedder,
        generator=DeterministicAnswerGenerator(),
        extractor=DeterministicClaimExtractor(),
        verifier=m3_verifier,
        config=M3ApplicationConfig(
            config_hash=CONFIG_HASH,
            question_top_k=1,
            claim_top_k=1,
            policy=policy,
        ),
    )
    manifest = application.register(corpus, "What does GroundLoop maintain?")
    inspection = inspect_m3_m4_activation(connection, run_id=manifest.run_id)
    assert inspection.replay_receipt is None
    verifier_spec = _verifier_spec(m3_verifier, policy)
    embedding_adapter = _embedding_adapter()
    activation = activate_published_m3_run(
        connection,
        run_id=manifest.run_id,
        embeddings=embedding_adapter,
        verifier_spec=verifier_spec,
        repo_root=REPO_ROOT,
    )
    return _PublishedActivation(
        manifest=manifest,
        source=inspection.source,
        activation=activation,
        embedding_spec=embedding_adapter.spec,
        verifier_spec=verifier_spec,
    )


def _compose(
    connection: Connection[Any],
    published: _PublishedActivation,
    payloads: Mapping[str, StructuralPayload],
    *,
    fail_on_call: bool = False,
) -> M3M4RuntimeComposition:
    embedding_adapter = _embedding_adapter(fail_on_call=fail_on_call)
    assert embedding_adapter.spec == published.embedding_spec
    return compose_activated_m3_runtime(
        connection,
        run_id=published.manifest.run_id,
        embedding_adapter=embedding_adapter,
        verifier_adapter=_verifier_adapter(
            published.verifier_spec,
            fail_on_call=fail_on_call,
        ),
        repo_root=REPO_ROOT,
        structural_payloads=payloads,
    )


def _inserted_document(
    *,
    chunker: FixedCharChunker,
    document_id: str,
    version_namespace: str,
    text: str,
) -> tuple[InsertedDocument, str]:
    content_hash = normalized_text_hash(text)
    version_id = stable_m4_digest(version_namespace, document_id, content_hash)
    drafts = chunker.chunk(version_id, text)
    assert len(drafts) == 1
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
            source_uri=f"test://{document_id}",
            authority_class="m3-m4-runtime-test",
            chunker_version="fixed-char-v1",
            chunker_artifact_id=chunker.artifact_id,
            chunker_input_hash=content_hash,
        ),
        draft.chunk_version_id,
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
            "m3-m4-runtime-composition-event-v1",
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


def _open_event_without_running_workers(
    composition: M3M4RuntimeComposition,
    plan: DynamicEventPlan,
) -> int:
    """Persist the public production declaration, then simulate process loss."""
    withdrawal = composition.ports.plan_exact_withdrawal(plan)
    roots = composition.application._root_jobs(  # noqa: SLF001 - recovery fixture
        plan, withdrawal.fallback_claim_ids
    )
    scopes = tuple(
        DiscoveryScope(
            root_job_id=root.job_id,
            registry_snapshot_id=plan.claim_registry_snapshot_id,
            registered_claim_ids=(),
        )
        for root in roots
        if root.kind is JobKind.IMPACT_DISCOVERY
    )
    opened = composition.ports.open_event(plan, withdrawal, roots, scopes)
    assert not opened.replayed
    assert not opened.already_sealed
    assert not opened.already_failed
    return opened.epoch_id


def _table_projection(connection: Connection[Any]) -> dict[str, tuple[str, ...]]:
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


def _m3_provenance_projection(
    connection: Connection[Any], published: _PublishedActivation
) -> dict[str, tuple[str, ...]]:
    run_id = published.manifest.run_id
    answer_id = published.source.answer_version_id
    epoch_id = published.source.epoch_id
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
            str(row[0]) for row in connection.execute(statement, parameters).fetchall()
        )
        for name, (statement, parameters) in statements.items()
    }


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
    assert claim_row is not None and answer_row is not None
    return cast(int, claim_row[0]), cast(int, answer_row[0])


def _current_statuses(
    connection: Connection[Any], published: _PublishedActivation
) -> tuple[ClaimStatus, AnswerStatus]:
    claim_row = connection.execute(
        """
        SELECT status::text FROM groundloop_published_claim_state
        WHERE claim_id = %s AND valid_to_epoch IS NULL
        """,
        (published.source.claim_ids[0],),
    ).fetchone()
    answer_row = connection.execute(
        """
        SELECT status::text FROM groundloop_published_answer_state
        WHERE answer_version_id = %s AND valid_to_epoch IS NULL
        """,
        (published.source.answer_version_id,),
    ).fetchone()
    assert claim_row is not None and answer_row is not None
    return ClaimStatus(str(claim_row[0])), AnswerStatus(str(answer_row[0]))


def _assert_exact_sealed_event(
    connection: Connection[Any],
    composition: M3M4RuntimeComposition,
    published: _PublishedActivation,
    result: EventRunResult,
    *,
    expected_claim_status: ClaimStatus,
    expected_answer_status: AnswerStatus,
) -> None:
    assert result.state is EventRunState.SEALED
    assert result.publication_id is not None
    composition.ports.audit_grounding_exactness(result.epoch_id)
    assert _sql_oracle_mismatches(connection, result.epoch_id) == (0, 0)
    header = composition.ports.runtime_store.read_epoch_header_point(result.epoch_id)
    assert header.state is RuntimeEpochState.SEALED
    assert header.open_job_count == 0
    assert header.open_scope_count == 0
    epoch_row = connection.execute(
        """
        SELECT structural_status, semantic_status, evaluation_state,
               publication_mode, sealed_at IS NOT NULL
        FROM groundloop_epoch WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone()
    assert epoch_row == ("committed", "sealed", "complete", "strict", True)
    assert _current_statuses(connection, published) == (
        expected_claim_status,
        expected_answer_status,
    )
    verifier_rows = connection.execute(
        """
        SELECT count(*), count(*) FILTER (
            WHERE execution.calibration_artifact_sha256 = %s
        )
        FROM groundloop_m4_verification_execution AS execution
        JOIN groundloop_semantic_job AS job USING (job_id)
        WHERE job.epoch_id = %s
        """,
        (CALIBRATION_ARTIFACT_SHA256, result.epoch_id),
    ).fetchone()
    assert verifier_rows == (result.verifier_call_count,) * 2


def test_composed_runtime_seals_three_event_history_and_exactly_replays(
    m3_m4_activation_connection: Connection[Any],
    tmp_path: Path,
) -> None:
    published = _publish_and_activate(m3_m4_activation_connection, tmp_path)
    assert len(published.source.claim_ids) == 1
    assert len(published.source.chunk_ids) == 2
    assert _current_statuses(m3_m4_activation_connection, published) == (
        ClaimStatus.UNSUPPORTED,
        AnswerStatus.UNSUPPORTED,
    )
    schema_name = _schema_name(m3_m4_activation_connection)
    m3_provenance = _m3_provenance_projection(m3_m4_activation_connection, published)

    activation_projection = _table_projection(m3_m4_activation_connection)
    with _connect(schema_name) as replay_connection:
        activation_replay = activate_published_m3_run(
            replay_connection,
            run_id=published.manifest.run_id,
            embeddings=_embedding_adapter(fail_on_call=True),
            verifier_spec=published.verifier_spec,
            repo_root=REPO_ROOT,
        )
        assert activation_replay.replayed
        assert _table_projection(replay_connection) == activation_projection

    chunker = FixedCharChunker()
    inserted, inserted_chunk_id = _inserted_document(
        chunker=chunker,
        document_id="m3-m4-runtime-support-document",
        version_namespace="m3-m4-runtime-support-version-v1",
        text=SUPPORT_TEXT,
    )
    base_chunk = next(
        chunk for chunk in published.source.chunks if chunk.text == BASE_TEXT
    )
    assert any(chunk.text == AUXILIARY_TEXT for chunk in published.source.chunks)
    replacement, replacement_chunk_id = _inserted_document(
        chunker=chunker,
        document_id=inserted.version.document_id,
        version_namespace="m3-m4-runtime-refuting-version-v1",
        text=REFUTE_TEXT,
    )
    payloads = {
        "m3-m4-runtime-insert": StructuralPayload(inserted=inserted),
        "m3-m4-runtime-delete": StructuralPayload(
            deactivated_document_version_id=base_chunk.document_version_id
        ),
        "m3-m4-runtime-replace": StructuralPayload(
            inserted=replacement,
            deactivated_document_version_id=inserted.version.document_version_id,
        ),
    }

    history: list[_HistoryEvent] = []
    previous_epoch = published.activation.base_epoch_id
    event_specs = (
        (
            "m3-m4-runtime-insert",
            UpdateKind.INSERT,
            (inserted_chunk_id,),
            (),
            ClaimStatus.SUPPORTED,
            AnswerStatus.VALID,
            1,
            1,
        ),
        (
            "m3-m4-runtime-delete",
            UpdateKind.DELETE,
            (),
            (base_chunk.chunk_version_id,),
            ClaimStatus.SUPPORTED,
            AnswerStatus.VALID,
            0,
            0,
        ),
        (
            "m3-m4-runtime-replace",
            UpdateKind.REPLACE,
            (replacement_chunk_id,),
            (inserted_chunk_id,),
            ClaimStatus.REFUTED,
            AnswerStatus.CONTRADICTED,
            1,
            1,
        ),
    )
    for (
        event_id,
        kind,
        inserted_chunk_ids,
        deactivated_chunk_ids,
        expected_claim,
        expected_answer,
        expected_embedding_requests,
        expected_verifier_calls,
    ) in event_specs:
        with _connect(schema_name) as event_connection:
            composition = _compose(event_connection, published, payloads)
            plan = _event_plan(
                event_id=event_id,
                kind=kind,
                previous_epoch_id=previous_epoch,
                composition=composition,
                payload=payloads[event_id],
                inserted_chunk_ids=inserted_chunk_ids,
                deactivated_chunk_ids=deactivated_chunk_ids,
            )
            result = composition.application.run_event(plan)
            _assert_exact_sealed_event(
                event_connection,
                composition,
                published,
                result,
                expected_claim_status=expected_claim,
                expected_answer_status=expected_answer,
            )
            assert result.discovery_call_count >= 1
            assert result.verifier_call_count == expected_verifier_calls
            assert composition.embeddings.request_count == (expected_embedding_requests)
            assert composition.verifier.request_count == result.verifier_call_count
            assert composition.verifier.backend_pair_calls == result.verifier_call_count
            history.append(
                _HistoryEvent(
                    plan,
                    payloads[event_id],
                    result,
                    expected_claim,
                    expected_answer,
                )
            )
            previous_epoch = result.epoch_id

    assert history[-2].claim_status is ClaimStatus.SUPPORTED
    assert history[-1].claim_status is ClaimStatus.REFUTED
    assert history[-2].answer_status is AnswerStatus.VALID
    assert history[-1].answer_status is AnswerStatus.CONTRADICTED
    with _connect(schema_name) as provenance_connection:
        assert _m3_provenance_projection(provenance_connection, published) == (
            m3_provenance
        )

    for event in history:
        with _connect(schema_name) as replay_connection:
            before_replay = _table_projection(replay_connection)
            composition = _compose(
                replay_connection,
                published,
                {event.plan.update.event_id: event.payload},
                fail_on_call=True,
            )
            event_replay = composition.application.run_event(event.plan)
            assert event_replay.state is EventRunState.REPLAYED
            assert event_replay.epoch_id == event.result.epoch_id
            assert event_replay.publication_id == event.result.publication_id
            assert event_replay.discovery_call_count == 0
            assert event_replay.verifier_call_count == 0
            assert composition.embeddings.request_count == 0
            assert composition.verifier.request_count == 0
            assert composition.verifier.backend_pair_calls == 0
            assert _table_projection(replay_connection) == before_replay

    with _connect(schema_name) as final_connection:
        assert _m3_provenance_projection(final_connection, published) == m3_provenance
        assert _current_statuses(final_connection, published) == (
            ClaimStatus.REFUTED,
            AnswerStatus.CONTRADICTED,
        )


def test_public_composition_reconnects_and_finishes_one_open_event(
    m3_m4_activation_connection: Connection[Any],
    tmp_path: Path,
) -> None:
    published = _publish_and_activate(m3_m4_activation_connection, tmp_path)
    schema_name = _schema_name(m3_m4_activation_connection)
    inserted, inserted_chunk_id = _inserted_document(
        chunker=FixedCharChunker(),
        document_id="m3-m4-open-recovery-document",
        version_namespace="m3-m4-open-recovery-version-v1",
        text=SUPPORT_TEXT,
    )
    event_id = "m3-m4-open-recovery"
    payload = StructuralPayload(inserted=inserted)
    initial = _compose(
        m3_m4_activation_connection,
        published,
        {event_id: payload},
    )
    plan = _event_plan(
        event_id=event_id,
        kind=UpdateKind.INSERT,
        previous_epoch_id=published.activation.base_epoch_id,
        composition=initial,
        payload=payload,
        inserted_chunk_ids=(inserted_chunk_id,),
    )
    open_epoch_id = _open_event_without_running_workers(initial, plan)
    header = initial.ports.runtime_store.read_epoch_header_point(open_epoch_id)
    assert header.state is RuntimeEpochState.SEMANTIC_PENDING

    with _connect(schema_name) as reconnect:
        recovered = _compose(reconnect, published, {event_id: payload})
        result = recovered.application.run_event(plan)
        _assert_exact_sealed_event(
            reconnect,
            recovered,
            published,
            result,
            expected_claim_status=ClaimStatus.SUPPORTED,
            expected_answer_status=AnswerStatus.VALID,
        )
        assert result.epoch_id == open_epoch_id


def test_public_composition_accepts_failed_history_and_seals_next_event(
    m3_m4_activation_connection: Connection[Any],
    tmp_path: Path,
) -> None:
    published = _publish_and_activate(m3_m4_activation_connection, tmp_path)
    schema_name = _schema_name(m3_m4_activation_connection)
    chunker = FixedCharChunker()
    failed_document, failed_chunk_id = _inserted_document(
        chunker=chunker,
        document_id="m3-m4-failed-history-document",
        version_namespace="m3-m4-failed-history-version-v1",
        text=AUXILIARY_TEXT,
    )
    failed_event_id = "m3-m4-terminal-failure"
    failed_payload = StructuralPayload(inserted=failed_document)
    initial = _compose(
        m3_m4_activation_connection,
        published,
        {failed_event_id: failed_payload},
    )
    failed_plan = _event_plan(
        event_id=failed_event_id,
        kind=UpdateKind.INSERT,
        previous_epoch_id=published.activation.base_epoch_id,
        composition=initial,
        payload=failed_payload,
        inserted_chunk_ids=(failed_chunk_id,),
    )
    failed_epoch_id = _open_event_without_running_workers(initial, failed_plan)
    initial.ports.fail_epoch(failed_epoch_id, "simulated terminal worker failure")
    failed_header = initial.ports.runtime_store.read_epoch_header_point(failed_epoch_id)
    assert failed_header.state is RuntimeEpochState.FAILED

    next_document, next_chunk_id = _inserted_document(
        chunker=chunker,
        document_id="m3-m4-after-failure-document",
        version_namespace="m3-m4-after-failure-version-v1",
        text=SUPPORT_TEXT,
    )
    next_event_id = "m3-m4-after-terminal-failure"
    next_payload = StructuralPayload(inserted=next_document)
    payloads = {
        failed_event_id: failed_payload,
        next_event_id: next_payload,
    }
    with _connect(schema_name) as reconnect:
        recovered = _compose(reconnect, published, payloads)
        failed_replay = recovered.application.run_event(failed_plan)
        assert failed_replay.state is EventRunState.FAILED
        assert failed_replay.epoch_id == failed_epoch_id
        assert failed_replay.discovery_call_count == 0
        assert failed_replay.verifier_call_count == 0

        next_plan = _event_plan(
            event_id=next_event_id,
            kind=UpdateKind.INSERT,
            previous_epoch_id=published.activation.base_epoch_id,
            composition=recovered,
            payload=next_payload,
            inserted_chunk_ids=(next_chunk_id,),
        )
        result = recovered.application.run_event(next_plan)
        _assert_exact_sealed_event(
            reconnect,
            recovered,
            published,
            result,
            expected_claim_status=ClaimStatus.SUPPORTED,
            expected_answer_status=AnswerStatus.VALID,
        )
