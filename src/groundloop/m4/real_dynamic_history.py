"""Bounded real-model M4.8 insert/delete/replace history.

This is an opt-in integration harness, not an AI-quality benchmark.  It runs
the production measured M4 coordinator against a disposable PostgreSQL schema
and pinned local M3 model artifacts.  Full recomputation audits run only after
each measured event has sealed.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection, sql

from groundloop.ai.chunking import FixedCharChunker
from groundloop.domain import ChunkVersion, DocumentVersion, normalized_text_hash
from groundloop.errors import ValidationError
from groundloop.m4.admission.fresh import PostgresExactFreshFrontierRetriever
from groundloop.m4.admission.lexical import load_frozen_lexical_v1
from groundloop.m4.admission.postgres_common import PostgresAdmissionServerIdentity
from groundloop.m4.admission.postgres_vector import (
    ExactPgvectorConfig,
    PostgresExactReverseVectorIndex,
)
from groundloop.m4.admission.service import PostgresHybridAdmissionPort
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DynamicEventPlan,
    EventRunState,
    M4Application,
)
from groundloop.m4.artifacts import PostgresM4ArtifactRegistry
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    CorpusUpdateIdentity,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m4.evaluation_overlay import EvaluationLifecycle
from groundloop.m4.execution import M4ExecutionIdentity
from groundloop.m4.models.config import (
    PinnedM3AdapterBundle,
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
    inspect_local_artifacts,
)
from groundloop.m4.models.contracts import decision_policy_hash
from groundloop.m4.models.ports import (
    M4AdmissionEmbeddingService,
    M4VerificationApplicationPort,
)
from groundloop.m4.pipeline import (
    InsertedDocument,
    M4ExecutionMode,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    PostgresM4VerificationExecutionWriter,
    PostgresPairInputResolver,
    StructuralPayload,
)
from groundloop.m4.runtime import RuntimeEpochState
from groundloop.m4.smoke import (
    _ANSWER_ID,
    _CLAIM_ID,
    _CLAIM_TEXT,
    SmokeUnavailableError,
    _artifact_counts,
    _build_lexical_policy,
    _build_manifest,
    _create_schema,
    _drop_schema,
    _PersistingEmbeddingService,
    _prepare_registry,
    _psycopg_url,
    _register_static_artifacts,
    _row_counts,
    _seed_b0,
)

_SCHEMA_VERSION = "groundloop-m4-real-dynamic-history-v1"
_REGISTRY_NAMESPACE = "m4-real-dynamic-history-registry-v1"
_AUXILIARY_DOCUMENT_ID = "m4-real-history-replace-target-document"
_AUXILIARY_VERSION_ID = "m4-real-history-replace-target-v1"
_AUXILIARY_TEXT = "This maintenance note describes a disposable integration fixture."
_INSERT_TEXT = (
    "GroundLoop maintains claim grounding relative to versioned evidence and "
    "stored semantic observations; it does not establish objective truth."
)
_REPLACEMENT_TEXT = (
    "GroundLoop does not maintain claim grounding relative to stored semantic "
    "observations."
)


@dataclass(frozen=True, slots=True)
class M4RealDynamicHistoryConfig:
    database_url: str
    repo_root: Path
    artifact_root: Path
    model_config_path: Path | None = None
    lexical_config_path: Path | None = None
    schema_prefix: str = "groundloop_m4_real_dynamic"
    keep_schema: bool = False

    def __post_init__(self) -> None:
        if not self.database_url.strip():
            raise SmokeUnavailableError("PostgreSQL database URL is absent")
        if not self.schema_prefix or any(
            not (character.isalnum() or character == "_")
            for character in self.schema_prefix
        ):
            raise ValidationError(
                "history schema prefix must contain only letters, digits, underscores"
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
class M4RealDynamicHistoryResult:
    manifest: Mapping[str, object]

    def to_json(self) -> str:
        return json.dumps(self.manifest, indent=2, sort_keys=True) + "\n"


@dataclass(frozen=True, slots=True)
class _Composition:
    application: M4Application
    ports: PostgresM4ApplicationPorts
    embeddings: _PersistingEmbeddingService
    verifier: M4VerificationApplicationPort
    identity: M4ExecutionIdentity


@dataclass(frozen=True, slots=True)
class _HistoryEvent:
    plan: DynamicEventPlan
    payload: StructuralPayload
    expected_discovery_calls: int
    expected_embedding_requests: int
    expected_verifier_calls: int


def _set_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )


def _connect(database_url: str, schema_name: str) -> Connection[Any]:
    connection = psycopg.connect(_psycopg_url(database_url), autocommit=True)
    _set_schema(connection, schema_name)
    return connection


def _seed_auxiliary_replacement_target(
    connection: Connection[Any], *, epoch_id: int, chunker: FixedCharChunker
) -> str:
    content_hash = normalized_text_hash(_AUXILIARY_TEXT)
    draft = chunker.chunk(_AUXILIARY_VERSION_ID, _AUXILIARY_TEXT)
    if len(draft) != 1:
        raise ValidationError("bounded replacement target must have one chunk")
    connection.execute(
        """
        INSERT INTO groundloop_document(document_id, source_uri, authority_class)
        VALUES (%s, 'history://replacement-target', 'm4-history')
        """,
        (_AUXILIARY_DOCUMENT_ID,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_document_version(
            document_version_id, document_id, content_hash,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, %s, %s, NULL)
        """,
        (_AUXILIARY_VERSION_ID, _AUXILIARY_DOCUMENT_ID, content_hash, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version(
            chunk_version_id, document_version_id, chunk_index, text,
            text_hash, chunker_version, valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, 0, %s, %s, 'fixed-char-v1', %s, NULL)
        """,
        (
            draft[0].chunk_version_id,
            _AUXILIARY_VERSION_ID,
            draft[0].text,
            draft[0].text_hash,
            epoch_id,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_chunk_provenance(
            chunk_version_id, chunker_artifact_id, input_hash
        ) VALUES (%s, %s, %s)
        """,
        (draft[0].chunk_version_id, chunker.artifact_id, content_hash),
    )
    return draft[0].chunk_version_id


def _inserted_document(
    *,
    chunker: FixedCharChunker,
    document_id: str,
    version_namespace: str,
    text: str,
    source_uri: str,
) -> tuple[InsertedDocument, str]:
    content_hash = normalized_text_hash(text)
    version_id = stable_m4_digest(version_namespace, content_hash)
    drafts = chunker.chunk(version_id, text)
    if len(drafts) != 1:
        raise ValidationError("bounded history documents must have one chunk")
    draft = drafts[0]
    inserted = InsertedDocument(
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
        source_uri=source_uri,
        authority_class="m4-history",
        chunker_version="fixed-char-v1",
        chunker_artifact_id=chunker.artifact_id,
        chunker_input_hash=content_hash,
    )
    return inserted, draft.chunk_version_id


def _event_plan(
    *,
    event_id: str,
    kind: UpdateKind,
    previous_epoch_id: int,
    manifest: CandidatePolicyManifest,
    payload: StructuralPayload,
    inserted_chunk_ids: tuple[str, ...] = (),
    deactivated_chunk_ids: tuple[str, ...] = (),
) -> DynamicEventPlan:
    update = CorpusUpdateIdentity(
        event_id=event_id,
        payload_hash=stable_m4_digest(
            "m4-real-dynamic-event-payload-v1",
            event_id,
            kind.value,
            json.dumps(payload.manifest, sort_keys=True, separators=(",", ":")),
            manifest.policy_hash,
            str(previous_epoch_id),
        ),
        update_kind=kind,
        previous_published_epoch_id=previous_epoch_id,
        candidate_policy_id=manifest.policy_id,
    )
    return DynamicEventPlan(
        update=update,
        inserted_chunk_version_ids=inserted_chunk_ids,
        deactivated_chunk_version_ids=deactivated_chunk_ids,
        registered_claim_ids=(),
        claim_registry_snapshot_id=manifest.claim_registry_snapshot_id,
    )


def _compose(
    connection: Connection[Any],
    *,
    bundle: PinnedM3AdapterBundle,
    registry: PostgresM4ArtifactRegistry,
    manifest: CandidatePolicyManifest,
    vector_config: ExactPgvectorConfig,
    server: PostgresAdmissionServerIdentity,
    lexical_config_path: Path,
    payloads: Mapping[str, StructuralPayload],
) -> _Composition:
    embedding_delegate = M4AdmissionEmbeddingService(bundle.embeddings)
    embeddings = _PersistingEmbeddingService(embedding_delegate, registry)
    lexical, analyzer, lexical_backend = _build_lexical_policy(
        connection,
        server=server,
        manifest=manifest,
        lexical_config_path=lexical_config_path,
    )
    vector = PostgresExactReverseVectorIndex(
        connection=connection,
        server=server,
        manifest=manifest,
        config=vector_config,
    )
    vector.validate_registry()
    admission = PostgresHybridAdmissionPort(
        connection=connection,
        manifest=manifest,
        embeddings=embeddings,
        vector_index=vector,
        lexical_policy=lexical,
        fresh_frontier_retriever=PostgresExactFreshFrontierRetriever(connection),
    )
    verifier = M4VerificationApplicationPort(
        PostgresPairInputResolver(connection), bundle.verifier
    )
    writer = PostgresM4VerificationExecutionWriter(verifier)
    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads=payloads,
        verification_provenance_writer=writer,
        execution_mode=M4ExecutionMode.MEASURED,
    )
    identity = M4ExecutionIdentity.build(
        manifest=manifest,
        embedding_adapter_spec_hash=bundle.embeddings.spec.spec_hash,
        vector_adapter_artifact_id=vector.artifact_id,
        lexical_analyzer_artifact_id=analyzer.artifact_id,
        lexical_backend_artifact_id=lexical_backend.artifact_id,
        frontier_retriever_artifact_id="m4-real-history-exact-frontier-v1",
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
    return _Composition(application, ports, embeddings, verifier, identity)


def _table_snapshot(connection: Connection[Any]) -> dict[str, dict[str, object]]:
    tables = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = current_schema() ORDER BY tablename
            """
        ).fetchall()
    )
    snapshot: dict[str, dict[str, object]] = {}
    for table in tables:
        rows = tuple(
            str(row[0])
            for row in connection.execute(
                sql.SQL(
                    "SELECT to_jsonb(item)::text FROM {} AS item "
                    "ORDER BY to_jsonb(item)::text"
                ).format(sql.Identifier(table))
            ).fetchall()
        )
        snapshot[table] = {
            "row_count": len(rows),
            "content_sha256": stable_m4_digest(
                "m4-real-history-table-v1", table, *rows
            ),
        }
    return snapshot


def _snapshot_digest(snapshot: Mapping[str, Mapping[str, object]]) -> str:
    return stable_m4_digest(
        "m4-real-history-schema-snapshot-v1",
        *(
            f"{table}:{values['row_count']}:{values['content_sha256']}"
            for table, values in sorted(snapshot.items())
        ),
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
        (epoch_id, epoch_id, epoch_id, epoch_id),
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
        (epoch_id, epoch_id, epoch_id, epoch_id),
    ).fetchone()
    if claim_row is None or answer_row is None:
        raise ValidationError("SQL oracle comparison returned no count")
    return int(claim_row[0]), int(answer_row[0])


def _assert_exact_surfaces(
    connection: Connection[Any],
    *,
    composition: _Composition,
    epoch_id: int,
) -> dict[str, object]:
    # Explicitly outside M4Application.run_event(): this audit is not charged
    # to the measured event kernel.
    composition.ports.audit_grounding_exactness(epoch_id)
    claim_mismatches, answer_mismatches = _sql_oracle_mismatches(connection, epoch_id)
    if claim_mismatches or answer_mismatches:
        raise ValidationError("measured state differs from the independent SQL oracle")
    header = composition.ports.runtime_store.read_epoch_header_point(epoch_id)
    if (
        header.state is not RuntimeEpochState.SEALED
        or header.open_job_count != 0
        or header.open_scope_count != 0
    ):
        raise ValidationError("sealed measured epoch retains open coordination work")
    default = composition.ports.evaluation_store.read_default(epoch_id)
    override_count = connection.execute(
        """
        SELECT count(*) FROM groundloop_m4_evaluation_override_counter
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if (
        default.lifecycle is not EvaluationLifecycle.SEALED
        or default.default_state.value != "complete"
        or default.confirmed_as_of_epoch != epoch_id
        or default.open_discovery_scope_count != 0
        or default.revision != header.revision
    ):
        raise ValidationError("compact evaluation default is not sealed and complete")
    if override_count != (0,):
        raise ValidationError("sealed compact evaluation retains overrides")
    claim = connection.execute(
        """
        SELECT support_count, refute_count, best_support_score,
               best_refute_score, supporting_observation_ids,
               refuting_observation_ids, status::text
        FROM groundloop_published_claim_state
        WHERE claim_id = %s AND valid_to_epoch IS NULL
        """,
        (_CLAIM_ID,),
    ).fetchone()
    answer = connection.execute(
        """
        SELECT required_claim_count, supported_count, unsupported_count,
               refuted_count, conflicted_count, status::text
        FROM groundloop_published_answer_state
        WHERE answer_version_id = %s AND valid_to_epoch IS NULL
        """,
        (_ANSWER_ID,),
    ).fetchone()
    if claim is None or answer is None:
        raise ValidationError("strict published grounding rows are absent")
    return {
        "python_full_recomputation_equal": True,
        "sql_full_recomputation_equal": True,
        "claim_mismatch_count": claim_mismatches,
        "answer_mismatch_count": answer_mismatches,
        "epoch_revision": header.revision,
        "open_job_count": header.open_job_count,
        "open_scope_count": header.open_scope_count,
        "claim_state": {
            "support_count": int(claim[0]),
            "refute_count": int(claim[1]),
            "best_support_score": (None if claim[2] is None else float(claim[2])),
            "best_refute_score": None if claim[3] is None else float(claim[3]),
            "supporting_observation_ids": list(claim[4]),
            "refuting_observation_ids": list(claim[5]),
            "status": str(claim[6]),
        },
        "answer_state": {
            "required_claim_count": int(answer[0]),
            "supported_count": int(answer[1]),
            "unsupported_count": int(answer[2]),
            "refuted_count": int(answer[3]),
            "conflicted_count": int(answer[4]),
            "status": str(answer[5]),
        },
    }


def _event_provenance(
    connection: Connection[Any],
    *,
    event: _HistoryEvent,
    epoch_id: int,
    manifest: CandidatePolicyManifest,
    bundle: PinnedM3AdapterBundle,
) -> dict[str, int]:
    values = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_semantic_job WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_semantic_job_attempt AS attempt
             JOIN groundloop_semantic_job AS job USING (job_id)
            WHERE job.epoch_id = %s),
          (SELECT count(*) FROM groundloop_m4_discovery_result WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_impact_channel_hit WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_admitted_pair WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_working_observation_delta
            WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_semantic_observation
            WHERE produced_epoch = %s),
          (SELECT count(*) FROM groundloop_m4_verification_execution AS execution
             JOIN groundloop_semantic_job AS job ON job.job_id = execution.job_id
            WHERE job.epoch_id = %s),
          (SELECT count(*) FROM groundloop_pair_judgment AS judgment
             JOIN groundloop_semantic_job AS job
               ON job.result_artifact_id = judgment.source_artifact_id
            WHERE job.epoch_id = %s)
        """,
        (epoch_id,) * 9,
    ).fetchone()
    if values is None:
        raise ValidationError("event provenance count query returned no row")
    counts = tuple(int(value) for value in values)
    (
        jobs,
        attempts,
        discovery,
        hits,
        admitted,
        deltas,
        observations,
        executions,
        judgments,
    ) = counts
    root_count = connection.execute(
        """
        SELECT count(*) FROM groundloop_semantic_job
        WHERE epoch_id = %s AND parent_job_id IS NULL
        """,
        (epoch_id,),
    ).fetchone()
    verifier_count = connection.execute(
        """
        SELECT count(*) FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_kind = 'verify_pair'
        """,
        (epoch_id,),
    ).fetchone()
    incomplete = connection.execute(
        """
        SELECT count(*) FROM groundloop_semantic_job AS job
        LEFT JOIN groundloop_semantic_job_attempt AS attempt
          ON attempt.job_id = job.job_id
        WHERE job.epoch_id = %s
          AND (job.job_state NOT IN ('completed_active', 'completed_inactive')
               OR job.result_artifact_id IS NULL
               OR job.result_artifact_hash IS NULL
               OR attempt.attempt_state <> 'completed')
        """,
        (epoch_id,),
    ).fetchone()
    if root_count != (event.expected_discovery_calls,):
        raise ValidationError("event root-job count differs from the frozen history")
    if verifier_count != (event.expected_verifier_calls,):
        raise ValidationError("event verifier-job count differs from model calls")
    if incomplete != (0,) or attempts != jobs:
        raise ValidationError("event job/attempt provenance is incomplete")
    if discovery != event.expected_discovery_calls:
        raise ValidationError("event discovery result provenance is incomplete")
    if not (observations == executions == judgments == event.expected_verifier_calls):
        raise ValidationError("event verifier provenance cardinalities disagree")
    execution_conflicts = connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m4_verification_execution AS execution
        JOIN groundloop_semantic_job AS job ON job.job_id = execution.job_id
        WHERE job.epoch_id = %s
          AND (execution.model_artifact_id <> %s
               OR execution.prompt_artifact_id <> %s
               OR execution.execution_spec_hash <> %s
               OR execution.calibration_artifact_sha256 IS NULL
               OR cardinality(execution.raw_logits) <> 3)
        """,
        (
            epoch_id,
            bundle.verifier.spec.model_artifact.artifact_id,
            bundle.verifier.spec.prompt_artifact.artifact_id,
            manifest.verifier_execution_spec_hash,
        ),
    ).fetchone()
    if execution_conflicts != (0,):
        raise ValidationError("persisted verifier identity/provenance drifted")
    for chunk_id in event.plan.inserted_chunk_version_ids:
        artifact = connection.execute(
            """
            SELECT count(*) FROM groundloop_m4_role_embedding_artifact
            WHERE embedding_role = 'chunk_passage'
              AND chunk_version_id = %s
              AND model_artifact_id = %s
            """,
            (chunk_id, manifest.embedding_model_artifact_id),
        ).fetchone()
        chunker = connection.execute(
            """
            SELECT count(*) FROM groundloop_chunk_provenance
            WHERE chunk_version_id = %s
            """,
            (chunk_id,),
        ).fetchone()
        if artifact != (1,) or chunker != (1,):
            raise ValidationError("inserted chunk provenance is incomplete")
    update_row = connection.execute(
        """
        SELECT update_kind::text, candidate_policy_id, registry_snapshot_id,
               manifest #>> '{_groundloop_m4_runtime_v1,event_manifest,schema}'
        FROM groundloop_m4_update WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    if update_row != (
        event.plan.update.update_kind.value,
        manifest.policy_id,
        manifest.claim_registry_snapshot_id,
        "groundloop-m4-structural-payload-v1",
    ):
        raise ValidationError("durable event identity or structural manifest drifted")
    return {
        "jobs": jobs,
        "attempts": attempts,
        "discovery_results": discovery,
        "channel_hits": hits,
        "admitted_pairs": admitted,
        "working_observation_deltas": deltas,
        "semantic_observations": observations,
        "verification_executions": executions,
        "pair_judgments": judgments,
    }


def _run_and_audit_event(
    *,
    config: M4RealDynamicHistoryConfig,
    schema_name: str,
    model_config: PinnedM3ReuseConfig,
    manifest: CandidatePolicyManifest,
    vector_config: ExactPgvectorConfig,
    server: PostgresAdmissionServerIdentity,
    event: _HistoryEvent,
) -> tuple[dict[str, object], int]:
    payloads = {event.plan.update.event_id: event.payload}
    with _connect(config.database_url, schema_name) as connection:
        live_server = PostgresAdmissionServerIdentity.inspect(connection)
        if live_server != server:
            raise ValidationError("PostgreSQL server identity drifted across reconnect")
        bundle = build_pinned_m3_adapters(
            model_config, artifact_root=config.artifact_root
        )
        composition = _compose(
            connection,
            bundle=bundle,
            registry=PostgresM4ArtifactRegistry(connection),
            manifest=manifest,
            vector_config=vector_config,
            server=live_server,
            lexical_config_path=config.resolved_lexical_config_path,
            payloads=payloads,
        )
        result = composition.application.run_event(event.plan)
        if result.state is not EventRunState.SEALED:
            raise ValidationError(f"real dynamic event did not seal: {result}")
        actual_calls = (
            result.discovery_call_count,
            composition.embeddings.request_count,
            composition.verifier.backend_pair_calls,
        )
        expected_calls = (
            event.expected_discovery_calls,
            event.expected_embedding_requests,
            event.expected_verifier_calls,
        )
        if actual_calls != expected_calls:
            raise ValidationError(
                f"real event call counts {actual_calls!r} != {expected_calls!r}"
            )
        exact = _assert_exact_surfaces(
            connection, composition=composition, epoch_id=result.epoch_id
        )
        provenance = _event_provenance(
            connection,
            event=event,
            epoch_id=result.epoch_id,
            manifest=manifest,
            bundle=bundle,
        )
        before_replay = _table_snapshot(connection)
        before_digest = _snapshot_digest(before_replay)
        epoch_rows = _row_counts(connection, result.epoch_id)
        artifact_rows = _artifact_counts(connection)

    # Reconnect and rebuild every model/DB port.  A valid sealed replay must
    # short-circuit before either empirical model boundary is invoked.
    with _connect(config.database_url, schema_name) as replay_connection:
        replay_bundle = build_pinned_m3_adapters(
            model_config, artifact_root=config.artifact_root
        )
        replay_composition = _compose(
            replay_connection,
            bundle=replay_bundle,
            registry=PostgresM4ArtifactRegistry(replay_connection),
            manifest=manifest,
            vector_config=vector_config,
            server=PostgresAdmissionServerIdentity.inspect(replay_connection),
            lexical_config_path=config.resolved_lexical_config_path,
            payloads=payloads,
        )
        replay = replay_composition.application.run_event(event.plan)
        if replay.state is not EventRunState.REPLAYED:
            raise ValidationError("sealed event did not exact-replay after reconnect")
        replay_calls = (
            replay.discovery_call_count,
            replay.verifier_call_count,
            replay_composition.embeddings.request_count,
            replay_composition.verifier.request_count,
            replay_composition.verifier.backend_pair_calls,
        )
        if replay_calls != (0, 0, 0, 0, 0):
            raise ValidationError("exact replay performed model or discovery work")
        after_replay = _table_snapshot(replay_connection)
        after_digest = _snapshot_digest(after_replay)
        if after_replay != before_replay or after_digest != before_digest:
            raise ValidationError(
                "exact replay changed the durable database projection"
            )
        _assert_exact_surfaces(
            replay_connection,
            composition=replay_composition,
            epoch_id=replay.epoch_id,
        )

    return (
        {
            "event_id": result.event_id,
            "update_kind": event.plan.update.update_kind.value,
            "epoch_id": result.epoch_id,
            "state": result.state.value,
            "publication_id": result.publication_id,
            "compact_registry_identity_only": event.plan.registered_claim_ids == (),
            "model_calls": {
                "discovery": result.discovery_call_count,
                "admission_embedding_requests": composition.embeddings.request_count,
                "verifier_requests": composition.verifier.request_count,
                "verifier_backend_pair_calls": (
                    composition.verifier.backend_pair_calls
                ),
            },
            "exact_surfaces": exact,
            "durable_provenance": provenance,
            "durable_epoch_rows": dict(sorted(epoch_rows.items())),
            "durable_artifact_counts_after_event": dict(sorted(artifact_rows.items())),
            "fresh_connection_exact_replay": {
                "state": replay.state.value,
                "zero_model_and_discovery_calls": True,
                "database_projection_unchanged": True,
                "projection_sha256": after_digest,
                "table_count": len(after_replay),
            },
        },
        result.epoch_id,
    )


def run_m4_real_dynamic_history(
    config: M4RealDynamicHistoryConfig,
) -> M4RealDynamicHistoryResult:
    """Execute M4.8 and return a machine-readable evidence manifest."""
    model_config = PinnedM3ReuseConfig.load(config.resolved_model_config_path)
    availability = inspect_local_artifacts(
        model_config, artifact_root=config.artifact_root
    )
    if not availability.available:
        raise SmokeUnavailableError("; ".join(availability.problems))
    schema_name = f"{config.schema_prefix}_{uuid.uuid4().hex}"
    try:
        setup_connection = psycopg.connect(
            _psycopg_url(config.database_url), autocommit=True
        )
    except psycopg.Error as error:
        raise SmokeUnavailableError(f"PostgreSQL is unavailable: {error}") from error

    created = False
    try:
        with setup_connection:
            _create_schema(setup_connection, schema_name)
            created = True
            server = PostgresAdmissionServerIdentity.inspect(setup_connection)
            chunker = FixedCharChunker()
            bundle = build_pinned_m3_adapters(
                model_config, artifact_root=config.artifact_root
            )
            registry = PostgresM4ArtifactRegistry(setup_connection)
            _register_static_artifacts(registry, bundle, chunker)
            base_epoch_id = _seed_b0(
                setup_connection, config=model_config, chunker=chunker
            )
            auxiliary_chunk_id = _seed_auxiliary_replacement_target(
                setup_connection, epoch_id=base_epoch_id, chunker=chunker
            )
            registry_snapshot_id = stable_m4_digest(
                _REGISTRY_NAMESPACE, _CLAIM_ID, _CLAIM_TEXT
            )
            lexical_config = load_frozen_lexical_v1(config.repo_root)
            manifest, vector_config = _build_manifest(
                bundle=bundle,
                server=server,
                registry_snapshot_id=registry_snapshot_id,
                lexical_config_hash=lexical_config.config_hash,
            )
            setup_ports = PostgresM4ApplicationPorts(
                setup_connection,
                structural_payloads={},
                execution_mode=M4ExecutionMode.MEASURED,
            )
            if not setup_ports.register_claim_registry_snapshot(
                registry_snapshot_id, (_CLAIM_ID,)
            ):
                raise ValidationError("fresh compact claim registry was replayed")
            _prepare_registry(
                setup_connection,
                bundle=bundle,
                registry=registry,
                manifest=manifest,
            )
            if not setup_ports.runtime_store.register_candidate_policy(manifest):
                raise ValidationError("fresh history candidate policy was replayed")

            insert_document, insert_chunk_id = _inserted_document(
                chunker=chunker,
                document_id="m4-real-history-insert-document",
                version_namespace="m4-real-history-insert-version",
                text=_INSERT_TEXT,
                source_uri="history://insert",
            )
            replacement_document, replacement_chunk_id = _inserted_document(
                chunker=chunker,
                document_id=_AUXILIARY_DOCUMENT_ID,
                version_namespace="m4-real-history-replacement-version",
                text=_REPLACEMENT_TEXT,
                source_uri="history://replacement-target",
            )

        insert_payload = StructuralPayload(inserted=insert_document)
        insert_plan = _event_plan(
            event_id="m4-real-history-insert",
            kind=UpdateKind.INSERT,
            previous_epoch_id=base_epoch_id,
            manifest=manifest,
            payload=insert_payload,
            inserted_chunk_ids=(insert_chunk_id,),
        )
        insert_event = _HistoryEvent(insert_plan, insert_payload, 1, 1, 1)
        insert_result, insert_epoch = _run_and_audit_event(
            config=config,
            schema_name=schema_name,
            model_config=model_config,
            manifest=manifest,
            vector_config=vector_config,
            server=server,
            event=insert_event,
        )

        delete_payload = StructuralPayload(
            deactivated_document_version_id="m4-real-smoke-base-version"
        )
        delete_plan = _event_plan(
            event_id="m4-real-history-delete",
            kind=UpdateKind.DELETE,
            previous_epoch_id=insert_epoch,
            manifest=manifest,
            payload=delete_payload,
            deactivated_chunk_ids=("m4-real-smoke-base-chunk",),
        )
        delete_event = _HistoryEvent(delete_plan, delete_payload, 1, 0, 0)
        delete_result, delete_epoch = _run_and_audit_event(
            config=config,
            schema_name=schema_name,
            model_config=model_config,
            manifest=manifest,
            vector_config=vector_config,
            server=server,
            event=delete_event,
        )

        replacement_payload = StructuralPayload(
            inserted=replacement_document,
            deactivated_document_version_id=_AUXILIARY_VERSION_ID,
        )
        replacement_plan = _event_plan(
            event_id="m4-real-history-replace",
            kind=UpdateKind.REPLACE,
            previous_epoch_id=delete_epoch,
            manifest=manifest,
            payload=replacement_payload,
            inserted_chunk_ids=(replacement_chunk_id,),
            deactivated_chunk_ids=(auxiliary_chunk_id,),
        )
        replacement_event = _HistoryEvent(
            replacement_plan, replacement_payload, 1, 1, 1
        )
        replacement_result, replacement_epoch = _run_and_audit_event(
            config=config,
            schema_name=schema_name,
            model_config=model_config,
            manifest=manifest,
            vector_config=vector_config,
            server=server,
            event=replacement_event,
        )

        verifier_spec = bundle.verifier.spec
        output: dict[str, object] = {
            "schema_version": _SCHEMA_VERSION,
            "scope": "bounded-real-dynamic-integration-not-model-quality",
            "downloads_allowed": False,
            "execution_mode": M4ExecutionMode.MEASURED.value,
            "temporary_schema": schema_name,
            "schema_retained": config.keep_schema,
            "postgres_version": server.postgres_version,
            "pgvector_version": server.pgvector_version,
            "claim_registry": {
                "snapshot_id": registry_snapshot_id,
                "claim_count": 1,
                "prebuilt_outside_event_kernel": True,
                "events_carry_identity_only": True,
            },
            "candidate_policy_id": manifest.policy_id,
            "candidate_policy_hash": manifest.policy_hash,
            "embedding_model_artifact_id": manifest.embedding_model_artifact_id,
            "embedding_model_tree_sha256": (
                model_config.embedding_snapshot_tree_sha256
            ),
            "verifier_model_artifact_id": (verifier_spec.model_artifact.artifact_id),
            "verifier_checkpoint_tree_sha256": (
                model_config.verifier_checkpoint_tree_sha256
            ),
            "verifier_prompt_artifact_id": (verifier_spec.prompt_artifact.artifact_id),
            "verifier_prompt_template_sha256": (
                verifier_spec.prompt_artifact.template_hash
            ),
            "calibration_version": verifier_spec.calibration_version,
            "calibration_artifact_sha256": (verifier_spec.calibration_artifact_sha256),
            "decision_policy_version": (verifier_spec.decision_policy.policy_version),
            "decision_policy_hash": decision_policy_hash(verifier_spec.decision_policy),
            "verifier_execution_spec_hash": manifest.verifier_execution_spec_hash,
            "history": [insert_result, delete_result, replacement_result],
            "history_epoch_ids": [insert_epoch, delete_epoch, replacement_epoch],
            "reconnected_for_every_event_and_replay": True,
            "all_events_sealed": True,
            "all_replays_zero_model_calls": True,
            "all_replays_database_unchanged": True,
            "all_events_equal_python_and_sql_oracles": True,
        }
        return M4RealDynamicHistoryResult(output)
    finally:
        if created and not config.keep_schema:
            cleanup = psycopg.connect(
                _psycopg_url(config.database_url), autocommit=True
            )
            with cleanup:
                _set_schema(cleanup, schema_name)
                _drop_schema(cleanup, schema_name)


def write_dynamic_history_manifest(
    result: M4RealDynamicHistoryResult, path: Path
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.to_json(), encoding="utf-8")
