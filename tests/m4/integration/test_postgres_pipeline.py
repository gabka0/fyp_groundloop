from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

import pytest
from psycopg import Connection

from groundloop.domain import (
    AnswerStatus,
    ChunkVersion,
    ClaimStatus,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    normalized_text_hash,
)
from groundloop.errors import EventConflictError
from groundloop.m4.admission.fresh import PostgresExactFreshFrontierRetriever
from groundloop.m4.admission.service import PostgresHybridAdmissionPort
from groundloop.m4.admission.vector import ChunkRoleVector, ReverseVectorSearch
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    EventRunState,
    JobLease,
    M4Application,
    ObservationCompletionReceipt,
    VerificationResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.pipeline import (
    InsertedDocument,
    M4ExecutionMode,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.postgres import record_epoch


def _hash(value: str) -> str:
    return stable_m4_digest(value)


def _unit_vector_literal(axis: int = 0) -> str:
    values = [0.0] * 384
    values[axis] = 1.0
    return "[" + ",".join(format(value, ".17g") for value in values) + "]"


IMPACT_HASH = _hash("m4-pipeline-impact")
FRONTIER_HASH = _hash("m4-pipeline-frontier")
VERIFIER_HASH = _hash("m4-pipeline-verifier")


def _candidate_policy(
    *, frontier_depth: int = 4, claim_count: int = 1
) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id="candidate-v1",
        embedding_model_artifact_id="embedder-v1",
        claim_role_template_hash=_hash("claim-role"),
        chunk_role_template_hash=_hash("chunk-role"),
        vector_method_version="exact-reverse-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_hash("vector-build"),
        vector_search_config_hash=_hash("vector-search"),
        lexical_method_version="postgres-simple-v1",
        lexical_config_hash=_hash("lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="registry-v1",
        claim_count=claim_count,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=4,
        frontier_depth=frontier_depth,
        verifier_execution_spec_hash=VERIFIER_HASH,
        decision_policy_version="decision-v1",
    )


def _seed_b0(connection: Connection[Any]) -> int:
    epoch_id, created = record_epoch(
        connection, event_id="b0", payload_hash=_hash("b0")
    )
    assert created
    connection.execute(
        """
        UPDATE groundloop_epoch SET revision = 1,
            structural_status = 'committed', semantic_status = 'sealed',
            evaluation_state = 'complete', publication_mode = 'strict',
            sealed_at = now() WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES "
        "('doc-support', 'fixture://support', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES "
        "('dv-support', 'doc-support', 'support-content', %s, NULL)",
        (epoch_id,),
    )
    support_text = "Nimbus is an atmospheric probe."
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES (
            'chunk-support', 'dv-support', 0, %s, %s,
            'fixed-char-v1', %s, NULL
        )
        """,
        (support_text, normalized_text_hash(support_text), epoch_id),
    )
    connection.execute(
        "INSERT INTO groundloop_question VALUES "
        "('question-1', 'What is Nimbus?', %s)",
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version VALUES (
            'answer-1', 'question-1', 'Nimbus is an atmospheric probe.',
            'generator', 'v1', 'prompt-v1', %s
        )
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim VALUES (
            'claim-1', 'answer-1', 'Nimbus is an atmospheric probe.',
            'extractor', 'v1', 'prompt-v1', true
        )
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy VALUES (
            'decision-v1', 0.8, 0.8, 'v1', NULL, NULL, %s, NULL
        )
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id, immutable_revision,
            tokenizer_revision, license_id, config_hash
        ) VALUES (
            'embedder-v1', 'embedding', 'test', 'embedder', 'v1', 'v1',
            'MIT', %s
        )
        """,
        (_hash("embedder-config"),),
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score, model_id,
            model_version, prompt_version, input_hash, produced_epoch,
            raw_output_hash
        ) VALUES (
            'observation-support', 'claim', 'claim-1', 'chunk-support',
            'claim-verification-v1', 0.9, 0.05, 0.05, 'verifier', 'v1',
            'prompt-v1', %s, %s, %s
        )
        """,
        (_hash("support-input"), epoch_id, _hash("support-output")),
    )
    connection.execute(
        """
        INSERT INTO groundloop_observation_currency VALUES (
            'claim', 'claim-1', 'chunk-support', 'claim-verification-v1',
            'observation-support', 1
        )
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim_state_materialized VALUES (
            'claim-1', 1, 0, 0.9, NULL,
            ARRAY['observation-support'], ARRAY[]::text[], 'supported', %s, 1
        )
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_state_materialized VALUES (
            'answer-1', 1, 1, 0, 0, 0, 'valid', %s, 1
        )
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim_certificate VALUES (
            'claim-1', 'observation-support', NULL, %s, 1
        )
        """,
        (epoch_id,),
    )
    bootstrap_m4_publication(connection, sealed_epoch_id=epoch_id)
    return epoch_id


@dataclass(slots=True)
class ScriptedAdmission:
    claims_by_chunk: dict[str, tuple[str, ...]]
    calls: int = 0

    def discover(
        self, epoch_id: int, root_job: LogicalJobSpec
    ) -> DiscoveryResult:
        self.calls += 1
        if root_job.kind is JobKind.IMPACT_DISCOVERY:
            chunk_id = root_job.target_chunk_version_id
            assert chunk_id is not None
            pairs = tuple(
                PairKey(claim_id, chunk_id)
                for claim_id in self.claims_by_chunk.get(chunk_id, ())
            )
        else:
            pairs = ()
        admitted = tuple(
            AdmittedPair(
                epoch_id,
                pair,
                root_job.candidate_policy_id,
                rank,
                (AdmissionChannel.VECTOR,),
                False,
            )
            for rank, pair in enumerate(pairs, start=1)
        )
        hits = tuple(
            ChannelHit(
                epoch_id,
                pair,
                root_job.candidate_policy_id,
                AdmissionChannel.VECTOR,
                rank,
                0.5,
                _hash(f"scripted-hit:{root_job.job_id}:{pair}"),
            )
            for rank, pair in enumerate(pairs, start=1)
        )
        result_hash = _hash(f"admission:{root_job.job_id}:{pairs}")
        return DiscoveryResult(
            root_job.job_id,
            f"admission:{root_job.job_id}",
            result_hash,
            admitted,
            channel_hits=hits,
        )


@dataclass(slots=True)
class ScriptedVerifier:
    labels: dict[str, str]
    calls: int = 0
    observations: dict[str, SemanticObservation] = field(default_factory=dict)

    def verify(
        self, epoch_id: int, verifier_job: LogicalJobSpec
    ) -> VerificationResult:
        del epoch_id
        self.calls += 1
        pair = verifier_job.pair
        assert pair is not None
        label = self.labels[pair.chunk_version_id]
        scores = {
            "support": (0.9, 0.05, 0.05),
            "neutral": (0.05, 0.05, 0.9),
            "refute": (0.05, 0.9, 0.05),
        }[label]
        observation = SemanticObservation(
            observation_id=_hash(
                f"observation:{pair.claim_id}:{pair.chunk_version_id}:{label}"
            ),
            subject_kind=SubjectKind.CLAIM,
            subject_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
            task_type="claim-verification-v1",
            support_score=scores[0],
            refute_score=scores[1],
            neutral_score=scores[2],
            producer=ModelStamp("scripted-verifier", "v1", "prompt-v1"),
            input_hash=_hash(f"input:{pair.claim_id}:{pair.chunk_version_id}"),
        )
        self.observations[observation.observation_id] = observation
        return VerificationResult(
            f"verification:{observation.observation_id}",
            _hash(repr(observation)),
            observation,
        )


@dataclass(slots=True)
class RecordingObservationPort:
    delegate: PostgresM4ApplicationPorts
    last_completion: (
        tuple[
            int,
            JobLease,
            LogicalJobSpec,
            JobCompletion,
            SemanticObservation,
            bool,
        ]
        | None
    ) = None

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
        self.last_completion = (
            epoch_id,
            lease,
            verifier_job,
            completion,
            observation,
            make_effective,
        )
        return self.delegate.complete_verifier_atomically(
            epoch_id,
            lease,
            verifier_job,
            completion,
            observation,
            make_effective=make_effective,
        )


def _inserted(
    document_id: str,
    version_id: str,
    chunk_id: str,
    text: str,
) -> InsertedDocument:
    return InsertedDocument(
        DocumentVersion(version_id, document_id, _hash(f"content:{version_id}")),
        (ChunkVersion(chunk_id, version_id, 0, text),),
        source_uri=f"fixture://{document_id}",
    )


def _event(
    event_id: str,
    kind: UpdateKind,
    previous: int,
    *,
    inserted: tuple[str, ...] = (),
    deactivated: tuple[str, ...] = (),
) -> DynamicEventPlan:
    return DynamicEventPlan(
        CorpusUpdateIdentity(
            event_id,
            _hash(f"payload:{event_id}:{kind}:{inserted}:{deactivated}"),
            kind,
            previous,
            "candidate-v1",
        ),
        inserted,
        deactivated,
        ("claim-1",),
        "registry-v1",
    )


def _state(connection: Connection[Any]) -> tuple[str, str]:
    claim = connection.execute(
        "SELECT status FROM groundloop_claim_state_materialized "
        "WHERE claim_id = 'claim-1'"
    ).fetchone()
    answer = connection.execute(
        "SELECT status FROM groundloop_answer_state_materialized "
        "WHERE answer_version_id = 'answer-1'"
    ).fetchone()
    assert claim is not None and answer is not None
    return str(claim[0]), str(answer[0])


def test_insert_delete_replace_replay_and_publication_are_exact(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    neutral = _inserted(
        "doc-neutral", "dv-neutral", "chunk-neutral", "Nimbus is blue."
    )
    refute = _inserted(
        "doc-neutral",
        "dv-refute",
        "chunk-refute",
        "Nimbus is not an atmospheric probe.",
    )
    payloads = {
        "insert-neutral": StructuralPayload(inserted=neutral),
        "delete-support": StructuralPayload(
            deactivated_document_version_id="dv-support"
        ),
        "replace-refute": StructuralPayload(
            inserted=refute,
            deactivated_document_version_id="dv-neutral",
        ),
    }
    store_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection, structural_payloads=payloads
    )
    store_ports.runtime_store.register_candidate_policy(_candidate_policy())
    admission_delegate = ScriptedAdmission(
        {
            "chunk-neutral": ("claim-1",),
            "chunk-refute": ("claim-1",),
        }
    )
    admission = PersistingAdmissionPort(
        m4_pipeline_connection, admission_delegate
    )
    verifier = ScriptedVerifier(
        {"chunk-neutral": "neutral", "chunk-refute": "refute"}
    )
    application = M4Application(
        structural=store_ports,
        runtime=store_ports,
        admission=admission,
        verifier=verifier,
        observations=store_ports,
        equality_gates=store_ports,
        publication=store_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )

    insertion = _event(
        "insert-neutral",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-neutral",),
    )
    inserted = application.run_event(insertion)
    assert inserted.state is EventRunState.SEALED
    assert _state(m4_pipeline_connection) == (
        ClaimStatus.SUPPORTED.value,
        AnswerStatus.VALID.value,
    )

    deletion = _event(
        "delete-support",
        UpdateKind.DELETE,
        inserted.epoch_id,
        deactivated=("chunk-support",),
    )
    deleted = application.run_event(deletion)
    assert deleted.state is EventRunState.SEALED
    assert deleted.verifier_call_count == 0
    assert _state(m4_pipeline_connection) == (
        ClaimStatus.UNSUPPORTED.value,
        AnswerStatus.UNSUPPORTED.value,
    )

    replacement = _event(
        "replace-refute",
        UpdateKind.REPLACE,
        deleted.epoch_id,
        inserted=("chunk-refute",),
        deactivated=("chunk-neutral",),
    )
    replaced = application.run_event(replacement)
    assert replaced.state is EventRunState.SEALED
    assert _state(m4_pipeline_connection) == (
        ClaimStatus.REFUTED.value,
        AnswerStatus.CONTRADICTED.value,
    )
    calls = (admission_delegate.calls, verifier.calls)
    restarted_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection, structural_payloads=payloads
    )
    restarted_application = M4Application(
        structural=restarted_ports,
        runtime=restarted_ports,
        admission=admission,
        verifier=verifier,
        observations=restarted_ports,
        equality_gates=restarted_ports,
        publication=restarted_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    replay = restarted_application.run_event(replacement)
    assert replay.state is EventRunState.REPLAYED
    assert (admission_delegate.calls, verifier.calls) == calls

    conflicting = replace(
        replacement,
        update=replace(replacement.update, payload_hash=_hash("conflict")),
    )
    m4_pipeline_connection.execute(
        "DELETE FROM groundloop_m4_execution_accounting WHERE epoch_id = %s",
        (replaced.epoch_id,),
    )
    with pytest.raises(EventConflictError):
        restarted_application.run_event(conflicting)
    assert m4_pipeline_connection.execute(
        """
        SELECT count(*) FROM groundloop_m4_execution_accounting
        WHERE epoch_id = %s
        """,
        (replaced.epoch_id,),
    ).fetchone() == (0,)
    assert restarted_application.run_event(replacement).state is EventRunState.REPLAYED
    assert m4_pipeline_connection.execute(
        """
        SELECT execution_mode FROM groundloop_m4_execution_accounting
        WHERE epoch_id = %s
        """,
        (replaced.epoch_id,),
    ).fetchone() == (M4ExecutionMode.AUDIT.value,)

    head = m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone()
    assert head == (replaced.epoch_id,)
    sealed_revision = m4_pipeline_connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (replaced.epoch_id,),
    ).fetchone()
    assert sealed_revision is not None
    assert m4_pipeline_connection.execute(
        """
        SELECT evaluation_state, confirmed_as_of_epoch,
               open_required_job_count, discovery_scope_open,
               updated_revision
        FROM groundloop_object_evaluation
        WHERE epoch_id = %s ORDER BY object_type
        """,
        (replaced.epoch_id,),
    ).fetchall() == [
        ("complete", replaced.epoch_id, 0, False, sealed_revision[0]),
        ("complete", replaced.epoch_id, 0, False, sealed_revision[0]),
    ]
    deltas = m4_pipeline_connection.execute(
        """
        SELECT event_id, object_type, old_status, new_status
        FROM groundloop_status_delta ORDER BY delta_id
        """
    ).fetchall()
    assert deltas == [
        ("delete-support", "claim", "supported", "unsupported"),
        ("delete-support", "answer", "valid", "unsupported"),
        ("replace-refute", "claim", "unsupported", "refuted"),
        ("replace-refute", "answer", "unsupported", "contradicted"),
    ]


def test_failed_event_preserves_publication_head_and_published_state(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-fail", "dv-fail", "chunk-fail", "failure fixture"
    )
    event = _event(
        "failed-insert",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-fail",),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "failed-insert": StructuralPayload(inserted=inserted_document)
        },
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())

    @dataclass(slots=True)
    class FailingAdmission:
        calls: int = 0

        def discover(
            self, epoch_id: int, root_job: LogicalJobSpec
        ) -> DiscoveryResult:
            del epoch_id, root_job
            from groundloop.m4.application import ExternalWorkFailure

            self.calls += 1
            raise ExternalWorkFailure("injected admission failure")

    failing_admission = FailingAdmission()
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=failing_admission,
        verifier=ScriptedVerifier({}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    result = application.run_event(event)
    assert result.state is EventRunState.FAILED
    restarted_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "failed-insert": StructuralPayload(inserted=inserted_document)
        },
    )
    replay = M4Application(
        structural=restarted_ports,
        runtime=restarted_ports,
        admission=failing_admission,
        verifier=ScriptedVerifier({}),
        observations=restarted_ports,
        equality_gates=restarted_ports,
        publication=restarted_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(event)
    assert replay.state is EventRunState.FAILED
    assert replay.failure_reason == "injected admission failure"
    assert replay.discovery_call_count == 0
    assert failing_admission.calls == 1
    assert _state(m4_pipeline_connection) == ("supported", "valid")
    assert m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone() == (base,)
    assert m4_pipeline_connection.execute(
        "SELECT observation_id FROM groundloop_observation_currency"
    ).fetchall() == [("observation-support",)]
    assert m4_pipeline_connection.execute(
        """
        SELECT source_uri, authority_class FROM groundloop_document
        WHERE document_id = 'doc-fail'
        """
    ).fetchone() == (None, "unclassified")

    corrected_text = "corrected document metadata"
    corrected = InsertedDocument(
        DocumentVersion("dv-corrected", "doc-fail", _hash("corrected-content")),
        (
            ChunkVersion(
                "chunk-corrected",
                "dv-corrected",
                0,
                corrected_text,
            ),
        ),
        source_uri="fixture://corrected",
        authority_class="verified",
    )
    corrected_event = _event(
        "corrected-insert",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-corrected",),
    )
    corrected_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "corrected-insert": StructuralPayload(inserted=corrected)
        },
    )
    corrected_result = M4Application(
        structural=corrected_ports,
        runtime=corrected_ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-corrected": ("claim-1",)}),
        ),
        verifier=ScriptedVerifier({"chunk-corrected": "neutral"}),
        observations=corrected_ports,
        equality_gates=corrected_ports,
        publication=corrected_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(corrected_event)
    assert corrected_result.state is EventRunState.SEALED
    assert m4_pipeline_connection.execute(
        """
        SELECT source_uri, authority_class FROM groundloop_document
        WHERE document_id = 'doc-fail'
        """
    ).fetchone() == ("fixture://corrected", "verified")
    assert m4_pipeline_connection.execute(
        """
        SELECT source_uri, authority_class
        FROM groundloop_m4_document_metadata_overlay AS overlay
        JOIN groundloop_epoch AS epoch USING (epoch_id)
        WHERE epoch.event_id = 'failed-insert'
        """
    ).fetchone() == ("fixture://doc-fail", "dynamic")


@pytest.mark.parametrize(
    "failure_point",
    [
        "expansion_discovery_persisted",
        "expansion_runtime_completed",
        "expansion_evaluation_synced",
    ],
)
def test_discovery_result_and_child_closure_are_one_transaction(
    m4_pipeline_connection: Connection[Any], failure_point: str
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-expansion", "dv-expansion", "chunk-expansion", "Nimbus is blue."
    )

    def inject(point: str) -> None:
        if point == failure_point:
            raise RuntimeError(f"injected:{point}")

    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "expansion-failure": StructuralPayload(inserted=inserted_document)
        },
        failure_injector=inject,
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-expansion": ("claim-1",)}),
        ),
        verifier=ScriptedVerifier({"chunk-expansion": "neutral"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    event = _event(
        "expansion-failure",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-expansion",),
    )
    with pytest.raises(RuntimeError, match=failure_point):
        application.run_event(event)

    epoch_id = m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s",
        (event.update.event_id,),
    ).fetchone()
    assert epoch_id is not None
    assert m4_pipeline_connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m4_discovery_result
           WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_impact_channel_hit
           WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_admitted_pair
           WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_candidate_frontier
           WHERE valid_from_epoch = %s),
          (SELECT count(*) FROM groundloop_semantic_job
           WHERE epoch_id = %s AND parent_job_id IS NOT NULL)
        """,
        (epoch_id[0],) * 5,
    ).fetchone() == (0, 0, 0, 0, 0)
    assert m4_pipeline_connection.execute(
        """
        SELECT job_state, child_closed FROM groundloop_semantic_job
        WHERE epoch_id = %s AND parent_job_id IS NULL
        """,
        (epoch_id[0],),
    ).fetchone() == ("running", False)
    assert m4_pipeline_connection.execute(
        """
        SELECT closed_revision FROM groundloop_discovery_scope
        WHERE epoch_id = %s
        """,
        (epoch_id[0],),
    ).fetchone() == (None,)


def test_structural_open_surface_c_is_exact_before_any_attempt(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-open", "dv-open", "chunk-open", "Nimbus is blue."
    )
    event = _event(
        "surface-c-open",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-open",),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "surface-c-open": StructuralPayload(inserted=inserted_document)
        },
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection, ScriptedAdmission({})
        ),
        verifier=ScriptedVerifier({}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    withdrawal = ports.plan_exact_withdrawal(event)
    roots = application._root_jobs(event, withdrawal.fallback_claim_ids)
    scopes = tuple(
        DiscoveryScope(
            root.job_id,
            event.claim_registry_snapshot_id,
            event.registered_claim_ids,
        )
        for root in roots
        if root.kind is JobKind.IMPACT_DISCOVERY
    )
    opened = ports.open_event(event, withdrawal, roots, scopes)
    assert not opened.replayed
    assert m4_pipeline_connection.execute(
        """
        SELECT object_type, evaluation_state, confirmed_as_of_epoch,
               open_required_job_count, discovery_scope_open, updated_revision
        FROM groundloop_object_evaluation
        WHERE epoch_id = %s ORDER BY object_type
        """,
        (opened.epoch_id,),
    ).fetchall() == [
        ("answer", "pending", base, 0, True, 1),
        ("claim", "pending", base, 0, True, 1),
    ]


def test_compact_pending_does_not_propagate_optional_claim_to_answer(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_claim VALUES (
            'claim-optional', 'answer-1', 'Optional detail.',
            'extractor', 'v1', 'prompt-v1', false
        )
        """
    )
    inserted_document = _inserted(
        "doc-optional", "dv-optional", "chunk-optional", "Optional detail."
    )
    event = replace(
        _event(
            "optional-pending",
            UpdateKind.INSERT,
            base,
            inserted=("chunk-optional",),
        ),
        registered_claim_ids=("claim-1", "claim-optional"),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "optional-pending": StructuralPayload(inserted=inserted_document)
        },
        execution_mode=M4ExecutionMode.MEASURED,
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy(claim_count=2))
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection, ScriptedAdmission({})
        ),
        verifier=ScriptedVerifier({}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    withdrawal = ports.plan_exact_withdrawal(event)
    roots = application._root_jobs(event, withdrawal.fallback_claim_ids)
    scopes = tuple(
        DiscoveryScope(
            root.job_id,
            event.claim_registry_snapshot_id,
            event.registered_claim_ids,
        )
        for root in roots
        if root.kind is JobKind.IMPACT_DISCOVERY
    )
    opened = ports.open_event(event, withdrawal, roots, scopes)
    root = roots[0]
    pair = PairKey("claim-optional", "chunk-optional")
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event.update.event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=event.update.candidate_policy_id,
        execution_spec_hash=VERIFIER_HASH,
        parent_job_id=root.job_id,
        claim_id=pair.claim_id,
        chunk_version_id=pair.chunk_version_id,
    )
    optional_job = LogicalJobSpec(
        job_id=job_id,
        event_id=event.update.event_id,
        kind=JobKind.VERIFY_PAIR,
        candidate_policy_id=event.update.candidate_policy_id,
        payload_hash=_hash(f"verify:{pair.claim_id}:{pair.chunk_version_id}"),
        execution_spec_hash=VERIFIER_HASH,
        parent_job_id=root.job_id,
        pair=pair,
    )
    with m4_pipeline_connection.transaction():
        with m4_pipeline_connection.cursor() as cursor:
            ports._write_compact_evaluation(
                cursor,
                epoch_id=opened.epoch_id,
                revision=2,
                confirmed_as_of_epoch=base,
                open_jobs=(optional_job,),
                discovery_scope_open=False,
                failed=False,
            )

    assert m4_pipeline_connection.execute(
        """
        SELECT object_type, object_id FROM groundloop_object_evaluation
        WHERE epoch_id = %s ORDER BY object_type, object_id
        """,
        (opened.epoch_id,),
    ).fetchall() == [("claim", "claim-optional")]


@pytest.mark.parametrize(
    "failure_point",
    [
        "verifier_runtime_completed",
        "verifier_observation_archived",
        "verifier_overlay_written",
        "verifier_state_written",
    ],
)
def test_verifier_microtransaction_rolls_back_every_logical_step(
    m4_pipeline_connection: Connection[Any], failure_point: str
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-observation",
        "dv-observation",
        "chunk-observation",
        "Nimbus is blue.",
    )

    def inject(point: str) -> None:
        if point == failure_point:
            raise RuntimeError(f"injected:{point}")

    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "observation-failure": StructuralPayload(inserted=inserted_document)
        },
        failure_injector=inject,
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    admission = ScriptedAdmission({"chunk-observation": ("claim-1",)})
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(m4_pipeline_connection, admission),
        verifier=ScriptedVerifier({"chunk-observation": "neutral"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    event = _event(
        "observation-failure",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-observation",),
    )

    with pytest.raises(RuntimeError, match=failure_point):
        application.run_event(event)

    epoch_id = m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_epoch WHERE event_id = %s",
        (event.update.event_id,),
    ).fetchone()
    assert epoch_id is not None
    observation_count = m4_pipeline_connection.execute(
        """
        SELECT count(*) FROM groundloop_semantic_observation
        WHERE produced_epoch = %s
        """,
        (epoch_id[0],),
    ).fetchone()
    overlay_count = m4_pipeline_connection.execute(
        """
        SELECT count(*) FROM groundloop_working_observation_delta
        WHERE epoch_id = %s
        """,
        (epoch_id[0],),
    ).fetchone()
    child_state = m4_pipeline_connection.execute(
        """
        SELECT job_state FROM groundloop_semantic_job
        WHERE epoch_id = %s AND job_kind = 'verify_pair'
        """,
        (epoch_id[0],),
    ).fetchone()
    assert observation_count == (0,)
    assert overlay_count == (0,)
    assert child_state == ("running",)
    assert _state(m4_pipeline_connection) == ("supported", "valid")
    assert m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone() == (base,)


def test_active_verifier_completion_exact_replay_is_read_only_and_validated(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-replay", "dv-replay", "chunk-replay", "Nimbus is blue."
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "verifier-replay": StructuralPayload(inserted=inserted_document)
        },
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    recorder = RecordingObservationPort(ports)
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-replay": ("claim-1",)}),
        ),
        verifier=ScriptedVerifier({"chunk-replay": "neutral"}),
        observations=recorder,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    result = application.run_event(
        _event(
            "verifier-replay",
            UpdateKind.INSERT,
            base,
            inserted=("chunk-replay",),
        )
    )
    assert result.state is EventRunState.SEALED
    assert recorder.last_completion is not None
    before = m4_pipeline_connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_semantic_observation
           WHERE produced_epoch = %s),
          (SELECT count(*) FROM groundloop_working_observation_delta
           WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_working_transition
           WHERE epoch_id = %s),
          (SELECT revision FROM groundloop_epoch WHERE epoch_id = %s)
        """,
        (result.epoch_id, result.epoch_id, result.epoch_id, result.epoch_id),
    ).fetchone()
    assert before is not None
    epoch_id, lease, job, completion, observation, effective = recorder.last_completion

    replay = ports.complete_verifier_atomically(
        epoch_id,
        lease,
        job,
        completion,
        observation,
        make_effective=effective,
    )
    assert replay == ObservationCompletionReceipt(False, False)
    assert (
        m4_pipeline_connection.execute(
            """
        SELECT
          (SELECT count(*) FROM groundloop_semantic_observation
           WHERE produced_epoch = %s),
          (SELECT count(*) FROM groundloop_working_observation_delta
           WHERE epoch_id = %s),
          (SELECT count(*) FROM groundloop_working_transition
           WHERE epoch_id = %s),
          (SELECT revision FROM groundloop_epoch WHERE epoch_id = %s)
        """,
            (result.epoch_id, result.epoch_id, result.epoch_id, result.epoch_id),
        ).fetchone()
        == before
    )
    assert m4_pipeline_connection.execute(
        """
        SELECT attempt_state FROM groundloop_semantic_job_attempt
        WHERE attempt_id = %s
        """,
        (lease.attempt_id,),
    ).fetchone() == ("completed",)

    with pytest.raises(EventConflictError, match="different content"):
        ports.complete_verifier_atomically(
            epoch_id,
            lease,
            job,
            completion,
            replace(observation, input_hash=_hash("replay-input-drift")),
            make_effective=effective,
        )


@pytest.mark.parametrize(
    "failure_point",
    [
        "publication_structure_promoted",
        "publication_currency_promoted",
        "publication_states_written",
        "publication_head_advanced",
        "publication_evaluation_promoted",
        "publication_store_seal_epoch_written",
    ],
)
def test_publication_and_seal_roll_back_as_one_transaction(
    m4_pipeline_connection: Connection[Any], failure_point: str
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-publication",
        "dv-publication",
        "chunk-publication",
        "Nimbus is not an atmospheric probe.",
    )

    def inject(point: str) -> None:
        if point == failure_point:
            raise RuntimeError(f"injected:{point}")

    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "publication-failure": StructuralPayload(inserted=inserted_document)
        },
        failure_injector=inject,
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-publication": ("claim-1",)}),
        ),
        verifier=ScriptedVerifier({"chunk-publication": "refute"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    event = _event(
        "publication-failure",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-publication",),
    )

    with pytest.raises(RuntimeError, match=failure_point):
        application.run_event(event)

    epoch = m4_pipeline_connection.execute(
        """
        SELECT epoch_id, semantic_status FROM groundloop_epoch
        WHERE event_id = 'publication-failure'
        """
    ).fetchone()
    assert epoch is not None and epoch[1] == "complete"
    assert m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone() == (base,)
    assert _state(m4_pipeline_connection) == ("supported", "valid")
    assert m4_pipeline_connection.execute(
        """
        SELECT observation_id FROM groundloop_observation_currency
        ORDER BY observation_id
        """
    ).fetchall() == [("observation-support",)]
    working = m4_pipeline_connection.execute(
        """
        SELECT status FROM groundloop_m4_working_claim_state
        WHERE epoch_id = %s AND claim_id = 'claim-1'
        """,
        (epoch[0],),
    ).fetchone()
    assert working == ("conflicted",)
    evaluation_rows = m4_pipeline_connection.execute(
        """
        SELECT evaluation_state, confirmed_as_of_epoch,
               open_required_job_count, discovery_scope_open,
               updated_revision
        FROM groundloop_object_evaluation
        WHERE epoch_id = %s ORDER BY object_type
        """,
        (epoch[0],),
    ).fetchall()
    epoch_revision = m4_pipeline_connection.execute(
        "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s",
        (epoch[0],),
    ).fetchone()
    assert epoch_revision is not None
    assert evaluation_rows == [
        ("complete", base, 0, False, epoch_revision[0]),
        ("complete", base, 0, False, epoch_revision[0]),
    ]


@dataclass(frozen=True, slots=True)
class _EmbeddedChunkOnly:
    chunk_vectors: tuple[ChunkRoleVector, ...]


@dataclass(slots=True)
class _FakeEmbeddingService:
    def embed_for_admission(
        self, *, claims: tuple[object, ...] = (), chunks: tuple[object, ...] = ()
    ) -> _EmbeddedChunkOnly:
        del claims
        chunk = chunks[0]
        return _EmbeddedChunkOnly(
            (
                ChunkRoleVector(
                    chunk.chunk_version_id,
                    (1.0, 0.0),
                    _hash(chunk.text),
                ),
            )
        )


@dataclass(slots=True)
class _FakeVectorIndex:
    def search(
        self,
        *,
        epoch_id: int,
        candidate_policy_id: str,
        chunk: ChunkRoleVector,
        limit: int,
    ) -> ReverseVectorSearch:
        del limit
        hit = ChannelHit(
            epoch_id,
            PairKey("claim-1", chunk.chunk_version_id),
            candidate_policy_id,
            AdmissionChannel.VECTOR,
            1,
            0.8,
            _hash(f"vector:{epoch_id}:{chunk.chunk_version_id}"),
        )
        return ReverseVectorSearch(
            (hit,), _hash(f"vector-query:{epoch_id}:{chunk.chunk_version_id}")
        )


@dataclass(slots=True)
class _FakeLexicalPolicy:
    def search(
        self,
        *,
        epoch_id: int,
        manifest: CandidatePolicyManifest,
        chunk_version_id: str,
        chunk_text: str,
    ) -> tuple[ChannelHit, ...]:
        del chunk_text
        return (
            ChannelHit(
                epoch_id,
                PairKey("claim-1", chunk_version_id),
                manifest.policy_id,
                AdmissionChannel.LEXICAL,
                1,
                0.7,
                _hash(f"lexical:{epoch_id}:{chunk_version_id}"),
            ),
        )


def test_hybrid_admission_is_persisted_and_frontier_becomes_verified(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_chunker_artifact VALUES (
            'chunker-v1', 'fixed-char-v1', 'v1', %s, now()
        )
        """,
        (_hash("chunker-config"),),
    )
    inserted_document = InsertedDocument(
        DocumentVersion("dv-hybrid", "doc-hybrid", _hash("hybrid-content")),
        (
            ChunkVersion(
                "chunk-hybrid",
                "dv-hybrid",
                0,
                "Nimbus atmospheric probe evidence",
            ),
        ),
        source_uri="fixture://hybrid",
        chunker_artifact_id="chunker-v1",
        chunker_input_hash=_hash("hybrid-input"),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "hybrid-insert": StructuralPayload(inserted=inserted_document)
        },
    )
    manifest = _candidate_policy()
    ports.runtime_store.register_candidate_policy(manifest)
    admission = PostgresHybridAdmissionPort(
        connection=m4_pipeline_connection,
        manifest=manifest,
        embeddings=_FakeEmbeddingService(),
        vector_index=_FakeVectorIndex(),
        lexical_policy=_FakeLexicalPolicy(),
    )
    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=admission,
        verifier=ScriptedVerifier({"chunk-hybrid": "neutral"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    result = application.run_event(
        _event(
            "hybrid-insert",
            UpdateKind.INSERT,
            base,
            inserted=("chunk-hybrid",),
        )
    )
    assert result.state is EventRunState.SEALED
    assert m4_pipeline_connection.execute(
        """
        SELECT channel FROM groundloop_impact_channel_hit
        WHERE epoch_id = %s ORDER BY channel
        """,
        (result.epoch_id,),
    ).fetchall() == [("lexical",), ("vector",)]
    assert m4_pipeline_connection.execute(
        """
        SELECT reasons FROM groundloop_admitted_pair WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (["lexical", "vector"],)
    assert m4_pipeline_connection.execute(
        """
        SELECT frontier_state FROM groundloop_candidate_frontier
        WHERE claim_id = 'claim-1' AND chunk_version_id = 'chunk-hybrid'
          AND valid_to_epoch IS NULL
        """
    ).fetchone() == ("verified_current",)


def test_frontier_promotion_closes_exact_published_predecessor(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    reserve_text = "Nimbus has a reserve description."
    m4_pipeline_connection.execute(
        "INSERT INTO groundloop_document VALUES "
        "('doc-reserve', 'fixture://reserve', 'test')"
    )
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_document_version VALUES (
            'dv-reserve', 'doc-reserve', 'reserve-content', %s, NULL
        )
        """,
        (base,),
    )
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES (
            'chunk-reserve', 'dv-reserve', 0, %s, %s,
            'fixed-char-v1', %s, NULL
        )
        """,
        (reserve_text, normalized_text_hash(reserve_text), base),
    )
    policy = _candidate_policy(frontier_depth=1)
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={"delete-for-refill": StructuralPayload(
            deactivated_document_version_id="dv-support"
        )},
    )
    ports.runtime_store.register_candidate_policy(policy)
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_candidate_frontier VALUES (
            'claim-1', 'chunk-reserve', 'candidate-v1', 'unverified',
            1, 0.5, %s, %s, NULL
        )
        """,
        (_hash("reserve-frontier"), base),
    )
    admission = PostgresHybridAdmissionPort(
        m4_pipeline_connection,
        policy,
        _FakeEmbeddingService(),
        _FakeVectorIndex(),
        _FakeLexicalPolicy(),
    )
    event = _event(
        "delete-for-refill",
        UpdateKind.DELETE,
        base,
        deactivated=("chunk-support",),
    )
    result = M4Application(
        structural=ports,
        runtime=ports,
        admission=admission,
        verifier=ScriptedVerifier({"chunk-reserve": "neutral"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(event)
    assert result.state is EventRunState.SEALED
    assert m4_pipeline_connection.execute(
        """
        SELECT valid_from_epoch, valid_to_epoch, frontier_state
        FROM groundloop_candidate_frontier
        WHERE claim_id = 'claim-1' AND chunk_version_id = 'chunk-reserve'
          AND candidate_policy_id = 'candidate-v1'
        ORDER BY valid_from_epoch
        """
    ).fetchall() == [
        (base, result.epoch_id, "unverified"),
        (result.epoch_id, None, "verified_current"),
    ]


def test_empty_reserve_uses_exact_fresh_frontier_and_seals(
    m4_committed_pipeline_connection: Connection[Any],
) -> None:
    m4_pipeline_connection = m4_committed_pipeline_connection
    with m4_pipeline_connection.transaction():
        base = _seed_b0(m4_pipeline_connection)
    alternative_text = "Nimbus has an alternative neutral description."
    m4_pipeline_connection.execute(
        "INSERT INTO groundloop_document VALUES "
        "('doc-fresh-alternative', 'fixture://fresh-alternative', 'test')"
    )
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_document_version VALUES (
            'dv-fresh-alternative', 'doc-fresh-alternative',
            'fresh-alternative-content', %s, NULL
        )
        """,
        (base,),
    )
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES (
            'chunk-fresh-alternative', 'dv-fresh-alternative', 0, %s, %s,
            'fixed-char-v1', %s, NULL
        )
        """,
        (
            alternative_text,
            normalized_text_hash(alternative_text),
            base,
        ),
    )
    policy = _candidate_policy(frontier_depth=1)
    for subject_id, role, claim_id, chunk_id, role_hash in (
        (
            "claim-1",
            "claim_query",
            "claim-1",
            None,
            policy.claim_role_template_hash,
        ),
        (
            "chunk-fresh-alternative",
            "chunk_passage",
            None,
            "chunk-fresh-alternative",
            policy.chunk_role_template_hash,
        ),
    ):
        m4_pipeline_connection.execute(
            """
            INSERT INTO groundloop_m4_role_embedding_artifact (
                artifact_id, subject_id, embedding_role, claim_id,
                chunk_version_id, model_artifact_id, model_id,
                model_revision, tokenizer_revision, role_template_hash,
                input_hash, vector_hash, adapter_spec_hash, token_count,
                max_tokens, truncated, embedding
            ) VALUES (
                %s, %s, %s, %s, %s, 'embedder-v1', 'embedder', 'v1', 'v1',
                %s, %s, %s, %s, 5, 512, false, %s::vector
            )
            """,
            (
                _hash(f"fresh-artifact:{subject_id}:{role}"),
                subject_id,
                role,
                claim_id,
                chunk_id,
                role_hash,
                _hash(f"fresh-input:{subject_id}:{role}"),
                _hash(f"fresh-vector:{subject_id}:{role}"),
                _hash("fresh-adapter-v1"),
                _unit_vector_literal(),
            ),
        )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "delete-for-fresh-refill": StructuralPayload(
                deactivated_document_version_id="dv-support"
            )
        },
    )
    ports.runtime_store.register_candidate_policy(policy)
    admission = PostgresHybridAdmissionPort(
        m4_pipeline_connection,
        policy,
        _FakeEmbeddingService(),
        _FakeVectorIndex(),
        _FakeLexicalPolicy(),
        fresh_frontier_retriever=PostgresExactFreshFrontierRetriever(
            m4_pipeline_connection
        ),
    )
    result = M4Application(
        structural=ports,
        runtime=ports,
        admission=admission,
        verifier=ScriptedVerifier({"chunk-fresh-alternative": "neutral"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(
        _event(
            "delete-for-fresh-refill",
            UpdateKind.DELETE,
            base,
            deactivated=("chunk-support",),
        )
    )
    assert result.state is EventRunState.SEALED
    assert result.verifier_call_count == 1
    assert m4_pipeline_connection.execute(
        """
        SELECT fallback_satisfied, admitted_pair_count
        FROM groundloop_m4_discovery_result
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (True, 1)
    assert m4_pipeline_connection.execute(
        """
        SELECT channel, chunk_version_id, claim_id
        FROM groundloop_impact_channel_hit
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == ("frontier", "chunk-fresh-alternative", "claim-1")
    assert m4_pipeline_connection.execute(
        """
        SELECT frontier_state
        FROM groundloop_candidate_frontier
        WHERE valid_from_epoch = %s AND claim_id = 'claim-1'
          AND chunk_version_id = 'chunk-fresh-alternative'
        """,
        (result.epoch_id,),
    ).fetchone() == ("verified_current",)


def test_exact_fresh_frontier_can_close_complete_empty_corpus(
    m4_committed_pipeline_connection: Connection[Any],
) -> None:
    connection = m4_committed_pipeline_connection
    with connection.transaction():
        base = _seed_b0(connection)
    policy = _candidate_policy(frontier_depth=1)
    connection.execute(
        """
        INSERT INTO groundloop_m4_role_embedding_artifact (
            artifact_id, subject_id, embedding_role, claim_id,
            chunk_version_id, model_artifact_id, model_id,
            model_revision, tokenizer_revision, role_template_hash,
            input_hash, vector_hash, adapter_spec_hash, token_count,
            max_tokens, truncated, embedding
        ) VALUES (
            %s, 'claim-1', 'claim_query', 'claim-1', NULL,
            'embedder-v1', 'embedder', 'v1', 'v1', %s, %s, %s, %s,
            5, 512, false, %s::vector
        )
        """,
        (
            _hash("empty-fresh-claim-artifact"),
            policy.claim_role_template_hash,
            _hash("empty-fresh-claim-input"),
            _hash("empty-fresh-claim-vector"),
            _hash("empty-fresh-adapter-v1"),
            _unit_vector_literal(),
        ),
    )
    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads={
            "delete-to-empty-corpus": StructuralPayload(
                deactivated_document_version_id="dv-support"
            )
        },
    )
    ports.runtime_store.register_candidate_policy(policy)
    admission = PostgresHybridAdmissionPort(
        connection,
        policy,
        _FakeEmbeddingService(),
        _FakeVectorIndex(),
        _FakeLexicalPolicy(),
        fresh_frontier_retriever=PostgresExactFreshFrontierRetriever(connection),
    )
    result = M4Application(
        structural=ports,
        runtime=ports,
        admission=admission,
        verifier=ScriptedVerifier({}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(
        _event(
            "delete-to-empty-corpus",
            UpdateKind.DELETE,
            base,
            deactivated=("chunk-support",),
        )
    )
    assert result.state is EventRunState.SEALED
    assert result.verifier_call_count == 0
    assert _state(connection) == ("unsupported", "unsupported")
    assert connection.execute(
        """
        SELECT fallback_satisfied, admitted_pair_count
        FROM groundloop_m4_discovery_result
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (True, 0)


def test_empty_discovery_has_a_durable_closed_result_header(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-empty", "dv-empty", "chunk-empty", "No candidate matches."
    )
    event = _event(
        "empty-discovery",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-empty",),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "empty-discovery": StructuralPayload(inserted=inserted_document)
        },
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    result = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection, ScriptedAdmission({})
        ),
        verifier=ScriptedVerifier({}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(event)
    assert result.state is EventRunState.SEALED
    assert m4_pipeline_connection.execute(
        """
        SELECT result.fallback_satisfied, result.channel_hit_count,
               result.admitted_pair_count, job.child_closed,
               (SELECT count(*) FROM groundloop_semantic_job AS child
                WHERE child.parent_job_id = job.job_id)
        FROM groundloop_m4_discovery_result AS result
        JOIN groundloop_semantic_job AS job
          ON job.job_id = result.root_job_id
        WHERE result.epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (True, 0, 0, True, 0)


def test_restart_after_one_verifier_completion_rebuilds_working_snapshot(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = InsertedDocument(
        DocumentVersion("dv-restart", "doc-restart", _hash("restart-content")),
        (
            ChunkVersion("chunk-restart-a", "dv-restart", 0, "Nimbus is blue."),
            ChunkVersion("chunk-restart-b", "dv-restart", 1, "Nimbus is round."),
        ),
        source_uri="fixture://restart",
    )
    payloads = {
        "restart-insert": StructuralPayload(inserted=inserted_document)
    }
    event = _event(
        "restart-insert",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-restart-a", "chunk-restart-b"),
    )
    first_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection, structural_payloads=payloads
    )
    first_ports.runtime_store.register_candidate_policy(_candidate_policy())
    admission_delegate = ScriptedAdmission(
        {
            "chunk-restart-a": ("claim-1",),
            "chunk-restart-b": ("claim-1",),
        }
    )

    @dataclass(slots=True)
    class CrashOnSecondVerification:
        delegate: ScriptedVerifier
        calls: int = 0

        def verify(
            self, epoch_id: int, verifier_job: LogicalJobSpec
        ) -> VerificationResult:
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("simulated process crash")
            return self.delegate.verify(epoch_id, verifier_job)

    crashing = CrashOnSecondVerification(
        ScriptedVerifier(
            {"chunk-restart-a": "neutral", "chunk-restart-b": "neutral"}
        )
    )
    first_application = M4Application(
        structural=first_ports,
        runtime=first_ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection, admission_delegate
        ),
        verifier=crashing,
        observations=first_ports,
        equality_gates=first_ports,
        publication=first_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    )
    with pytest.raises(RuntimeError, match="simulated process crash"):
        first_application.run_event(event)

    epoch_row = m4_pipeline_connection.execute(
        "SELECT epoch_id, semantic_status FROM groundloop_epoch "
        "WHERE event_id = 'restart-insert'"
    ).fetchone()
    assert epoch_row is not None and epoch_row[1] == "pending"
    assert m4_pipeline_connection.execute(
        "SELECT count(*) FROM groundloop_semantic_observation "
        "WHERE produced_epoch = %s",
        (epoch_row[0],),
    ).fetchone() == (1,)
    assert m4_pipeline_connection.execute(
        """
        SELECT object_type, evaluation_state, open_required_job_count,
               discovery_scope_open
        FROM groundloop_object_evaluation
        WHERE epoch_id = %s ORDER BY object_type
        """,
        (epoch_row[0],),
    ).fetchall() == [
        ("answer", "pending", 1, False),
        ("claim", "pending", 1, False),
    ]

    restarted_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection, structural_payloads=payloads
    )
    restarted_verifier = ScriptedVerifier(
        {"chunk-restart-a": "neutral", "chunk-restart-b": "neutral"}
    )
    restarted = M4Application(
        structural=restarted_ports,
        runtime=restarted_ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection, admission_delegate
        ),
        verifier=restarted_verifier,
        observations=restarted_ports,
        equality_gates=restarted_ports,
        publication=restarted_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(event)

    assert restarted.state is EventRunState.SEALED
    assert restarted.discovery_call_count == 0
    assert restarted.verifier_call_count == 1
    assert restarted_verifier.calls == 1
    assert m4_pipeline_connection.execute(
        "SELECT count(*) FROM groundloop_semantic_observation "
        "WHERE produced_epoch = %s",
        (restarted.epoch_id,),
    ).fetchone() == (2,)
    assert _state(m4_pipeline_connection) == ("supported", "valid")


def test_late_postgres_completion_after_failure_is_archive_only(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-late", "dv-late", "chunk-late", "Nimbus is blue."
    )
    event = _event(
        "late-insert",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-late",),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "late-insert": StructuralPayload(inserted=inserted_document)
        },
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())

    @dataclass(slots=True)
    class FailingVerifier:
        def verify(
            self, epoch_id: int, verifier_job: LogicalJobSpec
        ) -> VerificationResult:
            del epoch_id, verifier_job
            from groundloop.m4.application import ExternalWorkFailure

            raise ExternalWorkFailure("worker disconnected")

    failed = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-late": ("claim-1",)}),
        ),
        verifier=FailingVerifier(),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(event)
    assert failed.state is EventRunState.FAILED
    epoch = ports.runtime_store.read_epoch(failed.epoch_id)
    child = next(job.spec for job in epoch.jobs if job.spec.kind is JobKind.VERIFY_PAIR)
    lease = ports.acquire_job(failed.epoch_id, child)
    verified = ScriptedVerifier({"chunk-late": "neutral"}).verify(
        failed.epoch_id, child
    )
    completion = JobCompletion.build(
        job_id=child.job_id,
        payload_hash=child.payload_hash,
        execution_spec_hash=child.execution_spec_hash,
        result_artifact_id=verified.result_artifact_id,
        result_artifact_hash=verified.result_artifact_hash,
        terminal_state=JobState.COMPLETED_INACTIVE,
    )
    receipt = ports.complete_verifier_atomically(
        failed.epoch_id,
        lease,
        child,
        completion,
        verified.observation,
        make_effective=False,
    )
    assert receipt.artifact_stored
    assert not receipt.made_effective
    assert ports.runtime_store.read_epoch(failed.epoch_id).state.value == "failed"
    assert m4_pipeline_connection.execute(
        "SELECT job_state FROM groundloop_semantic_job WHERE job_id = %s",
        (child.job_id,),
    ).fetchone() == ("completed_inactive",)
    assert m4_pipeline_connection.execute(
        """
        SELECT count(*) FROM groundloop_working_observation_delta
        WHERE epoch_id = %s
        """,
        (failed.epoch_id,),
    ).fetchone() == (0,)
    assert m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone() == (base,)


def test_failed_replacement_overlay_cannot_poison_next_published_event(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)

    def replacement(version_id: str, chunk_id: str, text: str) -> InsertedDocument:
        return InsertedDocument(
            DocumentVersion(version_id, "doc-support", _hash(version_id)),
            (ChunkVersion(chunk_id, version_id, 0, text),),
            source_uri="fixture://support",
            authority_class="test",
        )

    failed_document = replacement(
        "dv-failed-replace", "chunk-failed-replace", "Nimbus is blue."
    )
    failed_payload = StructuralPayload(
        inserted=failed_document,
        deactivated_document_version_id="dv-support",
    )
    failed_event = _event(
        "failed-replace-overlay",
        UpdateKind.REPLACE,
        base,
        inserted=("chunk-failed-replace",),
        deactivated=("chunk-support",),
    )
    failed_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={"failed-replace-overlay": failed_payload},
    )
    failed_ports.runtime_store.register_candidate_policy(_candidate_policy())

    @dataclass(slots=True)
    class ExternalFailure:
        def discover(
            self, epoch_id: int, root_job: LogicalJobSpec
        ) -> DiscoveryResult:
            del epoch_id, root_job
            from groundloop.m4.application import ExternalWorkFailure

            raise ExternalWorkFailure("failed replacement discovery")

    failed = M4Application(
        structural=failed_ports,
        runtime=failed_ports,
        admission=ExternalFailure(),
        verifier=ScriptedVerifier({}),
        observations=failed_ports,
        equality_gates=failed_ports,
        publication=failed_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(failed_event)
    assert failed.state is EventRunState.FAILED
    assert _state(m4_pipeline_connection) == ("supported", "valid")
    replay_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={"failed-replace-overlay": failed_payload},
    )
    replay = M4Application(
        structural=replay_ports,
        runtime=replay_ports,
        admission=ExternalFailure(),
        verifier=ScriptedVerifier({}),
        observations=replay_ports,
        equality_gates=replay_ports,
        publication=replay_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(failed_event)
    assert replay.state is EventRunState.FAILED
    assert replay.failure_reason == "failed replacement discovery"

    successful_document = replacement(
        "dv-success-replace",
        "chunk-success-replace",
        "Nimbus is not an atmospheric probe.",
    )
    successful_payload = StructuralPayload(
        inserted=successful_document,
        deactivated_document_version_id="dv-support",
    )
    successful_event = _event(
        "successful-replace-overlay",
        UpdateKind.REPLACE,
        base,
        inserted=("chunk-success-replace",),
        deactivated=("chunk-support",),
    )
    successful_ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={"successful-replace-overlay": successful_payload},
    )
    successful = M4Application(
        structural=successful_ports,
        runtime=successful_ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-success-replace": ("claim-1",)}),
        ),
        verifier=ScriptedVerifier({"chunk-success-replace": "refute"}),
        observations=successful_ports,
        equality_gates=successful_ports,
        publication=successful_ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(successful_event)

    assert successful.state is EventRunState.SEALED
    assert _state(m4_pipeline_connection) == ("refuted", "contradicted")
    published_versions = m4_pipeline_connection.execute(
        """
        SELECT version.document_version_id
        FROM groundloop_document_version AS version
        JOIN groundloop_epoch AS creator
          ON creator.epoch_id = version.valid_from_epoch
         AND creator.semantic_status = 'sealed'
        WHERE version.document_id = 'doc-support'
          AND version.valid_from_epoch <= %s
          AND (version.valid_to_epoch IS NULL OR %s < version.valid_to_epoch)
        ORDER BY version.document_version_id
        """,
        (successful.epoch_id, successful.epoch_id),
    ).fetchall()
    assert published_versions == [("dv-success-replace",)]
    assert m4_pipeline_connection.execute(
        """
        SELECT epoch.semantic_status, version.valid_to_epoch
        FROM groundloop_document_version AS version
        JOIN groundloop_epoch AS epoch
          ON epoch.epoch_id = version.valid_from_epoch
        WHERE version.document_version_id = 'dv-failed-replace'
        """
    ).fetchone() == ("failed", None)


def test_historical_oracle_is_bound_to_claim_registry_snapshot(
    m4_pipeline_connection: Connection[Any],
) -> None:
    base = _seed_b0(m4_pipeline_connection)
    inserted_document = _inserted(
        "doc-registry", "dv-registry", "chunk-registry", "Nimbus is blue."
    )
    event = _event(
        "registry-insert",
        UpdateKind.INSERT,
        base,
        inserted=("chunk-registry",),
    )
    ports = PostgresM4ApplicationPorts(
        m4_pipeline_connection,
        structural_payloads={
            "registry-insert": StructuralPayload(inserted=inserted_document)
        },
    )
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    result = M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(
            m4_pipeline_connection,
            ScriptedAdmission({"chunk-registry": ("claim-1",)}),
        ),
        verifier=ScriptedVerifier({"chunk-registry": "neutral"}),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_HASH, FRONTIER_HASH, VERIFIER_HASH
        ),
    ).run_event(event)
    assert result.state is EventRunState.SEALED
    before = m4_pipeline_connection.execute(
        """
        SELECT claim_id, status FROM groundloop_m4_claim_state_oracle
        WHERE epoch_id = %s ORDER BY claim_id
        """,
        (result.epoch_id,),
    ).fetchall()

    m4_pipeline_connection.execute(
        "INSERT INTO groundloop_question VALUES ('question-later', 'Later?', %s)",
        (result.epoch_id,),
    )
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_answer_version VALUES (
            'answer-later', 'question-later', 'Later answer.',
            'generator', 'v1', 'prompt-v1', %s
        )
        """,
        (result.epoch_id,),
    )
    m4_pipeline_connection.execute(
        """
        INSERT INTO groundloop_claim VALUES (
            'claim-later', 'answer-later', 'Later claim.',
            'extractor', 'v1', 'prompt-v1', true
        )
        """
    )
    after = m4_pipeline_connection.execute(
        """
        SELECT claim_id, status FROM groundloop_m4_claim_state_oracle
        WHERE epoch_id = %s ORDER BY claim_id
        """,
        (result.epoch_id,),
    ).fetchall()
    assert after == before == [("claim-1", "supported")]
    assert m4_pipeline_connection.execute(
        """
        SELECT count(*) FROM groundloop_object_evaluation
        WHERE epoch_id = %s
        """,
        (result.epoch_id,),
    ).fetchone() == (2,)
