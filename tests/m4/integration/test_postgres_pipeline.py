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
from groundloop.m4.admission.service import PostgresHybridAdmissionPort
from groundloop.m4.admission.vector import ChunkRoleVector, ReverseVectorSearch
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    EventRunState,
    M4Application,
    VerificationResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    CorpusUpdateIdentity,
    JobKind,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.pipeline import (
    InsertedDocument,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.postgres import record_epoch


def _hash(value: str) -> str:
    return stable_m4_digest(value)


IMPACT_HASH = _hash("m4-pipeline-impact")
FRONTIER_HASH = _hash("m4-pipeline-frontier")
VERIFIER_HASH = _hash("m4-pipeline-verifier")


def _candidate_policy() -> CandidatePolicyManifest:
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
        claim_count=1,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=4,
        frontier_depth=4,
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
        result_hash = _hash(f"admission:{root_job.job_id}:{pairs}")
        return DiscoveryResult(
            root_job.job_id,
            f"admission:{root_job.job_id}",
            result_hash,
            admitted,
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
    with pytest.raises(EventConflictError):
        restarted_application.run_event(conflicting)

    head = m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone()
    assert head == (replaced.epoch_id,)
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
        def discover(
            self, epoch_id: int, root_job: LogicalJobSpec
        ) -> DiscoveryResult:
            del epoch_id, root_job
            from groundloop.m4.application import ExternalWorkFailure

            raise ExternalWorkFailure("injected admission failure")

    application = M4Application(
        structural=ports,
        runtime=ports,
        admission=FailingAdmission(),
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
    assert _state(m4_pipeline_connection) == ("supported", "valid")
    assert m4_pipeline_connection.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head"
    ).fetchone() == (base,)
    assert m4_pipeline_connection.execute(
        "SELECT observation_id FROM groundloop_observation_currency"
    ).fetchall() == [("observation-support",)]


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


@pytest.mark.parametrize(
    "failure_point",
    [
        "publication_currency_promoted",
        "publication_states_written",
        "publication_head_advanced",
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
