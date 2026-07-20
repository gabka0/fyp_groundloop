from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import pytest
from conftest import CommittedM4Schema
from psycopg import Connection, Cursor, sql

from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
    normalized_text_hash,
)
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    JobLease,
    M4Application,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    ChildClosure,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobCompletion,
    JobState,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
    VectorIndexKind,
    stable_m4_digest,
)
from groundloop.m4.persistence import CompletionPlan
from groundloop.m4.pipeline import (
    InsertedDocument,
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.postgres import record_epoch


def _digest(value: str) -> str:
    return stable_m4_digest(value)


IMPACT_EXECUTION = _digest("crash-matrix-impact")
FRONTIER_EXECUTION = _digest("crash-matrix-frontier")
VERIFIER_EXECUTION = _digest("crash-matrix-verifier")


class RequestedExecutionMode(StrEnum):
    AUDIT = "audit"
    MEASURED = "measured"


@pytest.fixture(
    params=(RequestedExecutionMode.AUDIT, RequestedExecutionMode.MEASURED),
    ids=lambda mode: mode.value,
)
def execution_mode(request: pytest.FixtureRequest) -> tuple[str, object | None]:
    """Resolve the coordinator-owned execution mode without redefining it."""

    requested = RequestedExecutionMode(request.param)
    try:
        from groundloop.m4.pipeline import M4ExecutionMode
    except ImportError:
        if requested is RequestedExecutionMode.MEASURED:
            pytest.xfail(
                "M4ExecutionMode.MEASURED is not on this lane baseline; "
                "the coordinator's physical-incrementality lane owns it"
            )
        return requested.value, None
    return requested.value, M4ExecutionMode(requested.value)


@dataclass(slots=True)
class FailureSwitch:
    target: str
    seen: list[str]

    def __call__(self, point: str) -> None:
        self.seen.append(point)
        if point == self.target:
            raise RuntimeError(f"injected:{point}")


@dataclass(frozen=True, slots=True)
class DatabaseProjection:
    """Canonical logical contents of every base table in the test schema."""

    tables: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]


def _snapshot_all_tables(connection: Connection[Any]) -> DatabaseProjection:
    schema_row = connection.execute("SELECT current_schema()").fetchone()
    assert schema_row is not None
    schema_name = str(schema_row[0])
    table_names = tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema_name,),
        ).fetchall()
    )
    projection: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = []
    for table_name in table_names:
        columns = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            ).fetchall()
        )
        rows = tuple(
            str(row[0])
            for row in connection.execute(
                sql.SQL(
                    "SELECT to_jsonb(snapshot_row)::text "
                    "FROM {}.{} AS snapshot_row ORDER BY 1"
                ).format(
                    sql.Identifier(schema_name), sql.Identifier(table_name)
                )
            ).fetchall()
        )
        projection.append((table_name, columns, rows))
    return DatabaseProjection(tuple(projection))


def _candidate_policy() -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id="crash-candidate-v1",
        embedding_model_artifact_id="crash-embedder-v1",
        claim_role_template_hash=_digest("crash-claim-role"),
        chunk_role_template_hash=_digest("crash-chunk-role"),
        vector_method_version="exact-reverse-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_digest("crash-vector-build"),
        vector_search_config_hash=_digest("crash-vector-search"),
        lexical_method_version="postgres-simple-v1",
        lexical_config_hash=_digest("crash-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        claim_registry_snapshot_id="crash-registry-v1",
        claim_count=1,
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=1,
        frontier_depth=1,
        verifier_execution_spec_hash=VERIFIER_EXECUTION,
        decision_policy_version="crash-decision-v1",
    )


def _seed_b0(connection: Connection[Any]) -> int:
    epoch_id, created = record_epoch(
        connection,
        event_id="crash-b0",
        payload_hash=_digest("crash-b0"),
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
        (epoch_id,),
    )
    connection.execute(
        "INSERT INTO groundloop_document VALUES "
        "('crash-doc-b0', 'fixture://crash-b0', 'test')"
    )
    connection.execute(
        "INSERT INTO groundloop_document_version VALUES "
        "('crash-dv-b0', 'crash-doc-b0', %s, %s, NULL)",
        (_digest("crash-b0-content"), epoch_id),
    )
    text = "Nimbus is an atmospheric probe."
    connection.execute(
        """
        INSERT INTO groundloop_chunk_version VALUES (
            'crash-chunk-b0', 'crash-dv-b0', 0, %s, %s,
            'fixed-char-v1', %s, NULL
        )
        """,
        (text, normalized_text_hash(text), epoch_id),
    )
    connection.execute(
        "INSERT INTO groundloop_question VALUES "
        "('crash-question', 'What is Nimbus?', %s)",
        (epoch_id,),
    )
    # The answer/required-claim invariant is a deferred constraint, so these
    # two fixture rows intentionally commit together.  This setup transaction
    # is complete before any crash snapshot is taken.
    with connection.transaction():
        connection.execute(
            """
            INSERT INTO groundloop_answer_version VALUES (
                'crash-answer', 'crash-question',
                'Nimbus is an atmospheric probe.', 'generator', 'v1',
                'prompt-v1', %s
            )
            """,
            (epoch_id,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_claim VALUES (
                'crash-claim', 'crash-answer',
                'Nimbus is an atmospheric probe.', 'extractor', 'v1',
                'prompt-v1', true
            )
            """
        )
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy VALUES (
            'crash-decision-v1', 0.8, 0.8, 'v1', NULL, NULL, %s, NULL
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
            'crash-embedder-v1', 'embedding', 'test', 'embedder', 'v1',
            'v1', 'MIT', %s
        )
        """,
        (_digest("crash-embedder-config"),),
    )
    connection.execute(
        """
        INSERT INTO groundloop_semantic_observation (
            observation_id, subject_kind, subject_id, chunk_version_id,
            task_type, support_score, refute_score, neutral_score, model_id,
            model_version, prompt_version, input_hash, produced_epoch,
            raw_output_hash
        ) VALUES (
            'crash-observation-b0', 'claim', 'crash-claim', 'crash-chunk-b0',
            'claim-verification-v1', 0.9, 0.05, 0.05, 'verifier', 'v1',
            'prompt-v1', %s, %s, %s
        )
        """,
        (_digest("crash-b0-input"), epoch_id, _digest("crash-b0-output")),
    )
    connection.execute(
        """
        INSERT INTO groundloop_observation_currency VALUES (
            'claim', 'crash-claim', 'crash-chunk-b0',
            'claim-verification-v1', 'crash-observation-b0', 1
        )
        """
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim_state_materialized VALUES (
            'crash-claim', 1, 0, 0.9, NULL,
            ARRAY['crash-observation-b0'], ARRAY[]::text[],
            'supported', %s, 1
        )
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_state_materialized VALUES (
            'crash-answer', 1, 1, 0, 0, 0, 'valid', %s, 1
        )
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim_certificate VALUES (
            'crash-claim', 'crash-observation-b0', NULL, %s, 1
        )
        """,
        (epoch_id,),
    )
    bootstrap_m4_publication(connection, sealed_epoch_id=epoch_id)
    return epoch_id


def _inserted_document() -> InsertedDocument:
    text = "Nimbus is not an atmospheric probe."
    return InsertedDocument(
        DocumentVersion(
            "crash-dv-new", "crash-doc-new", _digest("crash-new-content")
        ),
        (ChunkVersion("crash-chunk-new", "crash-dv-new", 0, text),),
        source_uri="fixture://crash-new",
        authority_class="test",
    )


def _event(previous_epoch_id: int) -> DynamicEventPlan:
    return DynamicEventPlan(
        CorpusUpdateIdentity(
            "crash-insert",
            _digest("crash-insert-payload"),
            UpdateKind.INSERT,
            previous_epoch_id,
            "crash-candidate-v1",
        ),
        ("crash-chunk-new",),
        (),
        ("crash-claim",),
        "crash-registry-v1",
    )


def _ports(
    connection: Connection[Any],
    failure_switch: FailureSwitch,
    execution_mode: object | None,
) -> PostgresM4ApplicationPorts:
    kwargs: dict[str, object] = {
        "structural_payloads": {
            "crash-insert": StructuralPayload(inserted=_inserted_document())
        },
        "failure_injector": failure_switch,
    }
    if execution_mode is not None:
        kwargs["execution_mode"] = execution_mode
    return PostgresM4ApplicationPorts(connection, **kwargs)


def _application(ports: PostgresM4ApplicationPorts) -> M4Application:
    return M4Application(
        structural=ports,
        runtime=ports,
        admission=_UnusedPort(),
        verifier=_UnusedPort(),
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_EXECUTION, FRONTIER_EXECUTION, VERIFIER_EXECUTION
        ),
    )


class _UnusedPort:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"unexpected external port call: {name}")


@dataclass(frozen=True, slots=True)
class PreparedExpansion:
    ports: PostgresM4ApplicationPorts
    application: M4Application
    event: DynamicEventPlan
    epoch_id: int
    root: LogicalJobSpec
    lease: JobLease
    discovery: DiscoveryResult
    completion: JobCompletion
    child: LogicalJobSpec


def _prepare_expansion(
    connection: Connection[Any],
    failure_switch: FailureSwitch,
    execution_mode: object | None,
) -> PreparedExpansion:
    base = _seed_b0(connection)
    ports = _ports(connection, failure_switch, execution_mode)
    ports.runtime_store.register_candidate_policy(_candidate_policy())
    application = _application(ports)
    event = _event(base)
    withdrawal = ports.plan_exact_withdrawal(event)
    roots = application._root_jobs(event, withdrawal.fallback_claim_ids)
    assert len(roots) == 1
    root = roots[0]
    scopes = (
        DiscoveryScope(
            root.job_id,
            event.claim_registry_snapshot_id,
            event.registered_claim_ids,
        ),
    )
    opened = ports.open_event(event, withdrawal, roots, scopes)
    lease = ports.acquire_job(opened.epoch_id, root)
    pair = PairKey("crash-claim", "crash-chunk-new")
    admitted = AdmittedPair(
        opened.epoch_id,
        pair,
        root.candidate_policy_id,
        1,
        (AdmissionChannel.VECTOR,),
        False,
    )
    channel_hit = ChannelHit(
        opened.epoch_id,
        pair,
        root.candidate_policy_id,
        AdmissionChannel.VECTOR,
        1,
        0.75,
        _digest("crash-channel-hit"),
    )
    discovery = DiscoveryResult(
        root.job_id,
        "crash-discovery-artifact",
        _digest("crash-discovery-result"),
        (admitted,),
        channel_hits=(channel_hit,),
    )
    child = application._verifier_job(event, root, pair)
    closure = ChildClosure.build(
        parent_job_id=root.job_id,
        result_artifact_hash=discovery.result_artifact_hash,
        child_job_ids=(child.job_id,),
    )
    completion = JobCompletion.build(
        job_id=root.job_id,
        payload_hash=root.payload_hash,
        execution_spec_hash=root.execution_spec_hash,
        result_artifact_id=discovery.result_artifact_id,
        result_artifact_hash=discovery.result_artifact_hash,
        terminal_state=JobState.COMPLETED_ACTIVE,
        child_closure=closure,
    )
    return PreparedExpansion(
        ports,
        application,
        event,
        opened.epoch_id,
        root,
        lease,
        discovery,
        completion,
        child,
    )


@dataclass(frozen=True, slots=True)
class PreparedVerifier:
    expansion: PreparedExpansion
    lease: JobLease
    observation: SemanticObservation
    completion: JobCompletion


def _prepare_verifier(
    connection: Connection[Any],
    failure_switch: FailureSwitch,
    execution_mode: object | None,
) -> PreparedVerifier:
    prepared = _prepare_expansion(connection, failure_switch, execution_mode)
    prepared.ports.complete_expansion(
        prepared.epoch_id,
        prepared.lease,
        prepared.discovery,
        prepared.completion,
        (prepared.child,),
    )
    lease = prepared.ports.acquire_job(prepared.epoch_id, prepared.child)
    observation = SemanticObservation(
        observation_id=_digest("crash-new-observation"),
        subject_kind=SubjectKind.CLAIM,
        subject_id="crash-claim",
        chunk_version_id="crash-chunk-new",
        task_type="claim-verification-v1",
        support_score=0.05,
        refute_score=0.9,
        neutral_score=0.05,
        producer=ModelStamp("crash-verifier", "v1", "prompt-v1"),
        input_hash=_digest("crash-new-observation-input"),
    )
    completion = JobCompletion.build(
        job_id=prepared.child.job_id,
        payload_hash=prepared.child.payload_hash,
        execution_spec_hash=prepared.child.execution_spec_hash,
        result_artifact_id="crash-verification-artifact",
        result_artifact_hash=_digest("crash-verification-result"),
        terminal_state=JobState.COMPLETED_ACTIVE,
    )
    return PreparedVerifier(prepared, lease, observation, completion)


def _prepare_publication(
    connection: Connection[Any],
    failure_switch: FailureSwitch,
    execution_mode: object | None,
) -> PreparedVerifier:
    prepared = _prepare_verifier(connection, failure_switch, execution_mode)
    prepared.expansion.ports.complete_verifier_atomically(
        prepared.expansion.epoch_id,
        prepared.lease,
        prepared.expansion.child,
        prepared.completion,
        prepared.observation,
        make_effective=True,
    )
    snapshot = prepared.expansion.ports.sealing_snapshot(
        prepared.expansion.epoch_id
    )
    assert snapshot.ready
    return prepared


def _assert_rollback_after_reconnect(
    harness: CommittedM4Schema,
    before: DatabaseProjection,
) -> None:
    with harness.connect() as reconnected:
        assert _snapshot_all_tables(reconnected) == before


STRUCTURAL_POINTS = ("open_structural_written", "open_rows_written")
STORE_COMPLETION_POINTS = (
    "completion_children_written",
    "completion_parent_written",
)
EXPANSION_POINTS = (
    "expansion_discovery_persisted",
    "expansion_runtime_completed",
    "expansion_evaluation_synced",
)
VERIFIER_POINTS = (
    "verifier_runtime_completed",
    "verifier_observation_archived",
    "verifier_overlay_written",
    "verifier_state_written",
)
PUBLICATION_POINTS = (
    "publication_store_seal_checked",
    "publication_structure_promoted",
    "publication_currency_promoted",
    "publication_states_written",
    "publication_head_advanced",
    "publication_evaluation_promoted",
    "publication_store_seal_publication_written",
    "publication_store_seal_epoch_written",
)


@pytest.mark.parametrize("failure_point", STRUCTURAL_POINTS)
def test_structural_open_is_fully_atomic_after_reconnect(
    committed_m4_schema: CommittedM4Schema,
    execution_mode: tuple[str, object | None],
    failure_point: str,
) -> None:
    _mode_name, mode = execution_mode
    switch = FailureSwitch(failure_point, [])
    with committed_m4_schema.connect() as connection:
        base = _seed_b0(connection)
        ports = _ports(connection, switch, mode)
        ports.runtime_store.register_candidate_policy(_candidate_policy())
        event = _event(base)
        application = _application(ports)
        roots = application._root_jobs(event, ())
        root = roots[0]
        scopes = (
            DiscoveryScope(
                root.job_id,
                event.claim_registry_snapshot_id,
                event.registered_claim_ids,
            ),
        )

        def structural_action(cursor: Cursor[Any], epoch_id: int) -> None:
            cursor.execute(
                """
                INSERT INTO groundloop_document
                    (document_id, source_uri, authority_class)
                VALUES ('crash-doc-new', 'fixture://direct', 'test')
                """
            )
            cursor.execute(
                """
                INSERT INTO groundloop_document_version VALUES (
                    'crash-dv-new', 'crash-doc-new', %s, %s, NULL
                )
                """,
                (_digest("crash-new-content"), epoch_id),
            )
            text = "Nimbus is not an atmospheric probe."
            cursor.execute(
                """
                INSERT INTO groundloop_chunk_version VALUES (
                    'crash-chunk-new', 'crash-dv-new', 0, %s, %s,
                    'fixed-char-v1', %s, NULL
                )
                """,
                (text, normalized_text_hash(text), epoch_id),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_m4_claim_registry_member (
                    claim_registry_snapshot_id, claim_id, member_ordinal
                ) VALUES ('crash-registry-v1', 'crash-claim', 0)
                """
            )
            assert epoch_id > base

        before = _snapshot_all_tables(connection)
        with pytest.raises(RuntimeError, match=failure_point):
            ports.runtime_store.open_epoch(
                event.update,
                roots,
                scopes,
                registry_snapshot_id=event.claim_registry_snapshot_id,
                structural_action=structural_action,
                failure_injector=switch,
            )
        assert failure_point in switch.seen
    _assert_rollback_after_reconnect(committed_m4_schema, before)


@pytest.mark.parametrize("failure_point", STORE_COMPLETION_POINTS)
def test_runtime_child_closure_is_fully_atomic_after_reconnect(
    committed_m4_schema: CommittedM4Schema,
    execution_mode: tuple[str, object | None],
    failure_point: str,
) -> None:
    _mode_name, mode = execution_mode
    switch = FailureSwitch(failure_point, [])
    with committed_m4_schema.connect() as connection:
        prepared = _prepare_expansion(connection, switch, mode)
        epoch = prepared.ports.runtime_store.read_epoch(prepared.epoch_id)
        before = _snapshot_all_tables(connection)
        with pytest.raises(RuntimeError, match=failure_point):
            prepared.ports.runtime_store.complete(
                CompletionPlan(
                    prepared.epoch_id,
                    epoch.revision,
                    prepared.completion,
                    (prepared.child,),
                ),
                active_chunk_ids=frozenset({"crash-chunk-new"}),
                attempt_id=prepared.lease.attempt_id or "",
                lease_token_hash=prepared.lease.lease_token_hash or "",
                lease_expected_revision=prepared.lease.expected_revision or 0,
                failure_injector=switch,
            )
        assert failure_point in switch.seen
    _assert_rollback_after_reconnect(committed_m4_schema, before)


@pytest.mark.parametrize("failure_point", EXPANSION_POINTS)
def test_discovery_and_expansion_are_fully_atomic_after_reconnect(
    committed_m4_schema: CommittedM4Schema,
    execution_mode: tuple[str, object | None],
    failure_point: str,
) -> None:
    _mode_name, mode = execution_mode
    switch = FailureSwitch(failure_point, [])
    with committed_m4_schema.connect() as connection:
        prepared = _prepare_expansion(connection, switch, mode)
        before = _snapshot_all_tables(connection)
        with pytest.raises(RuntimeError, match=failure_point):
            prepared.ports.complete_expansion(
                prepared.epoch_id,
                prepared.lease,
                prepared.discovery,
                prepared.completion,
                (prepared.child,),
            )
        assert failure_point in switch.seen
    _assert_rollback_after_reconnect(committed_m4_schema, before)


@pytest.mark.parametrize("failure_point", VERIFIER_POINTS)
def test_verifier_completion_is_fully_atomic_after_reconnect(
    committed_m4_schema: CommittedM4Schema,
    execution_mode: tuple[str, object | None],
    failure_point: str,
) -> None:
    _mode_name, mode = execution_mode
    switch = FailureSwitch(failure_point, [])
    with committed_m4_schema.connect() as connection:
        prepared = _prepare_verifier(connection, switch, mode)
        before = _snapshot_all_tables(connection)
        with pytest.raises(RuntimeError, match=failure_point):
            prepared.expansion.ports.complete_verifier_atomically(
                prepared.expansion.epoch_id,
                prepared.lease,
                prepared.expansion.child,
                prepared.completion,
                prepared.observation,
                make_effective=True,
            )
        assert failure_point in switch.seen
    _assert_rollback_after_reconnect(committed_m4_schema, before)


@pytest.mark.parametrize("failure_point", PUBLICATION_POINTS)
def test_publication_and_seal_are_fully_atomic_after_reconnect(
    committed_m4_schema: CommittedM4Schema,
    execution_mode: tuple[str, object | None],
    failure_point: str,
) -> None:
    _mode_name, mode = execution_mode
    switch = FailureSwitch(failure_point, [])
    with committed_m4_schema.connect() as connection:
        prepared = _prepare_publication(connection, switch, mode)
        ports = prepared.expansion.ports
        snapshot = ports.sealing_snapshot(prepared.expansion.epoch_id)
        assert snapshot.ready
        before = _snapshot_all_tables(connection)
        with pytest.raises(RuntimeError, match=failure_point):
            ports.request_seal(
                prepared.expansion.epoch_id,
                snapshot.revision,
                prepared.expansion.event.update,
            )
        assert failure_point in switch.seen
    _assert_rollback_after_reconnect(committed_m4_schema, before)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "contract request: production structural open exposes only two coarse "
        "runtime-store hooks, not the per-step pipeline hooks required by the "
        "M4.1 acceptance matrix"
    ),
)
def test_pipeline_structural_open_exposes_required_per_step_hooks(
    committed_m4_schema: CommittedM4Schema,
) -> None:
    required = {
        "structural_registry_written",
        "structural_versions_written",
        "structural_withdrawal_written",
        "structural_working_states_written",
        "structural_evaluation_written",
    }
    switch = FailureSwitch("never", [])
    with committed_m4_schema.connect() as connection:
        base = _seed_b0(connection)
        ports = _ports(connection, switch, None)
        ports.runtime_store.register_candidate_policy(_candidate_policy())
        event = _event(base)
        application = _application(ports)
        withdrawal = ports.plan_exact_withdrawal(event)
        roots = application._root_jobs(event, withdrawal.fallback_claim_ids)
        scopes = (
            DiscoveryScope(
                roots[0].job_id,
                event.claim_registry_snapshot_id,
                event.registered_claim_ids,
            ),
        )
        ports.open_event(event, withdrawal, roots, scopes)
    assert required <= set(switch.seen)
