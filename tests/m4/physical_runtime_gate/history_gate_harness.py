"""Reusable fixtures for adversarial measured M4 event histories.

This module extends the one-event physical gate without changing its original
fixture.  Successful event kernels are guarded against repository hydration,
full runtime reconstruction, deep copies, and inline correctness oracles.  A
full audit is run only after each kernel returns.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest
from physical_gate_harness import (
    CANDIDATE_POLICY_ID,
    FRONTIER_EXECUTION_HASH,
    IMPACT_EXECUTION_HASH,
    REGISTRY_SNAPSHOT_ID,
    VERIFIER_EXECUTION_HASH,
    CountingConnection,
    ExecutionAccounting,
    candidate_policy,
    claim_ids,
    digest,
    seed_published_base,
    seed_unrelated_runtime_jobs,
)
from psycopg import Connection, sql

import groundloop.m4.pipeline as pipeline_module
from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    EventRunResult,
    ExternalWorkFailure,
    M4Application,
    VerificationResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    ChannelHit,
    CorpusUpdateIdentity,
    JobKind,
    LogicalJobSpec,
    PairKey,
    UpdateKind,
)
from groundloop.m4.pipeline import (
    InsertedDocument,
    M4ExecutionMode,
    PersistingAdmissionPort,
    PostgresM4ApplicationPorts,
    StructuralPayload,
)

REQUIRED_CLAIM_ID = "physical-claim-0000"
OPTIONAL_CLAIM_ID = "physical-claim-0001"
REQUIRED_ANSWER_ID = "physical-answer-0000"


def document(
    name: str,
    text: str,
    *,
    document_id: str | None = None,
) -> InsertedDocument:
    document_key = document_id or f"history-document-{name}"
    version_id = f"history-version-{name}"
    chunk_id = f"history-chunk-{name}"
    return InsertedDocument(
        DocumentVersion(version_id, document_key, digest("content", name)),
        (ChunkVersion(chunk_id, version_id, 0, text),),
        source_uri=f"fixture://history/{document_key}",
        authority_class="test",
    )


def compact_event(
    event_id: str,
    kind: UpdateKind,
    previous_epoch_id: int,
    *,
    inserted: tuple[str, ...] = (),
    deactivated: tuple[str, ...] = (),
) -> DynamicEventPlan:
    return DynamicEventPlan(
        CorpusUpdateIdentity(
            event_id,
            digest(
                "history-payload",
                event_id,
                kind.value,
                *inserted,
                *deactivated,
            ),
            kind,
            previous_epoch_id,
            CANDIDATE_POLICY_ID,
        ),
        inserted,
        deactivated,
        (),
        REGISTRY_SNAPSHOT_ID,
    )


@dataclass(slots=True)
class HistoryAdmission:
    claims_by_chunk: Mapping[str, tuple[str, ...]]
    calls: int = 0

    def discover(self, epoch_id: int, root_job: LogicalJobSpec) -> DiscoveryResult:
        self.calls += 1
        if root_job.kind is JobKind.IMPACT_DISCOVERY:
            chunk_id = root_job.target_chunk_version_id
            assert chunk_id is not None
            targets = self.claims_by_chunk.get(chunk_id, ())
        else:
            # This is the explicit empty-close fallback fixture.  It proves
            # that deletion cannot silently skip the required fresh frontier
            # root, not that a production retriever always returns no pairs.
            targets = ()
            chunk_id = None
        pairs = tuple(PairKey(claim_id, cast(str, chunk_id)) for claim_id in targets)
        admitted = tuple(
            AdmittedPair(
                epoch_id,
                pair,
                CANDIDATE_POLICY_ID,
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
                CANDIDATE_POLICY_ID,
                AdmissionChannel.VECTOR,
                rank,
                1.0 - rank / 100.0,
                digest("history-hit", root_job.job_id, pair.claim_id),
            )
            for rank, pair in enumerate(pairs, start=1)
        )
        return DiscoveryResult(
            root_job.job_id,
            f"history-discovery-{root_job.job_id}",
            digest("history-discovery", root_job.job_id, *targets),
            admitted,
            fallback_satisfied=True,
            channel_hits=hits,
        )


@dataclass(slots=True)
class HistoryVerifier:
    labels: Mapping[tuple[str, str], str]
    calls: int = 0

    def verify(self, epoch_id: int, verifier_job: LogicalJobSpec) -> VerificationResult:
        del epoch_id
        self.calls += 1
        pair = verifier_job.pair
        assert pair is not None
        label = self.labels[(pair.claim_id, pair.chunk_version_id)]
        support, refute, neutral = {
            "support": (0.92, 0.03, 0.05),
            "refute": (0.03, 0.92, 0.05),
            "neutral": (0.04, 0.04, 0.92),
        }[label]
        observation = SemanticObservation(
            observation_id=digest(
                "history-observation",
                pair.claim_id,
                pair.chunk_version_id,
                label,
            ),
            subject_kind=SubjectKind.CLAIM,
            subject_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
            task_type="claim-verification-v1",
            support_score=support,
            refute_score=refute,
            neutral_score=neutral,
            producer=ModelStamp("history-verifier", "v1", "prompt-v1"),
            input_hash=digest(
                "history-observation-input", pair.claim_id, pair.chunk_version_id
            ),
        )
        return VerificationResult(
            f"history-verification-{observation.observation_id}",
            digest("history-verification-result", observation.observation_id),
            observation,
        )


@dataclass(slots=True)
class FailingVerifier:
    reason: str = "history worker disconnected"
    calls: int = 0

    def verify(self, epoch_id: int, verifier_job: LogicalJobSpec) -> VerificationResult:
        del epoch_id, verifier_job
        self.calls += 1
        raise ExternalWorkFailure(self.reason)


@dataclass(frozen=True, slots=True)
class GuardedEvent:
    result: EventRunResult
    sql: tuple[str, ...]
    accounting: ExecutionAccounting


def application(
    connection: Connection[Any],
    ports: PostgresM4ApplicationPorts,
    admission: HistoryAdmission,
    verifier: HistoryVerifier | FailingVerifier,
) -> M4Application:
    return M4Application(
        structural=ports,
        runtime=ports,
        admission=PersistingAdmissionPort(connection, admission),
        verifier=verifier,
        observations=ports,
        equality_gates=ports,
        publication=ports,
        execution_policy=ApplicationExecutionPolicy(
            IMPACT_EXECUTION_HASH,
            FRONTIER_EXECUTION_HASH,
            VERIFIER_EXECUTION_HASH,
        ),
    )


def forbid_global_path(*_args: object, **_kwargs: object) -> Any:
    raise AssertionError(
        "measured event used deepcopy, repository hydration, a full runtime "
        "read, or an inline full-recomputation oracle"
    )


@contextmanager
def successful_kernel_guard(
    monkeypatch: pytest.MonkeyPatch,
    ports: PostgresM4ApplicationPorts,
) -> Iterator[None]:
    """Forbid every full-state path excluded from a successful M4 kernel."""

    with monkeypatch.context() as guard:
        guard.setattr(copy, "deepcopy", forbid_global_path)
        guard.setattr(pipeline_module, "deepcopy", forbid_global_path)
        guard.setattr(pipeline_module, "_load_repository_snapshot", forbid_global_path)
        guard.setattr(pipeline_module, "_load_published_repository", forbid_global_path)
        guard.setattr(pipeline_module, "_load_working_repository", forbid_global_path)
        guard.setattr(pipeline_module, "compute_all_states", forbid_global_path)
        guard.setattr(ports.runtime_store, "read_epoch", forbid_global_path)
        guard.setattr(ports.runtime_store, "read_book", forbid_global_path)
        guard.setattr(ports, "_assert_grounding_equality", forbid_global_path)
        yield


def assert_point_bounded_sql(statements: tuple[str, ...]) -> None:
    forbidden = (
        "INSERT INTO groundloop_m4_claim_registry_member",
        "ORDER BY member_ordinal",
        "FROM groundloop_semantic_job WHERE epoch_id = %s ORDER BY job_id",
        "JOIN groundloop_m4_update u USING (epoch_id) ORDER BY e.epoch_id",
    )
    for fragment in forbidden:
        assert not any(fragment in statement for statement in statements)


def run_guarded_event(
    connection: CountingConnection,
    monkeypatch: pytest.MonkeyPatch,
    ports: PostgresM4ApplicationPorts,
    app: M4Application,
    event: DynamicEventPlan,
) -> GuardedEvent:
    connection.reset_statement_trace()
    with successful_kernel_guard(monkeypatch, ports):
        result = app.run_event(event)
    statements = tuple(connection.statement_fingerprints)
    assert_point_bounded_sql(statements)
    accounting = ExecutionAccounting.read(connection, result.epoch_id)
    assert accounting.execution_mode == M4ExecutionMode.MEASURED.value
    assert accounting.inline_grounding_oracle_calls == 0
    return GuardedEvent(result, statements, accounting)


def audit_after_kernel(
    connection: Connection[Any],
    ports: PostgresM4ApplicationPorts,
    epoch_id: int,
) -> None:
    before = ExecutionAccounting.read(connection, epoch_id)
    ports.audit_grounding_exactness(epoch_id)
    book = ports.runtime_store.read_book()
    epoch = next(item for item in book.epochs if item.epoch_id == epoch_id)
    assert epoch.state.value == "sealed"
    # The compact overlay has its own point-read equality check after sealing;
    # full coordination reconstruction above independently validates the job
    # and discovery closure that drives it.
    ports.check_evaluation(epoch_id)
    assert ExecutionAccounting.read(connection, epoch_id) == before


def make_second_claim_optional(connection: Connection[Any]) -> None:
    """Attach claim 1 as optional to answer 0 and remove its empty old answer."""

    # The answer-required-claim invariant is deferred, so the reassignment and
    # removal of the now-empty answer must commit as one transaction.
    with connection.transaction():
        connection.execute(
            "UPDATE groundloop_claim SET answer_version_id = %s, required = false "
            "WHERE claim_id = %s",
            (REQUIRED_ANSWER_ID, OPTIONAL_CLAIM_ID),
        )
        connection.execute(
            "DELETE FROM groundloop_published_answer_state "
            "WHERE answer_version_id = 'physical-answer-0001'"
        )
        connection.execute(
            "DELETE FROM groundloop_answer_state_materialized "
            "WHERE answer_version_id = 'physical-answer-0001'"
        )
        connection.execute(
            "DELETE FROM groundloop_answer_version "
            "WHERE answer_version_id = 'physical-answer-0001'"
        )
        connection.execute(
            "DELETE FROM groundloop_question "
            "WHERE question_id = 'physical-question-0001'"
        )


def prepare_measured_ports(
    connection: CountingConnection,
    *,
    registry_size: int,
    unrelated_job_count: int,
    payloads: Mapping[str, StructuralPayload],
    failure_injector: Any | None = None,
) -> tuple[int, PostgresM4ApplicationPorts]:
    if registry_size < 2:
        raise ValueError("history fixture requires a required and optional claim")
    base = seed_published_base(connection, registry_size=registry_size)
    make_second_claim_optional(connection)
    ports = PostgresM4ApplicationPorts(
        connection,
        structural_payloads=payloads,
        failure_injector=failure_injector,
        execution_mode=M4ExecutionMode.MEASURED,
    )
    ports.runtime_store.register_claim_registry_snapshot(
        REGISTRY_SNAPSHOT_ID, claim_ids(registry_size)
    )
    ports.runtime_store.register_candidate_policy(candidate_policy(registry_size))
    seed_unrelated_runtime_jobs(
        connection, base_epoch=base, job_count=unrelated_job_count
    )
    return base, ports


def table_projection(
    connection: Connection[Any], table_names: tuple[str, ...]
) -> dict[str, tuple[str, ...]]:
    """Read complete row projections, not one-row atomicity proxies."""

    result: dict[str, tuple[str, ...]] = {}
    for table_name in table_names:
        rows = connection.execute(
            sql.SQL(
                "SELECT to_jsonb(row_value)::text FROM {} AS row_value "
                "ORDER BY to_jsonb(row_value)::text"
            ).format(sql.Identifier(table_name))
        ).fetchall()
        result[table_name] = tuple(str(row[0]) for row in rows)
    return result


def all_groundloop_table_names(connection: Connection[Any]) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_type = 'BASE TABLE'
              AND table_name LIKE 'groundloop_%'
            ORDER BY table_name
            """
        ).fetchall()
    )


PUBLICATION_TABLES = (
    "groundloop_m4_publication_head",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_published_observation_currency",
    "groundloop_observation_currency",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_claim_certificate",
    "groundloop_status_delta",
)


def conflicting_event(event: DynamicEventPlan) -> DynamicEventPlan:
    return replace(
        event,
        update=replace(
            event.update,
            payload_hash=digest("history-conflict", event.update.event_id),
        ),
    )
