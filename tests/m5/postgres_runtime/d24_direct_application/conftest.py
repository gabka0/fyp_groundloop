"""Live fixtures for the production typed-direct PostgreSQL facade."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, cast

import psycopg
import pytest
from psycopg import Connection
from psycopg.pq import TransactionStatus

from groundloop.domain import ModelStamp, SemanticObservation, SubjectKind
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
)
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    OpenEventReceipt,
    StructuralWithdrawal,
    VerificationResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    ChannelHit,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobKind,
    LogicalJobSpec,
    UpdateKind,
)
from groundloop.m4.pipeline import (
    M4ExecutionMode,
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5RequirementRootDeclaration,
    M5RuntimeMeasurementPort,
    M5TypedApplication,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5LogicalJobSpec,
    M5RequirementWithdrawalPlan,
    M5RunFailureReason,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5TypedDirectAcquisitionReceipt,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.direct_m4 import PostgresM5DirectM4Adapter
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_direct_application import (
    M5PostgresDirectDiscoveryExecution,
    M5PostgresDirectExternalWorkFailure,
    M5PostgresDirectVerifierExecution,
    PostgresM5TypedDirectPreSealPorts,
)
from tests.m5.postgres_runtime.d24_application import (
    conftest as d24_application_fixtures,
)
from tests.m5.postgres_runtime.d24_application.conftest import (
    ApplicationD24Database,
    ControlledDiscovery,
    ControlledMeasurements,
    ControlledVerifier,
    database_snapshot,
    requirement_snapshot,
    select_schema,
    sha,
)
from tests.m5.postgres_runtime.d24_direct.conftest import (
    REGISTRY_ID,
    m4_manifest,
)

_d24_application_db_fixture = d24_application_fixtures.d24_application_db

IMPACT_EXECUTION_HASH = sha("d24-direct-application-impact-execution")
FRONTIER_EXECUTION_HASH = sha("d24-direct-application-frontier-execution")
NEW_DOCUMENT_ID = "d24-direct-application-document"
NEW_DOCUMENT_VERSION_ID = "d24-direct-application-document-v1"
NEW_CHUNK_ID = "d24-direct-application-chunk-v1"
NEW_CHUNK_TEXT = "Nimbus is a controlled direct-application evidence chunk."
ATTEMPT_TIMING = M5RuntimeTiming(
    coordinator_non_db_non_neural_ns=3,
    neural_wall_ns=31,
    postgres_roundtrip_wall_ns=5,
    external_io_wall_ns=7,
    end_to_end_wall_ns=46,
)


@dataclass(slots=True)
class ControlledDirectDiscovery:
    """Deterministic direct provider with explicit success/failure controls."""

    database: DirectApplicationD24Database
    include_pair: bool = False
    fallback_satisfied: bool = True
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False
    direct_terminal_reason: str | None = None
    call_work: M5RuntimeWork = field(
        default_factory=lambda: M5RuntimeWork(
            direct_discovery_call_count=1,
            embedding_model_call_count=1,
        )
    )
    attempt_timing: M5RuntimeTiming | None = ATTEMPT_TIMING
    calls: list[str] = field(default_factory=list)
    connection: Connection[Any] | None = None
    transaction_statuses: list[TransactionStatus] = field(default_factory=list)

    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution:
        connection = (
            self.database.connection if self.connection is None else self.connection
        )
        status = connection.info.transaction_status
        self.transaction_statuses.append(status)
        if status is not TransactionStatus.IDLE:
            raise AssertionError("direct discovery ran inside a database transaction")
        job = acquisition.job
        self.calls.append(job.job_id)
        if self.failure_reason is not None:
            raise M5PostgresDirectExternalWorkFailure(
                self.failure_reason,
                retryable=self.retryable,
                direct_terminal_reason=self.direct_terminal_reason,
                call_work=self.call_work,
                attempt_timing=self.attempt_timing,
                error_hash=sha(
                    f"direct-provider-error:{epoch_id}:{job.job_id}:"
                    f"{self.failure_reason.value}"
                ),
            )
        admitted: tuple[AdmittedPair, ...] = ()
        hits: tuple[ChannelHit, ...] = ()
        if self.include_pair:
            chunk_id = job.target_chunk_version_id
            if chunk_id is None:
                chunk_id = self.database.base.base.chunk_ids[0]
            pair = self.database.pair(job.kind, chunk_id)
            admitted = (
                AdmittedPair(
                    epoch_id,
                    pair,
                    job.candidate_policy_id,
                    1,
                    (AdmissionChannel.VECTOR,),
                    False,
                ),
            )
            hits = (
                ChannelHit(
                    epoch_id,
                    pair,
                    job.candidate_policy_id,
                    AdmissionChannel.VECTOR,
                    1,
                    0.9,
                    sha(f"direct-channel:{epoch_id}:{job.job_id}"),
                ),
            )
        result = DiscoveryResult(
            root_job_id=job.job_id,
            result_artifact_id=f"direct-discovery:{job.job_id}",
            result_artifact_hash=sha(f"direct-discovery:{epoch_id}:{job.job_id}"),
            admitted_pairs=admitted,
            fallback_satisfied=self.fallback_satisfied,
            channel_hits=hits,
        )
        return M5PostgresDirectDiscoveryExecution(
            result,
            M5ExecutionEvidenceDisposition.RETURNED,
            self.call_work,
            self.attempt_timing,
        )


@dataclass(slots=True)
class ControlledDirectVerifier:
    """Deterministic verifier provider for one exact direct child."""

    database: DirectApplicationD24Database
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False
    direct_terminal_reason: str | None = None
    call_work: M5RuntimeWork = field(
        default_factory=lambda: M5RuntimeWork(
            direct_verifier_call_count=1,
            verifier_model_call_count=1,
            verifier_input_token_count=5,
            verifier_output_token_count=1,
        )
    )
    attempt_timing: M5RuntimeTiming | None = ATTEMPT_TIMING
    calls: list[str] = field(default_factory=list)
    connection: Connection[Any] | None = None
    transaction_statuses: list[TransactionStatus] = field(default_factory=list)

    def verify_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectVerifierExecution:
        connection = (
            self.database.connection if self.connection is None else self.connection
        )
        status = connection.info.transaction_status
        self.transaction_statuses.append(status)
        if status is not TransactionStatus.IDLE:
            raise AssertionError("direct verifier ran inside a database transaction")
        job = acquisition.job
        self.calls.append(job.job_id)
        if self.failure_reason is not None:
            raise M5PostgresDirectExternalWorkFailure(
                self.failure_reason,
                retryable=self.retryable,
                direct_terminal_reason=self.direct_terminal_reason,
                call_work=self.call_work,
                attempt_timing=self.attempt_timing,
                error_hash=sha(
                    f"direct-verifier-error:{epoch_id}:{job.job_id}:"
                    f"{self.failure_reason.value}"
                ),
            )
        pair = job.pair
        if pair is None:
            raise AssertionError("controlled verifier requires a pair job")
        observation = SemanticObservation(
            observation_id=f"direct-observation:{job.job_id}",
            subject_kind=SubjectKind.CLAIM,
            subject_id=pair.claim_id,
            chunk_version_id=pair.chunk_version_id,
            task_type="verify_claim_v1",
            support_score=0.9,
            refute_score=0.05,
            neutral_score=0.05,
            producer=ModelStamp("direct-fixture-model", "v1", "v1"),
            input_hash=sha(f"direct-verifier-input:{epoch_id}:{job.job_id}"),
        )
        artifact_hash = sha(f"direct-verifier-result:{epoch_id}:{job.job_id}")
        return M5PostgresDirectVerifierExecution(
            VerificationResult(
                f"direct-verifier:{job.job_id}",
                artifact_hash,
                observation,
            ),
            None,
            M5ExecutionEvidenceDisposition.RETURNED,
            self.call_work,
            self.attempt_timing,
        )


@dataclass(frozen=True, slots=True)
class DirectOpenInputs:
    requirement_withdrawal: M5RequirementWithdrawalPlan
    direct_payload: StructuralPayload
    direct_withdrawal: StructuralWithdrawal
    direct_roots: tuple[LogicalJobSpec, ...]
    direct_scopes: tuple[DiscoveryScope, ...]
    requirement_roots: tuple[M5RequirementRootDeclaration, ...]
    requirement_root_set_hash: str


@dataclass(frozen=True, slots=True)
class BoundDirectApplication:
    connection: Connection[Any]
    store: PostgresM5RuntimeStore
    adapter: PostgresM5DirectM4Adapter
    facade: PostgresM5TypedDirectPreSealPorts
    measurements: M5RuntimeMeasurementPort


@dataclass(frozen=True, slots=True)
class BoundTypedApplication:
    application: M5TypedApplication
    requirement_discovery: ControlledDiscovery
    requirement_verifier: ControlledVerifier
    measurements: Any


@dataclass(frozen=True, slots=True)
class DirectApplicationD24Database:
    """One activated schema with reconnectable direct facade composition."""

    base: ApplicationD24Database
    execution_policy: ApplicationExecutionPolicy
    discovery: ControlledDirectDiscovery
    verifier: ControlledDirectVerifier
    bound: BoundDirectApplication

    @property
    def connection(self) -> Connection[Any]:
        return self.base.connection

    @property
    def facade(self) -> PostgresM5TypedDirectPreSealPorts:
        return self.bound.facade

    def pair(self, kind: JobKind, chunk_id: str) -> Any:
        del kind
        from groundloop.m4.contracts import PairKey

        return PairKey(self.base.base.claim_ids[0], chunk_id)

    def insert_plan(
        self, *, tag: str = "insert", chunk_count: int = 1
    ) -> M5TypedEventPlan:
        if chunk_count not in {1, 2}:
            raise ValueError("the direct fixture supports one or two inserted chunks")
        event_id = f"d24-direct-application-{tag}-event"
        document_id = f"{NEW_DOCUMENT_ID}-{tag}"
        document_version_id = f"{NEW_DOCUMENT_VERSION_ID}-{tag}"
        chunks = tuple(
            ChunkInput(
                f"{NEW_CHUNK_ID}-{tag}-{ordinal}",
                ordinal,
                f"{NEW_CHUNK_TEXT} {tag} {ordinal}",
            )
            for ordinal in range(chunk_count)
        )
        event = InsertDocumentEvent(
            event_id,
            document_id,
            document_version_id,
            sha(f"direct-document-content:{tag}"),
            chunks,
        )
        payload_hash = legacy_event_payload_digest(event)
        direct_plan = DynamicEventPlan(
            update=CorpusUpdateIdentity(
                event_id,
                payload_hash,
                UpdateKind.INSERT,
                self.base.base.epoch_id,
                self.base.manifest.candidate_policy_id,
            ),
            inserted_chunk_version_ids=tuple(
                chunk.chunk_version_id for chunk in chunks
            ),
            deactivated_chunk_version_ids=(),
            registered_claim_ids=(),
            claim_registry_snapshot_id=REGISTRY_ID,
        )
        return M5TypedEventPlan(
            structural_event_id=event_id,
            event=event,
            payload_hash=payload_hash,
            direct_plan=direct_plan,
            candidate_policy_id=self.base.manifest.candidate_policy_id,
            candidate_policy_manifest_hash=self.base.manifest.manifest_hash,
            requirement_registry_snapshot=requirement_snapshot(
                (self.base.published_group,)
            ),
            active_chunk_snapshot=ActiveChunkSnapshot.build(
                (
                    *self.base.active_chunk_snapshot.entries,
                    *(
                        ActiveChunkSnapshotEntry.build(
                            chunk_version_id=chunk.chunk_version_id,
                            chunk_text=chunk.text,
                        )
                        for chunk in chunks
                    ),
                )
            ),
            expected_previous_published_epoch_id=self.base.base.epoch_id,
        )

    def delete_plan(self, *, tag: str = "delete") -> M5TypedEventPlan:
        event_id = f"d24-direct-application-{tag}-event"
        event = DeleteDocumentVersionEvent(
            event_id,
            "d24-application-base-document-v1",
        )
        payload_hash = legacy_event_payload_digest(event)
        direct_plan = DynamicEventPlan(
            update=CorpusUpdateIdentity(
                event_id,
                payload_hash,
                UpdateKind.DELETE,
                self.base.base.epoch_id,
                self.base.manifest.candidate_policy_id,
            ),
            inserted_chunk_version_ids=(),
            deactivated_chunk_version_ids=self.base.base.chunk_ids,
            registered_claim_ids=(),
            claim_registry_snapshot_id=REGISTRY_ID,
        )
        return M5TypedEventPlan(
            structural_event_id=event_id,
            event=event,
            payload_hash=payload_hash,
            direct_plan=direct_plan,
            candidate_policy_id=self.base.manifest.candidate_policy_id,
            candidate_policy_manifest_hash=self.base.manifest.manifest_hash,
            requirement_registry_snapshot=requirement_snapshot(
                (self.base.published_group,)
            ),
            active_chunk_snapshot=ActiveChunkSnapshot.build(()),
            expected_previous_published_epoch_id=self.base.base.epoch_id,
        )

    def bind(
        self,
        connection: Connection[Any],
        *,
        discovery: ControlledDirectDiscovery | None = None,
        verifier: ControlledDirectVerifier | None = None,
        measurements: M5RuntimeMeasurementPort | None = None,
    ) -> BoundDirectApplication:
        store = PostgresM5RuntimeStore(connection)
        ports = PostgresM4ApplicationPorts(
            connection,
            structural_payloads={},
            execution_mode=M4ExecutionMode.MEASURED,
        )
        adapter = PostgresM5DirectM4Adapter(ports)
        selected_discovery = self.discovery if discovery is None else discovery
        selected_verifier = self.verifier if verifier is None else verifier
        selected_measurements = (
            ControlledMeasurements() if measurements is None else measurements
        )
        selected_discovery.connection = connection
        selected_verifier.connection = connection
        facade = PostgresM5TypedDirectPreSealPorts(
            store,
            adapter,
            self.base.manifest,
            self.base.operational_config,
            self.execution_policy,
            selected_discovery,
            selected_verifier,
            measurements=selected_measurements,
        )
        return BoundDirectApplication(
            connection,
            store,
            adapter,
            facade,
            selected_measurements,
        )

    @contextmanager
    def reconnect(self) -> Iterator[BoundDirectApplication]:
        with psycopg.connect(self.base.dsn, autocommit=True) as connection:
            select_schema(connection, self.base.schema_name)
            yield self.bind(connection)

    @contextmanager
    def two_connections(
        self,
    ) -> Iterator[tuple[BoundDirectApplication, BoundDirectApplication]]:
        with self.reconnect() as left, self.reconnect() as right:
            yield left, right


def requirement_roots(
    plan: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
    withdrawal: M5RequirementWithdrawalPlan,
) -> tuple[M5RequirementRootDeclaration, ...]:
    """Independently derive forward fallback plus reverse declarations."""

    keys = withdrawal.fallback_keys
    scopes = [
        M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=key.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=plan.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        for key in keys
    ]
    assert plan.direct_plan is not None
    scopes.extend(
        M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.REVERSE_CHUNK,
            requirement_version_id=None,
            inserted_chunk_version_id=chunk_id,
            candidate_policy_id=plan.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        for chunk_id in plan.direct_plan.inserted_chunk_version_ids
    )
    roots = tuple(
        sorted(
            (
                M5RequirementRootDeclaration(
                    scope,
                    M5LogicalJobSpec.build(
                        structural_event_id=plan.structural_event_id,
                        job_kind=(
                            M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
                            if scope.direction
                            is M5DiscoveryDirection.FORWARD_REQUIREMENT
                            else M5JobKind.REVERSE_REQUIREMENT_DISCOVERY
                        ),
                        manifest=manifest,
                        scope=scope,
                    ),
                )
                for scope in scopes
            ),
            key=lambda declaration: declaration.job.logical_job_id,
        )
    )
    return roots


def open_inputs(
    database: DirectApplicationD24Database,
    plan: M5TypedEventPlan,
    *,
    facade: PostgresM5TypedDirectPreSealPorts | None = None,
) -> DirectOpenInputs:
    selected = database.facade if facade is None else facade
    direct = selected.plan_direct_open(plan)
    withdrawal = selected.plan_exact_requirement_withdrawal(plan)
    roots = requirement_roots(plan, database.base.manifest, withdrawal)
    assert direct.structural_payload is not None
    assert direct.withdrawal is not None
    return DirectOpenInputs(
        withdrawal,
        direct.structural_payload,
        direct.withdrawal,
        tuple(direct.root_jobs),
        tuple(direct.discovery_scopes),
        roots,
        digests.requirement_root_set_digest(
            declaration.job.logical_job_id for declaration in roots
        ),
    )


def open_event(
    database: DirectApplicationD24Database,
    plan: M5TypedEventPlan,
    *,
    facade: PostgresM5TypedDirectPreSealPorts | None = None,
) -> OpenEventReceipt:
    selected = database.facade if facade is None else facade
    inputs = open_inputs(database, plan, facade=selected)
    return selected.open_typed_event_atomically(
        plan,
        inputs.direct_payload,
        inputs.direct_withdrawal,
        inputs.requirement_withdrawal,
        inputs.direct_roots,
        inputs.direct_scopes,
        inputs.requirement_roots,
        inputs.requirement_root_set_hash,
    )


def assemble_typed_application(
    database: DirectApplicationD24Database,
    *,
    facade: PostgresM5TypedDirectPreSealPorts | None = None,
    requirement_discovery: ControlledDiscovery | None = None,
    requirement_verifier: ControlledVerifier | None = None,
    measurements: M5RuntimeMeasurementPort | None = None,
) -> BoundTypedApplication:
    """Build the real top-level application around the direct facade."""

    selected_discovery = (
        ControlledDiscovery(database.base)
        if requirement_discovery is None
        else requirement_discovery
    )
    selected_verifier = (
        ControlledVerifier(database.base)
        if requirement_verifier is None
        else requirement_verifier
    )
    selected_facade = database.facade if facade is None else facade
    application = selected_facade.build_application(
        discovery=selected_discovery,
        verifier=selected_verifier,
        measurements=measurements,
        post_seal_audit=None,
    )
    return BoundTypedApplication(
        application,
        selected_discovery,
        selected_verifier,
        application.measurements,
    )


@pytest.fixture
def d24_direct_application_db(
    request: pytest.FixtureRequest,
) -> Iterator[DirectApplicationD24Database]:
    """Extend the independent R2c schema with the M4 direct publication."""

    base = cast(
        ApplicationD24Database,
        request.getfixturevalue("_d24_application_db_fixture"),
    )
    if base.connection.info.transaction_status is not TransactionStatus.IDLE:
        raise AssertionError("direct fixture requires an idle base connection")
    base.connection.autocommit = True
    bootstrap_m4_publication(base.connection, sealed_epoch_id=base.base.epoch_id)
    ports = PostgresM4ApplicationPorts(
        base.connection,
        structural_payloads={},
        execution_mode=M4ExecutionMode.MEASURED,
    )
    ports.runtime_store.register_candidate_policy(m4_manifest(base.base, base.manifest))
    ports.register_claim_registry_snapshot(REGISTRY_ID, base.base.claim_ids)
    execution_policy = ApplicationExecutionPolicy(
        IMPACT_EXECUTION_HASH,
        FRONTIER_EXECUTION_HASH,
        base.manifest.verifier_execution_spec_hash,
    )
    provisional = object.__new__(DirectApplicationD24Database)
    discovery = ControlledDirectDiscovery(provisional)
    verifier = ControlledDirectVerifier(provisional)
    measurements = ControlledMeasurements()
    store = PostgresM5RuntimeStore(base.connection)
    adapter = PostgresM5DirectM4Adapter(ports)
    facade = PostgresM5TypedDirectPreSealPorts(
        store,
        adapter,
        base.manifest,
        base.operational_config,
        execution_policy,
        discovery,
        verifier,
        measurements=measurements,
    )
    bound = BoundDirectApplication(
        base.connection,
        store,
        adapter,
        facade,
        measurements,
    )
    database = DirectApplicationD24Database(
        base,
        execution_policy,
        discovery,
        verifier,
        bound,
    )
    discovery.database = database
    verifier.database = database
    yield database


__all__ = [
    "ATTEMPT_TIMING",
    "BoundDirectApplication",
    "BoundTypedApplication",
    "ControlledDirectDiscovery",
    "ControlledDirectVerifier",
    "ControlledMeasurements",
    "DirectApplicationD24Database",
    "DirectOpenInputs",
    "assemble_typed_application",
    "database_snapshot",
    "d24_direct_application_db",
    "open_event",
    "open_inputs",
    "requirement_roots",
    "sha",
]
