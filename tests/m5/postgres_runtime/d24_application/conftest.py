"""Independent live fixtures for the D24 group/requirement bridge.

This module intentionally installs its own unique PostgreSQL schema and uses
only public production types and the shared M5 PostgreSQL seed builders.  It
does not import helpers from either earlier nested D24 lane.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.domain import DecisionPolicy, SubjectKind
from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.events import (
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    m5_event_payload_digest,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.application import (
    M5DiscoveryExecution,
    M5ExternalWorkFailure,
    M5RequirementDiscoveryPort,
    M5RequirementRootDeclaration,
    M5RequirementVerifierPort,
    M5RuntimeMeasurementPort,
    M5RuntimePersistencePort,
    M5RuntimeReadPort,
    M5TerminalInvocationTelemetry,
    M5TypedApplication,
    M5VerifierExecution,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AttemptOutput,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5JobLease,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementPairInput,
    M5RequirementVerifierArtifact,
    M5RunFailureReason,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeWork,
    M5TransitionTimingAnchor,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
)
from groundloop.m5.runtime.frontier import build_rank_interleaved_discovery_result
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_application import (
    PostgresM5GroupRequirementPreSealPorts,
)
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m5.postgres.helpers import (
    SeededBase,
    insert_published_group,
    install_test_activation_barrier,
    make_group,
    seed_base,
)

ACCEPTED_016_LEDGER = (
    "m5-runtime-recovery-schema-bundle-v1",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
    "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd",
)
CHUNK_TEXTS = (
    "Aster is the first controlled evidence chunk.",
    "Beryl is the second controlled evidence chunk.",
)
EMBEDDING_ARTIFACT_ID = "d24-application-embedding"
CHUNKER_ARTIFACT_ID = "d24-application-chunker"
VERIFIER_MODEL_ARTIFACT_ID = "d24-application-verifier-model"
VERIFIER_MODEL_ID = "d24-application-verifier"
VERIFIER_PROMPT_ARTIFACT_ID = "d24-application-verifier-prompt"


def sha(value: str) -> str:
    """Return the lowercase SHA-256 used by controlled fixture artifacts."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def database_url() -> str:
    """Read the guarded live-test DSN without exposing it in test output."""

    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def select_schema(connection: Connection[Any], schema_name: str) -> None:
    """Bind one connection to the fixture schema and commit that setting."""

    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    connection.commit()
    assert connection.execute("SELECT current_schema()").fetchone() == (schema_name,)


def assert_accepted_016_ledger(connection: Connection[Any]) -> None:
    """Check the accepted literal five-field migration-016 identity."""

    rows = connection.execute(
        """
        SELECT bundle_id, btrim(bundle_sha256), btrim(migration_sha256),
               btrim(oracle_sha256), btrim(prerequisite_sha256)
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = 'm5-runtime-recovery-schema-bundle-v1'
        """
    ).fetchall()
    assert rows == [ACCEPTED_016_LEDGER]


def candidate_manifest(
    base: SeededBase, *, variant: str = "primary"
) -> M5CandidatePolicyManifest:
    """Build one immutable policy manifest; only the primary is registered."""

    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id=EMBEDDING_ARTIFACT_ID,
        requirement_role_template_hash=sha(f"d24-app-requirement-role:{variant}"),
        chunk_role_template_hash=sha(f"d24-app-chunk-role:{variant}"),
        vector_method_version="d24-application-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=sha(f"d24-app-vector-build:{variant}"),
        vector_search_config_hash=sha(f"d24-app-vector-search:{variant}"),
        lexical_method_version="d24-application-lexical-v1",
        lexical_config_hash=sha(f"d24-app-lexical:{variant}"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2 if variant == "primary" else 3,
        verifier_execution_spec_hash=sha(f"d24-app-verifier-execution:{variant}"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


def requirement_snapshot(
    groups: tuple[EvidenceGroupVersion, ...],
) -> RequirementRegistrySnapshot:
    """Build the frozen ID-sorted requirement registry for a plan."""

    return RequirementRegistrySnapshot.build(
        tuple(
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id=requirement.requirement_version_id,
                group_version_id=group.group_version_id,
                group_family_id=group.group_family_id,
                owner_claim_id=group.owner_claim_id,
                requirement_text=requirement.requirement_text,
            )
            for group in groups
            for requirement in group.requirements
        )
    )


def requirement_roots(
    plan: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
) -> tuple[M5RequirementRootDeclaration, ...]:
    """Derive the public group-only forward declarations independently."""

    if isinstance(plan.event, RegisterGroupEvent):
        requirements = plan.event.group.requirements
    elif isinstance(plan.event, ReplaceGroupEvent):
        requirements = plan.event.successor.requirements
    elif isinstance(plan.event, RetireGroupEvent):
        requirements = ()
    else:
        raise AssertionError("the R2c fixture derives group-lifecycle roots only")
    declarations: list[M5RequirementRootDeclaration] = []
    for requirement in requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=plan.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        scope.validate_snapshots(
            plan.requirement_registry_snapshot,
            plan.active_chunk_snapshot,
        )
        declarations.append(
            M5RequirementRootDeclaration(
                scope,
                M5LogicalJobSpec.build(
                    structural_event_id=plan.structural_event_id,
                    job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                    manifest=manifest,
                    scope=scope,
                ),
            )
        )
    return tuple(
        sorted(declarations, key=lambda declaration: declaration.job.logical_job_id)
    )


def requirement_root_set_hash(
    roots: tuple[M5RequirementRootDeclaration, ...],
) -> str:
    """Digest one canonical declaration tuple using the accepted recipe."""

    return runtime_digests.requirement_root_set_digest(
        declaration.job.logical_job_id for declaration in roots
    )


def database_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Capture all table rows/xmins and sequence state for zero-write proofs."""

    with connection.transaction():
        table_names = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = current_schema()
                ORDER BY tablename COLLATE "C"
                """
            ).fetchall()
        )
        image: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        for table_name in table_names:
            rows = connection.execute(
                sql.SQL(
                    "SELECT xmin::text, row_to_json(stored)::text "
                    "FROM {} AS stored "
                    'ORDER BY row_to_json(stored)::text COLLATE "C", xmin::text'
                ).format(sql.Identifier(table_name))
            ).fetchall()
            image.append(
                (
                    table_name,
                    tuple((str(xmin), str(payload)) for xmin, payload in rows),
                )
            )
        sequence_names = tuple(
            str(row[0])
            for row in connection.execute(
                """
                SELECT sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema = current_schema()
                ORDER BY sequence_name COLLATE "C"
                """
            ).fetchall()
        )
        for sequence_name in sequence_names:
            row = connection.execute(
                sql.SQL("SELECT last_value::text, is_called::text FROM {}").format(
                    sql.Identifier(sequence_name)
                )
            ).fetchone()
            assert row is not None
            image.append(
                (
                    f"sequence:{sequence_name}",
                    ((str(row[0]), str(row[1])),),
                )
            )
    return tuple(image)


def relation_rows(
    connection: Connection[Any],
    relation: str,
    columns: tuple[str, ...],
    *,
    order_by: tuple[str, ...] = (),
) -> tuple[tuple[object, ...], ...]:
    """Read an explicitly named relation surface with safe identifiers."""

    if not columns:
        raise ValueError("relation_rows requires at least one column")
    ordering = order_by or columns
    query = sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.Identifier(relation),
        sql.SQL(", ").join(sql.Identifier(column) for column in ordering),
    )
    with connection.transaction():
        return tuple(tuple(row) for row in connection.execute(query).fetchall())


@dataclass(frozen=True, slots=True)
class ChunkFixture:
    document_version_id: str
    chunk_index: int
    chunk_text: str
    chunk_text_hash: str


@dataclass(frozen=True, slots=True)
class BoundApplicationPorts:
    """One fresh connection and its same-connection store/facade pair."""

    connection: Connection[Any]
    store: PostgresM5RuntimeStore
    facade: PostgresM5GroupRequirementPreSealPorts


@dataclass(frozen=True, slots=True)
class ApplicationD24Database:
    """Reconnectable unique-schema context for the R2c live tests."""

    dsn: str
    schema_name: str
    connection: Connection[Any]
    base: SeededBase
    published_group: EvidenceGroupVersion
    manifest: M5CandidatePolicyManifest
    operational_config: M5RuntimeOperationalConfig
    active_chunk_snapshot: ActiveChunkSnapshot
    chunks: dict[str, ChunkFixture]
    store: PostgresM5RuntimeStore
    facade: PostgresM5GroupRequirementPreSealPorts

    def bind(self, connection: Connection[Any]) -> BoundApplicationPorts:
        """Build the concrete store and facade for one already-configured connection."""

        store = PostgresM5RuntimeStore(connection)
        return BoundApplicationPorts(
            connection,
            store,
            PostgresM5GroupRequirementPreSealPorts(
                store,
                self.manifest,
                self.operational_config,
            ),
        )

    @contextmanager
    def reconnect(self) -> Iterator[BoundApplicationPorts]:
        """Yield a genuinely fresh connection bound to this unique schema."""

        with psycopg.connect(self.dsn) as connection:
            select_schema(connection, self.schema_name)
            assert_accepted_016_ledger(connection)
            connection.commit()
            with self.connection.transaction():
                primary_pid = self.connection.execute(
                    "SELECT pg_backend_pid()"
                ).fetchone()
            with connection.transaction():
                reconnect_pid = connection.execute("SELECT pg_backend_pid()").fetchone()
            assert primary_pid is not None and reconnect_pid is not None
            assert primary_pid != reconnect_pid
            yield self.bind(connection)

    def alternate_manifest(self) -> M5CandidatePolicyManifest:
        return candidate_manifest(self.base, variant="alternate")

    def register_plan(
        self,
        *,
        tag: str = "register",
        requirement_count: int = 1,
        manifest: M5CandidatePolicyManifest | None = None,
    ) -> M5TypedEventPlan:
        if requirement_count not in {1, 2}:
            raise ValueError("the controlled fixture supports one or two requirements")
        selected = manifest or self.manifest
        group = make_group(
            group_id=f"d24-app-{tag}-group-v1",
            family_id=f"d24-app-{tag}-family",
            claim_id=self.base.claim_ids[0],
            texts=tuple(
                f"controlled requirement {ordinal} for {tag}"
                for ordinal in range(requirement_count)
            ),
            source_id="d24-r2c-controlled-register",
        )
        event = RegisterGroupEvent(f"d24-app-{tag}-event", group)
        return M5TypedEventPlan(
            structural_event_id=event.event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=selected.candidate_policy_id,
            candidate_policy_manifest_hash=selected.manifest_hash,
            requirement_registry_snapshot=requirement_snapshot(
                (self.published_group, group)
            ),
            active_chunk_snapshot=self.active_chunk_snapshot,
            expected_previous_published_epoch_id=self.base.epoch_id,
        )

    def replace_plan(
        self,
        *,
        tag: str = "replace",
        requirement_count: int = 1,
    ) -> M5TypedEventPlan:
        if requirement_count not in {1, 2}:
            raise ValueError("the controlled fixture supports one or two requirements")
        predecessor_ids = tuple(
            requirement.requirement_version_id
            for requirement in self.published_group.requirements
        )
        predecessors = tuple(
            predecessor_ids[min(index, len(predecessor_ids) - 1)]
            for index in range(requirement_count)
        )
        successor = make_group(
            group_id=f"d24-app-{tag}-group-v2",
            family_id=self.published_group.group_family_id,
            claim_id=self.published_group.owner_claim_id,
            texts=tuple(
                f"replacement requirement {ordinal} for {tag}"
                for ordinal in range(requirement_count)
            ),
            predecessors=predecessors,
            supersedes_group_id=self.published_group.group_version_id,
            source_id="d24-r2c-controlled-replace",
        )
        event = ReplaceGroupEvent(
            f"d24-app-{tag}-event",
            self.published_group.group_version_id,
            successor,
        )
        return M5TypedEventPlan(
            structural_event_id=event.event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=self.manifest.candidate_policy_id,
            candidate_policy_manifest_hash=self.manifest.manifest_hash,
            requirement_registry_snapshot=requirement_snapshot((successor,)),
            active_chunk_snapshot=self.active_chunk_snapshot,
            expected_previous_published_epoch_id=self.base.epoch_id,
        )

    def retire_plan(self, *, tag: str = "retire") -> M5TypedEventPlan:
        event = RetireGroupEvent(
            f"d24-app-{tag}-event",
            self.published_group.group_version_id,
        )
        return M5TypedEventPlan(
            structural_event_id=event.event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=self.manifest.candidate_policy_id,
            candidate_policy_manifest_hash=self.manifest.manifest_hash,
            requirement_registry_snapshot=RequirementRegistrySnapshot.build(()),
            active_chunk_snapshot=self.active_chunk_snapshot,
            expected_previous_published_epoch_id=self.base.epoch_id,
        )


class CombinedRuntimePorts(M5RuntimePersistencePort, M5RuntimeReadPort, Protocol):
    """Static intersection used only by explicit test-local runtime wrappers."""


def assemble_application(
    database: ApplicationD24Database,
    discovery: M5RequirementDiscoveryPort,
    verifier: M5RequirementVerifierPort,
    measurements: M5RuntimeMeasurementPort,
    *,
    runtime_override: CombinedRuntimePorts | None = None,
    bound: BoundApplicationPorts | None = None,
) -> M5TypedApplication:
    """Assemble the unchanged coordinator with one explicit concrete facade."""

    facade = database.facade if bound is None else bound.facade
    if runtime_override is None:
        return facade.build_application(
            discovery=discovery,
            verifier=verifier,
            measurements=measurements,
            post_seal_audit=None,
        )
    return M5TypedApplication(
        policies=facade,
        structural=facade,
        direct=facade,
        runtime=runtime_override,
        runtime_reads=runtime_override,
        discovery=discovery,
        verifier=verifier,
        measurements=measurements,
        post_seal_audit=None,
    )


def build_discovery_execution(
    database: ApplicationD24Database,
    *,
    epoch_id: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    manifest: M5CandidatePolicyManifest,
    event: M5TypedEventPlan,
    include_pair: bool,
    call_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> M5DiscoveryExecution:
    """Build one fully bound controlled discovery return."""

    if lease.attempt is None or job.scope_contract_digest is None:
        raise AssertionError("controlled discovery requires a dispatched root")
    requirement_ids = tuple(
        entry.requirement_version_id
        for entry in event.requirement_registry_snapshot.entries
        if M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=entry.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                event.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                event.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        ).scope_contract_digest
        == job.scope_contract_digest
    )
    if len(requirement_ids) != 1:
        raise AssertionError("R2c discovery supports a forward requirement root only")
    requirement_id = requirement_ids[0]
    hits: tuple[M5RequirementChannelHit, ...]
    if include_pair:
        pair = SemanticPairKey(
            SubjectKind.REQUIREMENT,
            requirement_id,
            database.base.chunk_ids[0],
        )
        hits = (
            M5RequirementChannelHit.build(
                epoch_id=epoch_id,
                root_job_id=job.logical_job_id,
                scope_contract_digest=job.scope_contract_digest,
                pair=pair,
                candidate_policy_id=manifest.candidate_policy_id,
                channel=M5RequirementAdmissionChannel.VECTOR,
                rank=1,
                score=0.95,
                channel_artifact_hash=sha(
                    f"d24-app-hit:{event.structural_event_id}:{job.logical_job_id}"
                ),
            ),
        )
    else:
        hits = ()
    result = build_rank_interleaved_discovery_result(
        hits=hits,
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        manifest=manifest,
        root_job_id=job.logical_job_id,
        scope_contract_digest=job.scope_contract_digest,
        eligible_snapshot_exhausted=True,
    )
    output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    return M5DiscoveryExecution(
        result,
        output,
        True,
        M5ExecutionEvidenceDisposition.RETURNED,
        call_work,
        attempt_timing,
    )


def build_verifier_execution(
    database: ApplicationD24Database,
    *,
    epoch_id: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    manifest: M5CandidatePolicyManifest,
    event: M5TypedEventPlan,
    call_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> M5VerifierExecution:
    """Build one typed verifier result from frozen plan and chunk inputs."""

    if lease.attempt is None or job.pair is None or job.scope_contract_digest is None:
        raise AssertionError("controlled verifier requires one dispatched pair job")
    entry = event.requirement_registry_snapshot.member(job.pair.subject_id)
    chunk = database.chunks[job.pair.chunk_version_id]
    pair_input = M5RequirementPairInput.build(
        pair=job.pair,
        scope_contract_digest=job.scope_contract_digest,
        candidate_policy_id=manifest.candidate_policy_id,
        owner_claim_id=entry.owner_claim_id,
        group_version_id=entry.group_version_id,
        group_family_id=entry.group_family_id,
        requirement_ordinal=next(
            requirement.ordinal
            for concrete_event in (event.event,)
            for group in (
                concrete_event.group
                if isinstance(concrete_event, RegisterGroupEvent)
                else concrete_event.successor
                if isinstance(concrete_event, ReplaceGroupEvent)
                else database.published_group,
            )
            for requirement in group.requirements
            if requirement.requirement_version_id == entry.requirement_version_id
        ),
        requirement_text=entry.normalized_requirement_text,
        document_version_id=chunk.document_version_id,
        chunk_index=chunk.chunk_index,
        chunk_text=chunk.chunk_text,
        stored_chunk_text_hash=chunk.chunk_text_hash,
        chunker_artifact_id=CHUNKER_ARTIFACT_ID,
    )
    policy = DecisionPolicy(
        database.base.policy_version,
        0.8,
        0.8,
        "v1",
    )
    artifact = M5RequirementVerifierArtifact.build_checked(
        pair=job.pair,
        pair_input_hash=pair_input.pair_input_hash,
        execution_spec_hash=job.execution_spec_hash,
        model_artifact_id=VERIFIER_MODEL_ARTIFACT_ID,
        model_id=VERIFIER_MODEL_ID,
        model_revision="v1",
        prompt_artifact_id=VERIFIER_PROMPT_ARTIFACT_ID,
        prompt_version="v1",
        calibration_version="uncalibrated-v1",
        calibration_artifact_hash=None,
        temperature=1.0,
        decision_policy=policy,
        support_score=0.9,
        refute_score=0.05,
        neutral_score=0.05,
        raw_logits=(-1.0, 2.0, 0.0),
        raw_output_hash=sha(
            f"d24-app-verifier-output:{event.structural_event_id}:{job.logical_job_id}"
        ),
    )
    output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=artifact.artifact_id,
        result_artifact_hash=artifact.artifact_hash,
    )
    return M5VerifierExecution(
        pair_input,
        artifact,
        output,
        M5ExecutionEvidenceDisposition.RETURNED,
        call_work,
        attempt_timing,
    )


@dataclass(slots=True)
class ControlledDiscovery:
    """Explicit deterministic discovery port with call accounting."""

    database: ApplicationD24Database
    include_pair: bool = False
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False
    call_work: M5RuntimeWork = field(default_factory=M5RuntimeWork)
    attempt_timing: M5RuntimeTiming | None = None
    calls: list[str] = field(default_factory=list)

    def discover_requirement_scope(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5DiscoveryExecution:
        self.calls.append(job.logical_job_id)
        if self.failure_reason is not None:
            raise M5ExternalWorkFailure(
                self.failure_reason,
                retryable=self.retryable,
                call_work=self.call_work,
                attempt_timing=self.attempt_timing,
                error_hash=sha(
                    f"d24-app-discovery-error:{event.structural_event_id}:"
                    f"{job.logical_job_id}:{self.failure_reason.value}"
                ),
            )
        return build_discovery_execution(
            self.database,
            epoch_id=epoch_id,
            lease=lease,
            job=job,
            manifest=manifest,
            event=event,
            include_pair=self.include_pair,
            call_work=self.call_work,
            attempt_timing=self.attempt_timing,
        )


@dataclass(slots=True)
class ControlledVerifier:
    """Explicit deterministic verifier port with call accounting."""

    database: ApplicationD24Database
    failure_reason: M5RunFailureReason | None = None
    retryable: bool = False
    call_work: M5RuntimeWork = field(default_factory=M5RuntimeWork)
    attempt_timing: M5RuntimeTiming | None = None
    calls: list[str] = field(default_factory=list)

    def verify_requirement_pair(
        self,
        epoch_id: int,
        lease: M5JobLease,
        job: M5LogicalJobSpec,
        manifest: M5CandidatePolicyManifest,
        event: M5TypedEventPlan,
    ) -> M5VerifierExecution:
        self.calls.append(job.logical_job_id)
        if self.failure_reason is not None:
            raise M5ExternalWorkFailure(
                self.failure_reason,
                retryable=self.retryable,
                call_work=self.call_work,
                attempt_timing=self.attempt_timing,
                error_hash=sha(
                    f"d24-app-verifier-error:{event.structural_event_id}:"
                    f"{job.logical_job_id}:{self.failure_reason.value}"
                ),
            )
        return build_verifier_execution(
            self.database,
            epoch_id=epoch_id,
            lease=lease,
            job=job,
            manifest=manifest,
            event=event,
            call_work=self.call_work,
            attempt_timing=self.attempt_timing,
        )


@dataclass(slots=True)
class ControlledMeasurements:
    """Deterministic timing port that keeps transition and terminal calls visible."""

    transition_timing: M5RuntimeTiming | None = field(
        default_factory=lambda: M5RuntimeTiming(
            coordinator_non_db_non_neural_ns=11,
            postgres_roundtrip_wall_ns=13,
            end_to_end_wall_ns=17,
        )
    )
    terminal_timing: M5RuntimeTiming | None = field(
        default_factory=lambda: M5RuntimeTiming(
            coordinator_non_db_non_neural_ns=19,
            postgres_roundtrip_wall_ns=23,
            end_to_end_wall_ns=29,
        )
    )
    transition_calls: list[M5TransitionTimingAnchor] = field(default_factory=list)
    terminal_calls: list[tuple[str, int]] = field(default_factory=list)
    invocation_namespace: str = field(default_factory=lambda: uuid.uuid4().hex)

    def transition_call_timing(
        self, anchor: M5TransitionTimingAnchor
    ) -> M5RuntimeTiming | None:
        self.transition_calls.append(anchor)
        return self.transition_timing

    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry:
        epoch_id = result.epoch_id
        if not isinstance(epoch_id, int) or isinstance(epoch_id, bool):
            raise AssertionError("terminal measurement requires a typed epoch ID")
        self.terminal_calls.append((event.structural_event_id, epoch_id))
        invocation_id = sha(
            f"d24-app-invocation:{self.invocation_namespace}:"
            f"{event.structural_event_id}:{epoch_id}:"
            f"{len(self.terminal_calls)}"
        )
        return M5TerminalInvocationTelemetry(invocation_id, self.terminal_timing)


@pytest.fixture
def d24_application_db() -> Iterator[ApplicationD24Database]:
    """Install 000--016 and one activated base in a fresh unique schema."""

    dsn = database_url()
    schema_name = f"groundloop_m5_d24_application_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    connection: Connection[Any] | None = None
    try:
        connection = psycopg.connect(dsn)
        select_schema(connection, schema_name)
        with connection.transaction():
            apply_legacy_migrations(connection)
        install_m5_core_bundle(connection)
        install_m5_runtime_bundle(connection)
        install_m5_runtime_recovery_bundle(connection)
        assert_accepted_016_ledger(connection)
        connection.commit()

        with connection.transaction():
            base = seed_base(
                connection,
                prefix="d24-application-base",
                claim_count=1,
                chunk_texts=CHUNK_TEXTS,
            )
            published_group = make_group(
                group_id="d24-application-published-group-v1",
                family_id="d24-application-published-family",
                claim_id=base.claim_ids[0],
                texts=("published controlled requirement",),
                source_id="d24-r2c-published-fixture",
            )
            insert_published_group(
                connection,
                group=published_group,
                epoch_id=base.epoch_id,
            )
            connection.execute(
                """
                INSERT INTO groundloop_model_artifact (
                    model_artifact_id, task, provider, model_id,
                    immutable_revision, tokenizer_revision, license_id,
                    config_hash
                ) VALUES
                    (%s, 'embedding', 'fixture', 'd24-application-embedding',
                     'v1', 'v1', 'MIT', %s),
                    (%s, 'verification', 'fixture', %s,
                     'v1', 'v1', 'MIT', %s)
                """,
                (
                    EMBEDDING_ARTIFACT_ID,
                    sha("d24-application-embedding-config"),
                    VERIFIER_MODEL_ARTIFACT_ID,
                    VERIFIER_MODEL_ID,
                    sha("d24-application-verifier-config"),
                ),
            )
            connection.execute(
                """
                INSERT INTO groundloop_chunker_artifact (
                    chunker_artifact_id, chunker_version,
                    normalization_version, config_hash
                ) VALUES (%s, 'fixture-v1', 'v1', %s)
                """,
                (CHUNKER_ARTIFACT_ID, sha("d24-application-chunker-config")),
            )
            for chunk_id in base.chunk_ids:
                connection.execute(
                    """
                    INSERT INTO groundloop_chunk_provenance (
                        chunk_version_id, chunker_artifact_id, input_hash
                    ) VALUES (%s, %s, %s)
                    """,
                    (
                        chunk_id,
                        CHUNKER_ARTIFACT_ID,
                        sha(f"d24-application-chunker-input:{chunk_id}"),
                    ),
                )
            prompt_template = "Classify whether the chunk supports the requirement."
            connection.execute(
                """
                INSERT INTO groundloop_prompt_artifact (
                    prompt_artifact_id, task, version, template,
                    template_hash, decoding_config_hash
                ) VALUES (%s, 'verification', 'v1', %s, %s, %s)
                """,
                (
                    VERIFIER_PROMPT_ARTIFACT_ID,
                    prompt_template,
                    sha(prompt_template),
                    sha("d24-application-verifier-decoding"),
                ),
            )

        with connection.transaction():
            install_test_activation_barrier(
                connection,
                base,
                activation_id="d24-application-activation",
            )

        manifest = candidate_manifest(base)
        operational_config = M5RuntimeOperationalConfig.build(250)
        store = PostgresM5RuntimeStore(connection)
        store.register_candidate_policy(manifest)
        chunk_snapshot = ActiveChunkSnapshot.build(
            tuple(
                ActiveChunkSnapshotEntry.build(
                    chunk_version_id=chunk_id,
                    chunk_text=chunk_text,
                )
                for chunk_id, chunk_text in zip(
                    base.chunk_ids,
                    CHUNK_TEXTS,
                    strict=True,
                )
            )
        )
        document_version_id = "d24-application-base-document-v1"
        chunks = {
            chunk_id: ChunkFixture(
                document_version_id=document_version_id,
                chunk_index=index,
                chunk_text=chunk_text,
                chunk_text_hash=sha(chunk_text),
            )
            for index, (chunk_id, chunk_text) in enumerate(
                zip(base.chunk_ids, CHUNK_TEXTS, strict=True)
            )
        }
        facade = PostgresM5GroupRequirementPreSealPorts(
            store,
            manifest,
            operational_config,
        )
        yield ApplicationD24Database(
            dsn,
            schema_name,
            connection,
            base,
            published_group,
            manifest,
            operational_config,
            chunk_snapshot,
            chunks,
            store,
            facade,
        )
    finally:
        if connection is not None:
            connection.close()
        assert schema_name.startswith("groundloop_m5_d24_application_")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
