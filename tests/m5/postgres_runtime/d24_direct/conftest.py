"""Independent live PostgreSQL fixture for the M5-D24 typed-direct lane."""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from psycopg import Connection, Cursor, sql

from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.m4.application import (
    DiscoveryResult,
    DynamicEventPlan,
    StructuralWithdrawal,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    ChildClosure,
    CorpusUpdateIdentity,
    DiscoveryScope,
    JobAttempt,
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
    PostgresM4ApplicationPorts,
    StructuralPayload,
    bootstrap_m4_publication,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5CandidatePolicyManifest,
    M5RuntimeOperationalConfig,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectScopeKind,
    RequirementRegistrySnapshot,
)
from groundloop.m5.runtime.direct_m4 import PostgresM5DirectM4Adapter
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m5.postgres.helpers import (
    SeededBase,
    install_test_activation_barrier,
    seed_base,
)

RECOVERY_LEDGER = (
    "m5-runtime-recovery-schema-bundle-v1",
    "28a31f37c13cdaa2b89676e6279740a1f366e1acd16502c4fa722c2e0be21565",
    "a63d2a878a5196e071e3e51c6e6737cf76552057ade65da4112e0f0bafb412d7",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "b7b03574dc2ba62fd6ba7be22744e2fe6d9ec178ffb2b4b9b552c5ff6281dacd",
)
REGISTRY_ID = "d24-direct-registry"
NEW_DOCUMENT_ID = "d24-direct-document"
NEW_DOCUMENT_VERSION_ID = "d24-direct-document-v1"
NEW_CHUNK_ID = "d24-direct-chunk-v1"
NEW_CHUNK_TEXT = "Nimbus is an atmospheric probe."
IMPACT_EXECUTION_HASH = hashlib.sha256(b"d24-direct-impact-execution").hexdigest()
VERIFIER_EXECUTION_HASH = hashlib.sha256(b"d24-direct-verifier-execution").hexdigest()
NORMALIZER_ID = "m5-normalize-text-v1"
NORMALIZER_PROVENANCE_HASH = (
    "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
)
_DIRECT_DECLARATION_QUERIES = (
    (
        "m4_update",
        """
        SELECT update_kind, previous_published_epoch_id,
               candidate_policy_id, registry_snapshot_id, manifest::text
        FROM groundloop_m4_update
        WHERE epoch_id = %s
        ORDER BY epoch_id
        """,
    ),
    (
        "semantic_job",
        """
        SELECT btrim(job_id), parent_job_id, job_kind,
               candidate_policy_id, btrim(payload_hash),
               btrim(execution_spec_hash), claim_id, chunk_version_id,
               expandable, created_revision
        FROM groundloop_semantic_job
        WHERE epoch_id = %s AND parent_job_id IS NULL
        ORDER BY job_id
        """,
    ),
    (
        "discovery_scope",
        """
        SELECT btrim(root_job_id), registry_snapshot_id, scope_kind,
               explicit_claim_ids::text
        FROM groundloop_discovery_scope
        WHERE epoch_id = %s
        ORDER BY root_job_id
        """,
    ),
)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def assert_literal_recovery_ledger(connection: Connection[Any]) -> None:
    rows = connection.execute(
        """
        SELECT bundle_id, btrim(bundle_sha256), btrim(migration_sha256),
               btrim(oracle_sha256), btrim(prerequisite_sha256)
        FROM groundloop_m5_schema_bundle
        WHERE bundle_id = 'm5-runtime-recovery-schema-bundle-v1'
        """
    ).fetchall()
    assert rows == [RECOVERY_LEDGER]


def candidate_manifest(base: SeededBase) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="d24-direct-embedding",
        requirement_role_template_hash=sha("d24-direct-requirement-role"),
        chunk_role_template_hash=sha("d24-direct-chunk-role"),
        vector_method_version="d24-direct-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=sha("d24-direct-vector-build"),
        vector_search_config_hash=sha("d24-direct-vector-search"),
        lexical_method_version="d24-direct-lexical-v1",
        lexical_config_hash=sha("d24-direct-lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=sha("d24-direct-verifier-execution"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


def m4_manifest(
    base: SeededBase, manifest: M5CandidatePolicyManifest
) -> CandidatePolicyManifest:
    return CandidatePolicyManifest.build(
        policy_id=manifest.candidate_policy_id,
        embedding_model_artifact_id=manifest.embedding_model_artifact_id,
        claim_role_template_hash=manifest.requirement_role_template_hash,
        chunk_role_template_hash=manifest.chunk_role_template_hash,
        vector_method_version=manifest.vector_method_version,
        vector_index_kind=manifest.vector_index_kind,
        vector_index_build_config_hash=manifest.vector_index_build_config_hash,
        vector_search_config_hash=manifest.vector_search_config_hash,
        lexical_method_version=manifest.lexical_method_version,
        lexical_config_hash=manifest.lexical_config_hash,
        lexical_postgres_version=manifest.lexical_postgres_version,
        lexical_regconfig_identity=manifest.lexical_regconfig_identity,
        claim_registry_snapshot_id=REGISTRY_ID,
        claim_count=len(base.claim_ids),
        fusion_version=manifest.fusion_version,
        approximate_cap_per_inserted_chunk=(manifest.reverse_budget_per_inserted_chunk),
        frontier_depth=manifest.forward_budget_per_requirement,
        verifier_execution_spec_hash=manifest.verifier_execution_spec_hash,
        decision_policy_version=manifest.decision_policy_version,
        lineage_safety_override=manifest.lineage_safety_override,
    )


@dataclass(slots=True)
class DirectD24Database:
    dsn: str
    schema_name: str
    connection: Connection[Any]
    base: SeededBase
    manifest: M5CandidatePolicyManifest
    ports: PostgresM4ApplicationPorts

    def configure(self, connection: Connection[Any]) -> None:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(self.schema_name)
            )
        )
        assert connection.execute("SELECT current_schema()").fetchone() == (
            self.schema_name,
        )
        assert_literal_recovery_ledger(connection)

    @contextmanager
    def reconnect(self) -> Iterator[Connection[Any]]:
        with psycopg.connect(self.dsn, autocommit=True) as connection:
            self.configure(connection)
            primary_pid = self.connection.execute("SELECT pg_backend_pid()").fetchone()
            reconnect_pid = connection.execute("SELECT pg_backend_pid()").fetchone()
            assert connection is not self.connection
            assert primary_pid is not None and reconnect_pid is not None
            assert primary_pid != reconnect_pid
            yield connection

    @contextmanager
    def two_connections(
        self,
    ) -> Iterator[tuple[Connection[Any], Connection[Any]]]:
        with self.reconnect() as left, self.reconnect() as right:
            left_pid = left.execute("SELECT pg_backend_pid()").fetchone()
            right_pid = right.execute("SELECT pg_backend_pid()").fetchone()
            assert left is not right
            assert left_pid is not None and right_pid is not None
            assert left_pid != right_pid
            yield left, right

    def ports_for(self, connection: Connection[Any]) -> PostgresM4ApplicationPorts:
        return PostgresM4ApplicationPorts(
            connection,
            structural_payloads={},
            execution_mode=M4ExecutionMode.MEASURED,
        )

    def adapter_for(self, connection: Connection[Any]) -> PostgresM5DirectM4Adapter:
        return PostgresM5DirectM4Adapter(self.ports_for(connection))


@pytest.fixture
def d24_direct_db() -> Iterator[DirectD24Database]:
    """Install 000--016 in a unique schema and seed one activated M5 base."""

    dsn = database_url()
    schema_name = f"groundloop_m5_d24_direct_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    connection: Connection[Any] | None = None
    try:
        connection = psycopg.connect(dsn, autocommit=True)
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
        )
        with connection.transaction():
            apply_legacy_migrations(connection)
        install_m5_core_bundle(connection)
        install_m5_runtime_bundle(connection)
        install_m5_runtime_recovery_bundle(connection)
        assert_literal_recovery_ledger(connection)

        with connection.transaction():
            base = seed_base(
                connection,
                prefix="d24-direct-base",
                claim_count=1,
                chunk_texts=("Nimbus was prior evidence.",),
            )
            connection.execute(
                """
                INSERT INTO groundloop_model_artifact (
                    model_artifact_id, task, provider, model_id,
                    immutable_revision, tokenizer_revision, license_id,
                    config_hash
                ) VALUES (
                    'd24-direct-embedding', 'embedding', 'fixture',
                    'd24-direct-embedding', 'v1', 'v1', 'MIT', %s
                )
                """,
                (sha("d24-direct-embedding-config"),),
            )

        bootstrap_m4_publication(connection, sealed_epoch_id=base.epoch_id)
        manifest = candidate_manifest(base)
        ports = PostgresM4ApplicationPorts(
            connection,
            structural_payloads={},
            execution_mode=M4ExecutionMode.MEASURED,
        )
        ports.runtime_store.register_candidate_policy(m4_manifest(base, manifest))
        ports.register_claim_registry_snapshot(REGISTRY_ID, base.claim_ids)
        PostgresM5RuntimeStore(connection).register_candidate_policy(manifest)
        with connection.transaction():
            install_test_activation_barrier(
                connection,
                base,
                activation_id="d24-direct-activation",
            )

        database = DirectD24Database(
            dsn=dsn,
            schema_name=schema_name,
            connection=connection,
            base=base,
            manifest=manifest,
            ports=ports,
        )
        database.configure(connection)
        yield database
    finally:
        if connection is not None:
            connection.close()
        assert schema_name.startswith("groundloop_m5_d24_direct_")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


@dataclass(frozen=True, slots=True)
class OpenedDirectEpoch:
    database: DirectD24Database
    event: DynamicEventPlan
    payload: StructuralPayload
    withdrawal: StructuralWithdrawal
    root: LogicalJobSpec
    scope: DiscoveryScope
    requeue_root: LogicalJobSpec | None
    requeue_scope: DiscoveryScope | None
    epoch_id: int
    adapter: PostgresM5DirectM4Adapter


def event_and_payload(
    database: DirectD24Database,
) -> tuple[DynamicEventPlan, StructuralPayload]:
    inserted = InsertedDocument(
        version=DocumentVersion(
            NEW_DOCUMENT_VERSION_ID,
            NEW_DOCUMENT_ID,
            sha("d24-direct-new-content"),
        ),
        chunks=(
            ChunkVersion(
                NEW_CHUNK_ID,
                NEW_DOCUMENT_VERSION_ID,
                0,
                NEW_CHUNK_TEXT,
            ),
        ),
        source_uri="fixture://d24-direct-document",
    )
    update = CorpusUpdateIdentity(
        event_id="d24-direct-document-insert",
        payload_hash=sha("d24-direct-document-payload"),
        update_kind=UpdateKind.INSERT,
        previous_published_epoch_id=database.base.epoch_id,
        candidate_policy_id=database.manifest.candidate_policy_id,
    )
    return (
        DynamicEventPlan(
            update=update,
            inserted_chunk_version_ids=(NEW_CHUNK_ID,),
            deactivated_chunk_version_ids=(),
            registered_claim_ids=(),
            claim_registry_snapshot_id=REGISTRY_ID,
        ),
        StructuralPayload(inserted=inserted),
    )


def make_job(
    event: DynamicEventPlan,
    *,
    kind: JobKind,
    execution_hash: str,
    parent_job_id: str | None = None,
    claim_id: str = "",
    chunk_id: str = "",
) -> LogicalJobSpec:
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event.update.event_id,
        kind=kind,
        candidate_policy_id=event.update.candidate_policy_id,
        execution_spec_hash=execution_hash,
        parent_job_id=parent_job_id or "",
        claim_id=claim_id,
        chunk_version_id=chunk_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event.update.event_id,
        kind=kind,
        candidate_policy_id=event.update.candidate_policy_id,
        payload_hash=stable_m4_digest(
            "m4-application-job-payload-v1",
            event.update.payload_hash,
            kind.value,
            parent_job_id or "",
            claim_id,
            chunk_id,
        ),
        execution_spec_hash=execution_hash,
        parent_job_id=parent_job_id,
        pair=(PairKey(claim_id, chunk_id) if kind is JobKind.VERIFY_PAIR else None),
        target_claim_id=(claim_id if kind is JobKind.FRONTIER_RETRIEVE else None),
        target_chunk_version_id=(
            chunk_id if kind is JobKind.IMPACT_DISCOVERY else None
        ),
        expandable=kind is not JobKind.VERIFY_PAIR,
    )


def active_chunk_snapshot(
    cursor: Cursor[Any], database: DirectD24Database
) -> ActiveChunkSnapshot:
    base_entries = tuple(
        ActiveChunkSnapshotEntry.build(
            chunk_version_id=str(row[0]), chunk_text=str(row[1])
        )
        for row in cursor.execute(
            """
            SELECT chunk_version_id, text
            FROM groundloop_chunk_version
            WHERE valid_from_epoch <= %s
              AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
            ORDER BY chunk_version_id
            """,
            (database.base.epoch_id, database.base.epoch_id),
        ).fetchall()
    )
    return ActiveChunkSnapshot.build(
        base_entries
        + (
            ActiveChunkSnapshotEntry.build(
                chunk_version_id=NEW_CHUNK_ID,
                chunk_text=NEW_CHUNK_TEXT,
            ),
        )
    )


def insert_outer_declaration(
    cursor: Cursor[Any],
    database: DirectD24Database,
    event: DynamicEventPlan,
) -> tuple[int, RequirementRegistrySnapshot, ActiveChunkSnapshot]:
    assert cursor.execute(
        "SELECT mode FROM groundloop_runtime_mode WHERE singleton FOR UPDATE"
    ).fetchone() == ("m5_active",)
    m4_head = cursor.execute(
        "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton FOR UPDATE"
    ).fetchone()
    m5_head = cursor.execute(
        "SELECT epoch_id FROM groundloop_m5_publication_head WHERE singleton FOR UPDATE"
    ).fetchone()
    assert m4_head == m5_head == (database.base.epoch_id,)
    row = cursor.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (
            %s, %s, 1, 'committed', 'pending', 'pending',
            'provisional', NULL
        ) RETURNING epoch_id
        """,
        (event.update.event_id, event.update.payload_hash),
    ).fetchone()
    assert row is not None
    epoch_id = int(row[0])
    cursor.execute(
        """
        INSERT INTO groundloop_m5_update (
            epoch_id, update_kind, previous_published_epoch_id,
            decision_policy_version, manifest
        ) VALUES (%s, 'document_insert', %s, %s, '{}'::jsonb)
        """,
        (epoch_id, database.base.epoch_id, database.base.policy_version),
    )
    requirements = RequirementRegistrySnapshot.build(())
    chunks = active_chunk_snapshot(cursor, database)
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_epoch (
            epoch_id, structural_event_id, candidate_policy_id,
            candidate_policy_manifest_hash,
            requirement_registry_snapshot_digest,
            active_chunk_snapshot_digest,
            expected_previous_published_epoch_id,
            requirement_root_set_hash, runtime_state, revision,
            open_work_count, open_scope_count,
            blocking_failure_count, terminal_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            'structural_committed', 1, 0, 0, 0, NULL
        )
        """,
        (
            epoch_id,
            event.update.event_id,
            database.manifest.candidate_policy_id,
            database.manifest.manifest_hash,
            requirements.requirement_registry_snapshot_digest,
            chunks.active_chunk_snapshot_digest,
            database.base.epoch_id,
            runtime_digests.requirement_root_set_digest(()),
        ),
    )
    return epoch_id, requirements, chunks


def persist_snapshots(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    requirements: RequirementRegistrySnapshot,
    chunks: ActiveChunkSnapshot,
) -> None:
    cursor.execute(
        """
        INSERT INTO groundloop_m5_requirement_registry_snapshot (
            requirement_registry_snapshot_digest, requirement_count,
            created_epoch_id
        ) VALUES (%s, %s, %s)
        """,
        (
            requirements.requirement_registry_snapshot_digest,
            requirements.requirement_count,
            epoch_id,
        ),
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_active_chunk_snapshot (
            active_chunk_snapshot_digest, chunk_count, created_epoch_id,
            normalizer_id, normalizer_provenance_hash
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (
            chunks.active_chunk_snapshot_digest,
            chunks.chunk_count,
            epoch_id,
            NORMALIZER_ID,
            NORMALIZER_PROVENANCE_HASH,
        ),
    )
    for ordinal, entry in enumerate(chunks.entries):
        cursor.execute(
            """
            INSERT INTO groundloop_m5_active_chunk_snapshot_member (
                active_chunk_snapshot_digest, member_ordinal,
                chunk_version_id, text_hash
            ) VALUES (%s, %s, %s, %s)
            """,
            (
                chunks.active_chunk_snapshot_digest,
                ordinal,
                entry.chunk_version_id,
                entry.text_hash,
            ),
        )


def _length_frame(value: bytes) -> bytes:
    return len(value).to_bytes(8, byteorder="big", signed=False) + value


def _canonical_db_scalar(value: object) -> bytes:
    if value is None:
        return b"n"
    if isinstance(value, bool):
        return b"b1" if value else b"b0"
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii")
    if isinstance(value, str):
        return b"s" + value.encode("utf-8")
    raise AssertionError(f"unsupported direct declaration scalar: {type(value)!r}")


def derive_direct_open_work(
    cursor: Cursor[Any], *, epoch_id: int, direct_declaration_count: int = 1
) -> M5RuntimeWork:
    """Measure the exact immutable direct-declaration rows with length framing."""

    framed = [_length_frame(b"m5-d24-direct-declaration-v1")]
    for relation_name, query in _DIRECT_DECLARATION_QUERIES:
        rows = cursor.execute(query, (epoch_id,)).fetchall()
        expected_count = 1 if relation_name == "m4_update" else direct_declaration_count
        assert len(rows) == expected_count, (relation_name, rows)
        relation = [_length_frame(relation_name.encode("ascii"))]
        for row in rows:
            row_bytes = b"".join(
                _length_frame(_canonical_db_scalar(value)) for value in row
            )
            relation.append(_length_frame(row_bytes))
        framed.append(_length_frame(b"".join(relation)))
    canonical = b"".join(framed)
    assert hashlib.sha256(canonical).digest()
    assert canonical
    return M5RuntimeWork(
        bytes_hashed=len(canonical),
        bytes_serialized=len(canonical),
    )


def assert_structural_open_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    expected: M5RuntimeWork,
    direct_declaration_count: int = 1,
) -> None:
    derived = derive_direct_open_work(
        cursor,
        epoch_id=epoch_id,
        direct_declaration_count=direct_declaration_count,
    )
    assert derived == expected and not derived.is_zero
    counters = M5RuntimeWork.counter_names()
    contribution = cursor.execute(
        sql.SQL(
            "SELECT {}, work_digest, applied_revision "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id = %s AND contribution_kind = 'structural_open'"
        ).format(sql.SQL(", ").join(sql.Identifier(name) for name in counters)),
        (epoch_id,),
    ).fetchone()
    assert contribution == (*expected.counter_values(), expected.work_digest, 1)
    accumulator = cursor.execute(
        sql.SQL(
            "SELECT {}, work_digest, updated_revision, terminalized "
            "FROM groundloop_m5_runtime_work_accumulator WHERE epoch_id = %s"
        ).format(sql.SQL(", ").join(sql.Identifier(name) for name in counters)),
        (epoch_id,),
    ).fetchone()
    assert accumulator == (*expected.counter_values(), expected.work_digest, 1, False)


def persist_initial_accounting(
    cursor: Cursor[Any],
    *,
    epoch_id: int,
    lease_duration_ms: int,
    direct_declaration_count: int = 1,
) -> M5RuntimeWork:
    event_row = cursor.execute(
        "SELECT event_id, btrim(payload_hash) "
        "FROM groundloop_epoch WHERE epoch_id = %s",
        (epoch_id,),
    ).fetchone()
    assert event_row is not None
    event_id = str(event_row[0])
    payload_hash = str(event_row[1])
    config = M5RuntimeOperationalConfig.build(lease_duration_ms)
    structural_work = derive_direct_open_work(
        cursor,
        epoch_id=epoch_id,
        direct_declaration_count=direct_declaration_count,
    )
    assert not structural_work.is_zero
    kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=event_id,
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_operational_config (
            epoch_id, lease_duration_ms, config_digest
        ) VALUES (%s, %s, %s)
        """,
        (epoch_id, config.lease_duration_ms, config.config_digest),
    )
    counters = M5RuntimeWork.counter_names()
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution "
            "(epoch_id, {}, work_digest, contribution_kind, source_id, "
            "source_identity_hash, contribution_key_digest, applied_revision) "
            "VALUES (%s, {}, %s, %s, %s, %s, %s, 1)"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in counters),
            sql.SQL(", ").join(sql.Placeholder() for _ in counters),
        ),
        (
            epoch_id,
            *structural_work.counter_values(),
            structural_work.work_digest,
            kind.value,
            event_id,
            payload_hash,
            key,
        ),
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_accumulator "
            "(epoch_id, {}, work_digest, updated_revision, terminalized) "
            "VALUES (%s, {}, %s, 1, false)"
        ).format(
            sql.SQL(", ").join(sql.Identifier(name) for name in counters),
            sql.SQL(", ").join(sql.Placeholder() for _ in counters),
        ),
        (
            epoch_id,
            *structural_work.counter_values(),
            structural_work.work_digest,
        ),
    )
    cursor.execute(
        """
        INSERT INTO groundloop_m5_runtime_timing_accumulator (
            epoch_id, required_expected_count,
            postgres_server_execution_expected_count,
            postgres_lock_wait_expected_count,
            postgres_wal_bytes_expected_count,
            postgres_shared_block_reads_expected_count,
            pending_contribution_kind, pending_source_id,
            pending_contribution_key_digest, pending_anchor_revision,
            updated_revision, terminalized
        ) VALUES (%s, 1, 1, 1, 1, 1, %s, %s, %s, 1, 1, false)
        """,
        (epoch_id, kind.value, event_id, key),
    )
    return structural_work


def open_direct_epoch(
    database: DirectD24Database,
    *,
    lease_duration_ms: int = 300,
    include_requeue_root: bool = False,
) -> OpenedDirectEpoch:
    event, payload = event_and_payload(database)
    withdrawal = database.ports.plan_exact_withdrawal(event)
    root = make_job(
        event,
        kind=JobKind.IMPACT_DISCOVERY,
        execution_hash=IMPACT_EXECUTION_HASH,
        chunk_id=NEW_CHUNK_ID,
    )
    scope = DiscoveryScope(root.job_id, REGISTRY_ID, ())
    requeue_root = (
        make_job(
            event,
            kind=JobKind.IMPACT_DISCOVERY,
            execution_hash=sha("d24-direct-requeue-impact-execution"),
            chunk_id=NEW_CHUNK_ID,
        )
        if include_requeue_root
        else None
    )
    requeue_scope = (
        DiscoveryScope(requeue_root.job_id, REGISTRY_ID, ())
        if requeue_root is not None
        else None
    )
    roots = (root,) if requeue_root is None else (root, requeue_root)
    scopes = (scope,) if requeue_scope is None else (scope, requeue_scope)
    adapter = PostgresM5DirectM4Adapter(database.ports)
    with database.connection.transaction(), database.connection.cursor() as cursor:
        epoch_id, requirements, chunks = insert_outer_declaration(
            cursor, database, event
        )
        receipt = adapter.stage_direct_open(
            cursor,
            event,
            payload,
            withdrawal,
            roots,
            scopes,
        )
        assert receipt.epoch_id == epoch_id and not receipt.replayed
        persist_snapshots(
            cursor,
            epoch_id=epoch_id,
            requirements=requirements,
            chunks=chunks,
        )
        structural_work = persist_initial_accounting(
            cursor,
            epoch_id=epoch_id,
            lease_duration_ms=lease_duration_ms,
            direct_declaration_count=len(roots),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    adapter._after_outer_commit()
    with database.reconnect() as reconnect, reconnect.cursor() as cursor:
        assert_structural_open_accounting(
            cursor,
            epoch_id=epoch_id,
            expected=structural_work,
            direct_declaration_count=len(roots),
        )
    return OpenedDirectEpoch(
        database=database,
        event=event,
        payload=payload,
        withdrawal=withdrawal,
        root=root,
        scope=scope,
        requeue_root=requeue_root,
        requeue_scope=requeue_scope,
        epoch_id=epoch_id,
        adapter=adapter,
    )


def deterministic_token(job: LogicalJobSpec, ordinal: int = 1) -> str:
    return stable_m4_digest("m4-lease-token-v1", job.job_id, str(ordinal))


def wait_until_expired(connection: Connection[Any], deadline: Any) -> None:
    connection.execute(
        """
        SELECT pg_sleep(
            greatest(extract(epoch FROM (%s::timestamptz - clock_timestamp())), 0)
            + 0.02
        )
        """,
        (deadline,),
    )


def discovery_envelope(
    opened: OpenedDirectEpoch,
    lease: M5TypedDirectJobLease,
    *,
    attempt_ordinal: int,
    job: LogicalJobSpec | None = None,
) -> tuple[M5TypedDirectLateReturnEnvelope, tuple[LogicalJobSpec, ...]]:
    assert lease.attempt_id is not None and lease.lease_token_hash is not None
    root = opened.root if job is None else job
    artifact_suffix = "" if root == opened.root else "-requeue"
    claim_id = opened.database.base.claim_ids[0]
    pair = PairKey(claim_id, NEW_CHUNK_ID)
    child = make_job(
        opened.event,
        kind=JobKind.VERIFY_PAIR,
        execution_hash=VERIFIER_EXECUTION_HASH,
        parent_job_id=root.job_id,
        claim_id=pair.claim_id,
        chunk_id=pair.chunk_version_id,
    )
    admitted = AdmittedPair(
        epoch_id=opened.epoch_id,
        pair=pair,
        candidate_policy_id=opened.event.update.candidate_policy_id,
        fused_rank=1,
        reasons=(AdmissionChannel.VECTOR,),
        mandatory_lineage=False,
    )
    hit = ChannelHit(
        epoch_id=opened.epoch_id,
        pair=pair,
        candidate_policy_id=opened.event.update.candidate_policy_id,
        channel=AdmissionChannel.VECTOR,
        rank=1,
        score=0.91,
        channel_artifact_hash=sha("d24-direct-channel-hit"),
    )
    discovery = DiscoveryResult(
        root_job_id=root.job_id,
        result_artifact_id=f"d24-direct-discovery-artifact{artifact_suffix}",
        result_artifact_hash=sha(f"d24-direct-discovery-result{artifact_suffix}"),
        admitted_pairs=(admitted,),
        channel_hits=(hit,),
    )
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
    attempt = JobAttempt(
        attempt_id=lease.attempt_id,
        job_id=root.job_id,
        execution_spec_hash=root.execution_spec_hash,
        attempt_ordinal=attempt_ordinal,
        lease_token_hash=lease.lease_token_hash,
    )
    open_scope = DiscoveryScope(
        root.job_id,
        REGISTRY_ID,
        opened.database.base.claim_ids,
    )
    envelope = M5TypedDirectLateReturnEnvelope.build_discovery(
        epoch_id=opened.epoch_id,
        job=root,
        attempt=attempt,
        completion=completion,
        discovery=discovery,
        scope=open_scope,
        persisted_scope_kind=M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS,
        explicit_claim_ids=None,
        closed_revision=None,
    )
    return envelope, (child,)


def verifier_envelope(
    opened: OpenedDirectEpoch,
    lease: M5TypedDirectJobLease,
    job: LogicalJobSpec,
    *,
    attempt_ordinal: int,
    make_effective: bool = True,
) -> M5TypedDirectLateReturnEnvelope:
    assert job.pair is not None
    assert lease.attempt_id is not None and lease.lease_token_hash is not None
    identity = f"{job.job_id}:{make_effective}"
    result_hash = sha(f"d24-direct-verification-result:{identity}")
    completion = JobCompletion.build(
        job_id=job.job_id,
        payload_hash=job.payload_hash,
        execution_spec_hash=job.execution_spec_hash,
        result_artifact_id=f"d24-direct-verification-artifact-{sha(identity)[:16]}",
        result_artifact_hash=result_hash,
        terminal_state=(
            JobState.COMPLETED_ACTIVE if make_effective else JobState.COMPLETED_INACTIVE
        ),
    )
    observation = SemanticObservation(
        observation_id=sha(f"d24-direct-observation:{identity}"),
        subject_kind=SubjectKind.CLAIM,
        subject_id=job.pair.claim_id,
        chunk_version_id=job.pair.chunk_version_id,
        task_type="claim-verification-v1",
        support_score=0.95,
        refute_score=0.025,
        neutral_score=0.025,
        producer=ModelStamp("d24-direct-verifier", "v1", "prompt-v1"),
        input_hash=sha(f"d24-direct-verifier-input:{identity}"),
    )
    attempt = JobAttempt(
        attempt_id=lease.attempt_id,
        job_id=job.job_id,
        execution_spec_hash=job.execution_spec_hash,
        attempt_ordinal=attempt_ordinal,
        lease_token_hash=lease.lease_token_hash,
    )
    return M5TypedDirectLateReturnEnvelope.build_verifier(
        epoch_id=opened.epoch_id,
        job=job,
        attempt=attempt,
        completion=completion,
        verification_execution=None,
        observation=observation,
        observation_produced_epoch=opened.epoch_id,
        observation_raw_output_hash=result_hash,
        observation_eligible_for_currency=True,
        requested_make_effective=make_effective,
    )


def cancel_direct_job_for_audit(
    opened: OpenedDirectEpoch,
    *,
    terminal_reason: str,
    job: LogicalJobSpec | None = None,
) -> str:
    connection = opened.database.connection
    selected = opened.root if job is None else job
    with connection.transaction():
        row = connection.execute(
            """
            SELECT runtime.revision, job.job_state, job.completion_digest
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_semantic_job AS job
              ON job.epoch_id = runtime.epoch_id
            WHERE runtime.epoch_id = %s AND job.job_id = %s
            """,
            (opened.epoch_id, selected.job_id),
        ).fetchone()
        assert row is not None
        revision = int(row[0])
        assert str(row[1]) == "running" and row[2] is None
        projection = runtime_digests.typed_direct_terminal_projection_digest(
            job_id=selected.job_id,
            terminal_state=JobState.CANCELLED,
            terminal_reason=terminal_reason,
            m4_completion_digest=None,
            completed_revision=revision,
        )
        changed = connection.execute(
            """
            UPDATE groundloop_semantic_job
            SET job_state = 'cancelled', completed_revision = %s,
                completed_at = clock_timestamp()
            WHERE epoch_id = %s AND job_id = %s AND job_state = 'running'
            """,
            (revision, opened.epoch_id, selected.job_id),
        ).rowcount
        assert changed == 1
        connection.execute(
            """
            INSERT INTO groundloop_m5_direct_terminal_projection (
                epoch_id, job_id, terminal_state, terminal_reason,
                m4_completion_digest, completed_revision,
                terminal_identity_hash
            ) VALUES (%s, %s, 'cancelled', %s, NULL, %s, %s)
            """,
            (
                opened.epoch_id,
                selected.job_id,
                terminal_reason,
                revision,
                projection,
            ),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return projection
