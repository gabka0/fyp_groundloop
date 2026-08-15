"""Schema-isolated live PostgreSQL fixtures for M5 failure/replay tests."""

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
from psycopg import Connection, sql

from groundloop.m4.contracts import VectorIndexKind
from groundloop.m5.domain import EvidenceGroupVersion
from groundloop.m5.events import (
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    m5_event_payload_digest,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5CandidatePolicyManifest,
    M5RuntimeOperationalConfig,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
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


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _select_schema(connection: Connection[Any], schema_name: str) -> None:
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
    )
    connection.commit()


def _manifest(
    base: SeededBase, *, variant: str = "primary"
) -> M5CandidatePolicyManifest:
    return M5CandidatePolicyManifest.build(
        embedding_model_artifact_id="failure-replay-embedding",
        requirement_role_template_hash=_sha("requirement-role"),
        chunk_role_template_hash=_sha("chunk-role"),
        vector_method_version="fixture-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=_sha("vector-build"),
        vector_search_config_hash=_sha("vector-search"),
        lexical_method_version="fixture-lexical-v1",
        lexical_config_hash=_sha("lexical-config"),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2 if variant == "primary" else 3,
        verifier_execution_spec_hash=_sha("verifier-execution"),
        decision_policy_version=base.policy_version,
        lineage_safety_override=True,
    )


def _requirement_snapshot(
    groups: tuple[EvidenceGroupVersion, ...],
) -> RequirementRegistrySnapshot:
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


@dataclass(frozen=True, slots=True)
class M5RuntimeDatabase:
    dsn: str
    schema_name: str
    connection: Connection[Any]
    base: SeededBase
    group: EvidenceGroupVersion
    manifest: M5CandidatePolicyManifest
    requirement_snapshot: RequirementRegistrySnapshot
    chunk_snapshot: ActiveChunkSnapshot
    operational_config: M5RuntimeOperationalConfig

    @contextmanager
    def reconnect(self) -> Iterator[Connection[Any]]:
        """Open a fresh connection bound to this fixture's isolated schema."""

        with psycopg.connect(self.dsn) as connection:
            _select_schema(connection, self.schema_name)
            yield connection

    def register_manifest(
        self, manifest: M5CandidatePolicyManifest
    ) -> M5CandidatePolicyManifest:
        PostgresM5RuntimeStore(self.connection).register_candidate_policy(manifest)
        return manifest

    def alternate_manifest(self) -> M5CandidatePolicyManifest:
        return _manifest(self.base, variant="alternate")

    def retire_plan(
        self,
        *,
        event_id: str = "failure-replay-retire",
        group_version_id: str | None = None,
        manifest: M5CandidatePolicyManifest | None = None,
        requirement_snapshot: RequirementRegistrySnapshot | None = None,
        expected_previous_published_epoch_id: int | None = None,
    ) -> M5TypedEventPlan:
        event = RetireGroupEvent(
            event_id=event_id,
            group_version_id=group_version_id or self.group.group_version_id,
        )
        selected_manifest = manifest or self.manifest
        return M5TypedEventPlan(
            structural_event_id=event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=selected_manifest.candidate_policy_id,
            candidate_policy_manifest_hash=selected_manifest.manifest_hash,
            requirement_registry_snapshot=(
                requirement_snapshot or self.requirement_snapshot
            ),
            active_chunk_snapshot=self.chunk_snapshot,
            expected_previous_published_epoch_id=(
                expected_previous_published_epoch_id or self.base.epoch_id
            ),
        )

    def register_plan(
        self,
        *,
        event_id: str = "failure-replay-register",
        group_version_id: str = "failure-replay-register-group-v1",
        group_family_id: str = "failure-replay-register-family",
    ) -> M5TypedEventPlan:
        group = make_group(
            group_id=group_version_id,
            family_id=group_family_id,
            claim_id=self.base.claim_ids[0],
            texts=("registered fact one", "registered fact two"),
            source_id="failure-replay-register-fixture",
        )
        event = RegisterGroupEvent(event_id=event_id, group=group)
        snapshot = _requirement_snapshot((self.group, group))
        return M5TypedEventPlan(
            structural_event_id=event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=self.manifest.candidate_policy_id,
            candidate_policy_manifest_hash=self.manifest.manifest_hash,
            requirement_registry_snapshot=snapshot,
            active_chunk_snapshot=self.chunk_snapshot,
            expected_previous_published_epoch_id=self.base.epoch_id,
        )

    def replace_plan(
        self,
        *,
        event_id: str = "failure-replay-replace",
        successor_group_version_id: str = "failure-replay-group-v2",
        successor_requirement_id: str = "failure-replay-group-v2-requirement-0",
    ) -> M5TypedEventPlan:
        old_requirement = self.group.requirements[0]
        successor = make_group(
            group_id=successor_group_version_id,
            family_id=self.group.group_family_id,
            claim_id=self.group.owner_claim_id,
            texts=("replacement fact",),
            requirement_ids=(successor_requirement_id,),
            predecessors=(old_requirement.requirement_version_id,),
            supersedes_group_id=self.group.group_version_id,
            source_id="failure-replay-replace-fixture",
        )
        event = ReplaceGroupEvent(
            event_id=event_id,
            old_group_version_id=self.group.group_version_id,
            successor=successor,
        )
        snapshot = _requirement_snapshot((successor,))
        return M5TypedEventPlan(
            structural_event_id=event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=self.manifest.candidate_policy_id,
            candidate_policy_manifest_hash=self.manifest.manifest_hash,
            requirement_registry_snapshot=snapshot,
            active_chunk_snapshot=self.chunk_snapshot,
            expected_previous_published_epoch_id=self.base.epoch_id,
        )


@pytest.fixture
def m5_runtime_db() -> Iterator[M5RuntimeDatabase]:
    """Create an activated one-group database with accepted migration 016."""

    dsn = _database_url()
    schema_name = f"groundloop_m5_failure_replay_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))

    try:
        with psycopg.connect(dsn) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            install_m5_runtime_bundle(connection)
            install_m5_runtime_recovery_bundle(connection)
            ledger = connection.execute(
                """
                SELECT bundle_id, bundle_sha256, migration_sha256,
                       oracle_sha256, prerequisite_sha256
                FROM groundloop_m5_schema_bundle
                WHERE bundle_id = %s
                """,
                (ACCEPTED_016_LEDGER[0],),
            ).fetchone()
            assert ledger is not None
            assert tuple(str(value).strip() for value in ledger) == (
                ACCEPTED_016_LEDGER
            )
            connection.commit()

            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="failure-replay",
                    claim_count=1,
                    chunk_texts=("alpha", "beta"),
                )
                group = make_group(
                    group_id="failure-replay-group-v1",
                    family_id="failure-replay-family",
                    claim_id=base.claim_ids[0],
                    texts=("required fact",),
                    source_id="failure-replay-fixture",
                )
                insert_published_group(
                    connection,
                    group=group,
                    epoch_id=base.epoch_id,
                )
                connection.execute(
                    """
                    INSERT INTO groundloop_model_artifact (
                        model_artifact_id, task, provider, model_id,
                        immutable_revision, tokenizer_revision, license_id,
                        config_hash
                    ) VALUES (
                        'failure-replay-embedding', 'embedding', 'fixture',
                        'fixture-embedding', 'v1', 'v1', 'MIT', %s
                    )
                    """,
                    (_sha("embedding-config"),),
                )

            with connection.transaction():
                install_test_activation_barrier(
                    connection,
                    base,
                    activation_id="failure-replay-activation",
                )

            manifest = _manifest(base)
            PostgresM5RuntimeStore(connection).register_candidate_policy(manifest)
            requirement_snapshot = RequirementRegistrySnapshot.build(())
            chunk_snapshot = ActiveChunkSnapshot.build(
                tuple(
                    ActiveChunkSnapshotEntry.build(
                        chunk_version_id=chunk_id,
                        chunk_text=chunk_text,
                    )
                    for chunk_id, chunk_text in zip(
                        base.chunk_ids, ("alpha", "beta"), strict=True
                    )
                )
            )
            yield M5RuntimeDatabase(
                dsn=dsn,
                schema_name=schema_name,
                connection=connection,
                base=base,
                group=group,
                manifest=manifest,
                requirement_snapshot=requirement_snapshot,
                chunk_snapshot=chunk_snapshot,
                operational_config=M5RuntimeOperationalConfig.build(300),
            )
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
