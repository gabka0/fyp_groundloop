"""Live PostgreSQL acceptance tests for the immutable migration-015 bundle."""

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
from psycopg import Connection, errors, sql

from groundloop.m5.digests import (
    bool_field,
    enum_field,
    hash_field,
    int_field,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.domain import EvidenceGroupVersion, EvidenceRequirementVersion
from groundloop.m5.events import RegisterGroupEvent, m5_event_payload_digest
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.postgres.migrations import (
    M5_BUNDLE_ID,
    M5_RUNTIME_BUNDLE_ID,
    M5_RUNTIME_MIGRATION_PATH,
    M5_RUNTIME_ORACLE_SHA256,
    M5BundleHashConflictError,
    M5PrerequisiteError,
    M5RuntimeBundleError,
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
)

RUNTIME_RELATIONS = (
    "groundloop_m5_candidate_policy",
    "groundloop_m5_requirement_registry_snapshot",
    "groundloop_m5_requirement_registry_snapshot_member",
    "groundloop_m5_active_chunk_snapshot",
    "groundloop_m5_active_chunk_snapshot_member",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_discovery_scope",
    "groundloop_m5_requirement_channel_hit",
    "groundloop_m5_requirement_scope_selection",
    "groundloop_m5_requirement_discovery_result",
    "groundloop_m5_requirement_admitted_pair",
    "groundloop_m5_requirement_admitted_pair_source",
    "groundloop_m5_semantic_job",
    "groundloop_m5_job_dependency",
    "groundloop_m5_job_attempt",
    "groundloop_m5_attempt_result_artifact",
    "groundloop_m5_requirement_pair_input",
    "groundloop_m5_requirement_verifier_artifact",
    "groundloop_m5_requirement_verifier_execution",
    "groundloop_m5_requirement_frontier_head",
    "groundloop_m5_owner_pending_counter",
    "groundloop_m5_answer_pending_counter",
    "groundloop_m5_runtime_work",
    "groundloop_m5_event_result",
    "groundloop_m5_event_result_delta",
    "groundloop_m5_event_result_state_reference",
)

EMPTY_REQUIREMENT_SNAPSHOT_DIGEST = stable_m5_digest(
    "m5-requirement-registry-snapshot-v2",
    int_field(0),
    sequence_field(()),
)
EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST = stable_m5_digest(
    "m5-active-chunk-snapshot-v2",
    int_field(0),
    sequence_field(()),
)
EMPTY_REQUIREMENT_ROOT_SET_HASH = stable_m5_digest(
    "m5-requirement-root-set-v2",
    sequence_field(()),
)
ZERO_RUNTIME_WORK_DIGEST = stable_m5_digest(
    "m5-runtime-work-v2",
    *(int_field(0) for _ in range(32)),
)
EMPTY_STATUS_DELTA_SET_HASH = stable_m5_digest(
    "m5-combined-status-delta-set-v2",
    sequence_field(()),
)
EMPTY_CHANGED_STATE_SET_HASH = stable_m5_digest(
    "m5-changed-state-set-v2",
    sequence_field(()),
)
RUNTIME_WORK_COUNTER_COLUMNS = (
    "deactivated_chunk_count",
    "withdrawn_candidate_edge_count",
    "withdrawn_current_observation_count",
    "direct_discovery_call_count",
    "direct_verifier_call_count",
    "direct_observation_artifact_count",
    "direct_effective_observation_count",
    "direct_inactive_completion_count",
    "requirement_forward_retrieval_call_count",
    "requirement_reverse_retrieval_call_count",
    "requirement_fallback_forward_call_count",
    "requirement_verifier_call_count",
    "requirement_observation_artifact_count",
    "requirement_effective_observation_count",
    "requirement_inactive_completion_count",
    "requirement_cancelled_job_count",
    "requirement_late_attempt_artifact_count",
    "requirement_channel_hit_count",
    "requirement_pre_dedup_selection_count",
    "requirement_admitted_pair_count",
    "group_state_write_count",
    "claim_state_write_count",
    "answer_state_write_count",
    "certificate_binding_write_count",
    "public_delta_count",
    "bytes_hashed",
    "bytes_serialized",
    "embedding_model_call_count",
    "verifier_model_call_count",
    "embedding_input_token_count",
    "verifier_input_token_count",
    "verifier_output_token_count",
)
NORMALIZER_PROVENANCE_HASH = (
    "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb"
)


@dataclass(frozen=True, slots=True)
class _BridgeFixture:
    base_epoch_id: int
    direct_policy_id: str
    typed_policy_manifest_hash: str
    decision_policy_version: str
    alternate_decision_policy_version: str
    registry_snapshot_id: str
    owner_claim_id: str
    answer_version_id: str


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


@contextmanager
def _isolated_schema(*, core: bool = True) -> Iterator[Connection[Any]]:
    dsn = _database_url()
    schema_name = f"groundloop_m5_runtime_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    try:
        with psycopg.connect(dsn) as connection:
            _select_schema(connection, schema_name)
            with connection.transaction():
                apply_legacy_migrations(connection)
            if core:
                install_m5_core_bundle(connection)
                connection.commit()
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _current_tables(connection: Connection[Any]) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            """
            SELECT relation.relname
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND relation.relkind = 'r'
            """
        ).fetchall()
    }


def _protected_runtime_snapshot(connection: Connection[Any]) -> tuple[object, ...]:
    return (
        tuple(
            connection.execute(
                """
                SELECT singleton, mode, mode_revision, updated_at
                FROM groundloop_runtime_mode
                ORDER BY singleton
                """
            ).fetchall()
        ),
        tuple(
            connection.execute(
                """
                SELECT singleton, epoch_id, updated_at
                FROM groundloop_m4_publication_head
                ORDER BY singleton
                """
            ).fetchall()
        ),
        tuple(
            connection.execute(
                """
                SELECT singleton, epoch_id, sealed_revision, updated_at
                FROM groundloop_m5_publication_head
                ORDER BY singleton
                """
            ).fetchall()
        ),
        tuple(
            connection.execute(
                """
                SELECT singleton, activation_id, payload_hash,
                       base_m4_epoch_id, activated_at
                FROM groundloop_m5_activation
                ORDER BY singleton
                """
            ).fetchall()
        ),
    )


def _insert_sealed_epoch(connection: Connection[Any], prefix: str) -> int:
    row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (
            %s, %s, 0, 'committed', 'sealed', 'complete', 'strict', now()
        ) RETURNING epoch_id
        """,
        (f"{prefix}-event", _sha(f"{prefix}-payload")),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_bridge_fixture(
    connection: Connection[Any],
    *,
    activate: bool,
    prefix: str = "bridge",
    seed_snapshots: bool = True,
) -> _BridgeFixture:
    base_epoch_id = _insert_sealed_epoch(connection, f"{prefix}-base")
    question_id = f"{prefix}-question"
    answer_id = f"{prefix}-answer"
    owner_claim_id = f"{prefix}-claim"
    connection.execute(
        """
        INSERT INTO groundloop_question (question_id, text, created_epoch)
        VALUES (%s, 'bridge question', %s)
        """,
        (question_id, base_epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_answer_version (
            answer_version_id, question_id, text, generator_model_id,
            generator_model_version, prompt_version, created_epoch
        ) VALUES (%s, %s, 'bridge answer', 'generator', 'v1', 'p1', %s)
        """,
        (answer_id, question_id, base_epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_claim (
            claim_id, answer_version_id, text, extractor_model_id,
            extractor_model_version, extractor_prompt_version, required
        ) VALUES (%s, %s, 'bridge claim', 'extractor', 'v1', 'p1', true)
        """,
        (owner_claim_id, answer_id),
    )
    decision_policy_version = f"{prefix}-decision-v1"
    alternate_decision_policy_version = f"{prefix}-decision-v2"
    alternate_policy_end_epoch_id = _insert_sealed_epoch(
        connection,
        f"{prefix}-alternate-policy-end",
    )
    for policy_version in (
        decision_policy_version,
        alternate_decision_policy_version,
    ):
        connection.execute(
            """
            INSERT INTO groundloop_decision_policy (
                policy_version, support_threshold, refute_threshold,
                tie_rule_version, calibration_version,
                source_policy_version, valid_from_epoch, valid_to_epoch
            ) VALUES (%s, 0.8, 0.8, 'v1', NULL, NULL, %s, %s)
            """,
            (
                policy_version,
                base_epoch_id,
                (
                    alternate_policy_end_epoch_id
                    if policy_version == alternate_decision_policy_version
                    else None
                ),
            ),
        )
    artifact_id = f"{prefix}-embedding-artifact"
    connection.execute(
        """
        INSERT INTO groundloop_model_artifact (
            model_artifact_id, task, provider, model_id,
            immutable_revision, tokenizer_revision, license_id, config_hash
        ) VALUES (%s, 'embedding', 'fixture', 'embedder',
                  'v1', 'v1', 'MIT', %s)
        """,
        (artifact_id, _sha(f"{prefix}-model-config")),
    )
    registry_snapshot_id = f"{prefix}-m4-registry"
    requirement_role_hash = _sha(f"{prefix}-requirement-role")
    chunk_role_hash = _sha(f"{prefix}-chunk-role")
    vector_build_hash = _sha(f"{prefix}-vector-build")
    vector_search_hash = _sha(f"{prefix}-vector-search")
    lexical_hash = _sha(f"{prefix}-lexical")
    verifier_hash = _sha(f"{prefix}-verifier")
    manifest_hash = stable_m5_digest(
        "m5-candidate-policy-v2",
        text_field(artifact_id),
        hash_field(requirement_role_hash),
        hash_field(chunk_role_hash),
        text_field("fixture-vector-v1"),
        enum_field("exact"),
        hash_field(vector_build_hash),
        hash_field(vector_search_hash),
        text_field("fixture-lexical-v1"),
        hash_field(lexical_hash),
        text_field("16.4"),
        text_field("simple"),
        text_field("rank-interleave-v1"),
        int_field(4),
        int_field(4),
        hash_field(verifier_hash),
        text_field(decision_policy_version),
        bool_field(True),
    )
    policy_id = manifest_hash
    connection.execute(
        """
        INSERT INTO groundloop_candidate_policy (
            candidate_policy_id, policy_hash, embedding_model_artifact_id,
            decision_policy_version, claim_role_template_hash,
            chunk_role_template_hash, vector_method_version,
            vector_index_kind, vector_index_build_config_hash,
            vector_search_config_hash, lexical_method_version,
            lexical_config_hash, lexical_postgres_version,
            lexical_regconfig_identity, claim_registry_snapshot_id,
            claim_count, fusion_version, approximate_cap_per_inserted_chunk,
            frontier_depth, manifest
        ) VALUES (
            %s, %s, %s, %s, %s, %s, 'fixture-vector-v1', 'exact',
            %s, %s, 'fixture-lexical-v1', %s, '16.4', 'simple',
            %s, 0, 'interleave-v1', 4, 2, '{}'::jsonb
        )
        """,
        (
            policy_id,
            _sha(f"{prefix}-m4-policy"),
            artifact_id,
            decision_policy_version,
            requirement_role_hash,
            chunk_role_hash,
            vector_build_hash,
            vector_search_hash,
            lexical_hash,
            registry_snapshot_id,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_candidate_policy (
            candidate_policy_id, candidate_policy_manifest_hash,
            embedding_model_artifact_id, requirement_role_template_hash,
            chunk_role_template_hash, vector_method_version,
            vector_index_kind, vector_index_build_config_hash,
            vector_search_config_hash, lexical_method_version,
            lexical_config_hash, lexical_postgres_version,
            lexical_regconfig_identity, fusion_version,
            reverse_budget_per_inserted_chunk,
            forward_budget_per_requirement, verifier_execution_spec_hash,
            decision_policy_version, lineage_safety_override
        ) VALUES (
            %s, %s, %s, %s, %s, 'fixture-vector-v1', 'exact', %s, %s,
            'fixture-lexical-v1', %s, '16.4', 'simple',
            'rank-interleave-v1', 4, 4, %s, %s, true
        )
        """,
        (
            policy_id,
            manifest_hash,
            artifact_id,
            requirement_role_hash,
            chunk_role_hash,
            vector_build_hash,
            vector_search_hash,
            lexical_hash,
            verifier_hash,
            decision_policy_version,
        ),
    )
    if seed_snapshots:
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_registry_snapshot (
                requirement_registry_snapshot_digest,
                requirement_count, created_epoch_id
            ) VALUES (%s, 0, %s)
            """,
            (EMPTY_REQUIREMENT_SNAPSHOT_DIGEST, base_epoch_id),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_active_chunk_snapshot (
                active_chunk_snapshot_digest, chunk_count, created_epoch_id,
                normalizer_id, normalizer_provenance_hash
            ) VALUES (%s, 0, %s, 'm5-normalize-text-v1', %s)
            """,
            (
                EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
                base_epoch_id,
                NORMALIZER_PROVENANCE_HASH,
            ),
        )
    if activate:
        connection.execute(
            """
            INSERT INTO groundloop_m4_publication_head (
                singleton, epoch_id, updated_at
            ) VALUES (true, %s, now())
            """,
            (base_epoch_id,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_publication_head (
                singleton, epoch_id, sealed_revision, updated_at
            ) VALUES (true, %s, 0, now())
            """,
            (base_epoch_id,),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_activation (
                singleton, activation_id, payload_hash,
                base_m4_epoch_id, activated_at
            ) VALUES (true, %s, %s, %s, now())
            """,
            (
                f"{prefix}-activation",
                _sha(f"{prefix}-activation"),
                base_epoch_id,
            ),
        )
        connection.execute(
            """
            UPDATE groundloop_runtime_mode
            SET mode = 'm5_active', mode_revision = mode_revision + 1,
                updated_at = now()
            WHERE singleton
            """
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")
    return _BridgeFixture(
        base_epoch_id=base_epoch_id,
        direct_policy_id=policy_id,
        typed_policy_manifest_hash=manifest_hash,
        decision_policy_version=decision_policy_version,
        alternate_decision_policy_version=alternate_decision_policy_version,
        registry_snapshot_id=registry_snapshot_id,
        owner_claim_id=owner_claim_id,
        answer_version_id=answer_id,
    )


def _insert_typed_header(
    connection: Connection[Any],
    fixture: _BridgeFixture,
    *,
    prefix: str,
    typed_kind: str = "document_insert",
    runtime_event_id: str | None = None,
    runtime_previous_epoch_id: int | None = None,
    runtime_policy_id: str | None = None,
    runtime_manifest_hash: str | None = None,
    typed_decision_policy_version: str | None = None,
    runtime_revision: int = 1,
    runtime_state: str = "structural_committed",
    requirement_snapshot_digest: str = EMPTY_REQUIREMENT_SNAPSHOT_DIGEST,
    active_snapshot_digest: str = EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
) -> int:
    event_id = f"{prefix}-event"
    row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (%s, %s, %s, 'committed', 'pending',
                  'pending', 'provisional', NULL)
        RETURNING epoch_id
        """,
        (event_id, _sha(f"{prefix}-payload"), runtime_revision),
    ).fetchone()
    assert row is not None
    epoch_id = int(row[0])
    connection.execute(
        """
        INSERT INTO groundloop_m5_update (
            epoch_id, update_kind, previous_published_epoch_id,
            decision_policy_version, manifest
        ) VALUES (%s, %s, %s, %s, '{}'::jsonb)
        """,
        (
            epoch_id,
            typed_kind,
            fixture.base_epoch_id,
            typed_decision_policy_version or fixture.decision_policy_version,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_runtime_epoch (
            epoch_id, structural_event_id, candidate_policy_id,
            candidate_policy_manifest_hash,
            requirement_registry_snapshot_digest,
            active_chunk_snapshot_digest,
            expected_previous_published_epoch_id,
            requirement_root_set_hash, runtime_state, revision,
            open_work_count, open_scope_count, blocking_failure_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0)
        """,
        (
            epoch_id,
            runtime_event_id or event_id,
            runtime_policy_id or fixture.direct_policy_id,
            runtime_manifest_hash or fixture.typed_policy_manifest_hash,
            requirement_snapshot_digest,
            active_snapshot_digest,
            (
                fixture.base_epoch_id
                if runtime_previous_epoch_id is None
                else runtime_previous_epoch_id
            ),
            EMPTY_REQUIREMENT_ROOT_SET_HASH,
            runtime_state,
            runtime_revision,
        ),
    )
    return epoch_id


def _insert_direct_update(
    connection: Connection[Any],
    fixture: _BridgeFixture,
    epoch_id: int,
    *,
    direct_kind: str = "insert",
    direct_policy_id: str | None = None,
    direct_previous_epoch_id: int | None = None,
    registry_snapshot_id: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m4_update (
            epoch_id, update_kind, candidate_policy_id,
            previous_published_epoch_id, registry_snapshot_id, manifest
        ) VALUES (%s, %s, %s, %s, %s, '{}'::jsonb)
        """,
        (
            epoch_id,
            direct_kind,
            direct_policy_id or fixture.direct_policy_id,
            (
                fixture.base_epoch_id
                if direct_previous_epoch_id is None
                else direct_previous_epoch_id
            ),
            registry_snapshot_id or fixture.registry_snapshot_id,
        ),
    )


def _insert_staged_group(
    connection: Connection[Any],
    *,
    group: EvidenceGroupVersion,
    epoch_id: int,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_family (
            group_family_id, claim_id, creator_epoch_id, lifecycle_state
        ) VALUES (%s, %s, %s, 'STAGED')
        """,
        (group.group_family_id, group.owner_claim_id, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_version (
            group_version_id, group_family_id, creator_epoch_id,
            lifecycle_state, group_type, construction_kind,
            construction_source_id, constructor_model_id,
            constructor_model_version, constructor_prompt_version,
            supersedes_group_version_id, semantic_structure_hash,
            record_payload_hash
        ) VALUES (
            %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            group.group_version_id,
            group.group_family_id,
            epoch_id,
            group.group_type.value,
            group.construction_kind.value,
            group.construction_source_id,
            group.constructor_model_id,
            group.constructor_model_version,
            group.constructor_prompt_version,
            group.supersedes_group_version_id,
            group.semantic_structure_hash,
            group.record_payload_hash,
        ),
    )
    for requirement in group.requirements:
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_version (
                requirement_version_id, group_version_id, creator_epoch_id,
                lifecycle_state, ordinal, requirement_text,
                requirement_text_hash, constructor_model_id,
                constructor_model_version, constructor_prompt_version,
                supersedes_requirement_version_id
            ) VALUES (
                %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                requirement.requirement_version_id,
                requirement.group_version_id,
                epoch_id,
                requirement.ordinal,
                requirement.requirement_text,
                requirement.requirement_text_hash,
                requirement.constructor_model_id,
                requirement.constructor_model_version,
                requirement.constructor_prompt_version,
                requirement.supersedes_requirement_version_id,
            ),
        )


def _make_staged_group(
    *,
    prefix: str,
    owner_claim_id: str,
) -> tuple[EvidenceGroupVersion, str]:
    group_version_id = f"{prefix}-group-version"
    requirement = EvidenceRequirementVersion(
        requirement_version_id=f"{prefix}-requirement",
        group_version_id=group_version_id,
        ordinal=0,
        requirement_text="A staged requirement",
    )
    group = EvidenceGroupVersion(
        group_version_id=group_version_id,
        group_family_id=f"{prefix}-group-family",
        owner_claim_id=owner_claim_id,
        requirements=(requirement,),
        construction_source_id=f"{prefix}-controlled-source",
    )
    snapshot_digest = stable_m5_digest(
        "m5-requirement-registry-snapshot-v2",
        int_field(1),
        sequence_field(
            (
                sequence_field(
                    (
                        text_field(requirement.requirement_version_id),
                        text_field(group.group_version_id),
                        text_field(group.group_family_id),
                        text_field(group.owner_claim_id),
                        text_field(requirement.requirement_text),
                        hash_field(requirement.requirement_text_hash),
                    )
                ),
            )
        ),
    )
    return group, snapshot_digest


def _insert_header_snapshots_and_counters(
    connection: Connection[Any],
    *,
    fixture: _BridgeFixture,
    group: EvidenceGroupVersion,
    requirement_snapshot_digest: str,
    epoch_id: int,
) -> None:
    requirement = group.requirements[0]
    connection.execute(
        """
        INSERT INTO groundloop_m5_requirement_registry_snapshot (
            requirement_registry_snapshot_digest,
            requirement_count, created_epoch_id
        ) VALUES (%s, 1, %s)
        """,
        (requirement_snapshot_digest, epoch_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_requirement_registry_snapshot_member (
            requirement_registry_snapshot_digest, member_ordinal,
            requirement_version_id, group_version_id, group_family_id,
            owner_claim_id, normalized_requirement_text,
            requirement_text_hash
        ) VALUES (%s, 0, %s, %s, %s, %s, %s, %s)
        """,
        (
            requirement_snapshot_digest,
            requirement.requirement_version_id,
            group.group_version_id,
            group.group_family_id,
            group.owner_claim_id,
            requirement.requirement_text,
            requirement.requirement_text_hash,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_active_chunk_snapshot (
            active_chunk_snapshot_digest, chunk_count, created_epoch_id,
            normalizer_id, normalizer_provenance_hash
        ) VALUES (%s, 0, %s, 'm5-normalize-text-v1', %s)
        """,
        (
            EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
            epoch_id,
            NORMALIZER_PROVENANCE_HASH,
        ),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_owner_pending_counter (
            epoch_id, owner_claim_id, broad_reverse_scope_count,
            forward_scope_count, verifier_job_count,
            blocking_failure_count, updated_revision
        ) VALUES (%s, %s, 0, 0, 0, 0, 1)
        """,
        (epoch_id, fixture.owner_claim_id),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_answer_pending_counter (
            epoch_id, answer_version_id, broad_reverse_scope_count,
            forward_scope_count, verifier_job_count,
            blocking_failure_count, updated_revision
        ) VALUES (%s, %s, 0, 0, 0, 0, 1)
        """,
        (epoch_id, fixture.answer_version_id),
    )


def _insert_zero_work(
    connection: Connection[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    work_kind: str,
) -> None:
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *RUNTIME_WORK_COUNTER_COLUMNS,
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        ),
        (
            ZERO_RUNTIME_WORK_DIGEST,
            structural_event_id,
            epoch_id,
            work_kind,
            *(0 for _ in RUNTIME_WORK_COUNTER_COLUMNS),
        ),
    )


def _insert_failed_event_result(
    connection: Connection[Any],
    *,
    structural_event_id: str,
    epoch_id: int,
    payload_hash: str,
    include_call_work: bool = True,
) -> None:
    _insert_zero_work(
        connection,
        structural_event_id=structural_event_id,
        epoch_id=epoch_id,
        work_kind="event",
    )
    if include_call_work:
        _insert_zero_work(
            connection,
            structural_event_id=structural_event_id,
            epoch_id=epoch_id,
            work_kind="call",
        )
    open_binding_hash = stable_m5_digest(
        "m5-open-event-receipt-binding-v2",
        int_field(epoch_id),
        bool_field(False),
        bool_field(False),
        option_field(None),
        bool_field(False),
        option_field(None),
    )
    failure_reason = "invariant_failure"
    logical_result_hash = stable_m5_digest(
        "m5-event-run-logical-result-v2",
        text_field(structural_event_id),
        hash_field(payload_hash),
        int_field(epoch_id),
        enum_field("failed"),
        hash_field(open_binding_hash),
        option_field(None),
        hash_field(ZERO_RUNTIME_WORK_DIGEST),
        hash_field(EMPTY_STATUS_DELTA_SET_HASH),
        hash_field(EMPTY_CHANGED_STATE_SET_HASH),
        option_field(enum_field(failure_reason)),
    )
    connection.execute(
        """
        INSERT INTO groundloop_m5_event_result (
            structural_event_id, payload_hash, epoch_id, outcome,
            original_open_receipt_binding_hash, publication_id,
            original_publication_receipt_binding_hash,
            event_work_digest, combined_status_delta_set_hash,
            changed_state_set_hash, failure_reason, logical_result_hash,
            delta_count, state_reference_count,
            coordinator_non_db_non_neural_ns, neural_wall_ns,
            postgres_roundtrip_wall_ns, external_io_wall_ns,
            end_to_end_wall_ns, postgres_server_execution_ns,
            postgres_lock_wait_ns, postgres_wal_bytes,
            postgres_shared_block_reads
        ) VALUES (
            %s, %s, %s, 'failed', %s, NULL, NULL, %s, %s, %s, %s, %s,
            0, 0, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL
        )
        """,
        (
            structural_event_id,
            payload_hash,
            epoch_id,
            open_binding_hash,
            ZERO_RUNTIME_WORK_DIGEST,
            EMPTY_STATUS_DELTA_SET_HASH,
            EMPTY_CHANGED_STATE_SET_HASH,
            failure_reason,
            logical_result_hash,
        ),
    )


def _fail_runtime_epoch(
    connection: Connection[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    payload_hash: str,
    include_call_work: bool = True,
) -> None:
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s, 1)",
        (epoch_id,),
    )
    connection.execute(
        """
        UPDATE groundloop_epoch
        SET revision = 2, structural_status = 'failed',
            semantic_status = 'failed', evaluation_state = 'failed',
            sealed_at = NULL
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    connection.execute(
        """
        UPDATE groundloop_m5_runtime_epoch
        SET revision = 2, runtime_state = 'failed', terminal_at = now()
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    )
    _insert_failed_event_result(
        connection,
        structural_event_id=structural_event_id,
        epoch_id=epoch_id,
        payload_hash=payload_hash,
        include_call_work=include_call_work,
    )


def test_fresh_install_creates_exact_named_relations_and_immutable_ledger() -> None:
    with _isolated_schema() as connection:
        before_tables = _current_tables(connection)
        protected_before = _protected_runtime_snapshot(connection)
        trigger_before = connection.execute(
            """
            SELECT pg_get_triggerdef(oid, true)
            FROM pg_trigger
            WHERE tgrelid = 'groundloop_m4_update'::regclass
              AND tgname = 'groundloop_m4_update_runtime_mode_guard'
            """
        ).fetchone()
        guard_before = connection.execute(
            """
            SELECT pg_get_functiondef('groundloop_m5_guard_v1_open()'::regprocedure)
            """
        ).fetchone()

        installed = install_m5_runtime_bundle(connection)
        connection.commit()

        assert installed.applied
        assert _current_tables(connection) - before_tables == set(RUNTIME_RELATIONS)
        assert _protected_runtime_snapshot(connection) == protected_before
        assert connection.execute(
            """
            SELECT pg_get_triggerdef(oid, true)
            FROM pg_trigger
            WHERE tgrelid = 'groundloop_m4_update'::regclass
              AND tgname = 'groundloop_m4_update_runtime_mode_guard'
            """
        ).fetchone() == trigger_before
        guard_after = connection.execute(
            """
            SELECT pg_get_functiondef('groundloop_m5_guard_v1_open()'::regprocedure)
            """
        ).fetchone()
        assert guard_after != guard_before
        assert guard_after is not None
        assert "current-transaction sidecar" in str(guard_after[0])
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_activation"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT mode, mode_revision FROM groundloop_runtime_mode"
        ).fetchone() == ("v1_only", 0)
        assert connection.execute(
            """
            SELECT bundle_sha256, migration_sha256, oracle_sha256,
                   prerequisite_sha256
            FROM groundloop_m5_schema_bundle
            WHERE bundle_id = %s
            """,
            (M5_RUNTIME_BUNDLE_ID,),
        ).fetchone() == (
            installed.identity.bundle_sha256,
            installed.identity.migration_sha256,
            M5_RUNTIME_ORACLE_SHA256,
            installed.identity.prerequisite_sha256,
        )
        assert installed.identity.migration_sha256 == hashlib.sha256(
            M5_RUNTIME_MIGRATION_PATH.read_bytes()
        ).hexdigest()
        assert connection.execute(
            """
            SELECT prerequisite_sha256
            FROM groundloop_m5_schema_bundle
            WHERE bundle_id = %s
            """,
            (M5_RUNTIME_BUNDLE_ID,),
        ).fetchone() == connection.execute(
            """
            SELECT bundle_sha256
            FROM groundloop_m5_schema_bundle
            WHERE bundle_id = %s
            """,
            (M5_BUNDLE_ID,),
        ).fetchone()

        with pytest.raises(errors.RaiseException, match="immutable M5 table"):
            connection.execute(
                """
                UPDATE groundloop_m5_schema_bundle
                SET applied_at = now()
                WHERE bundle_id = %s
                """,
                (M5_RUNTIME_BUNDLE_ID,),
            )
        connection.rollback()


def test_missing_or_wrong_014_ledger_rejects_before_any_015_ddl() -> None:
    with _isolated_schema(core=False) as connection:
        with pytest.raises(M5PrerequisiteError, match="migration-014"):
            install_m5_runtime_bundle(connection)
        connection.rollback()
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_runtime_epoch')"
        ).fetchone() == (None,)

    with _isolated_schema() as connection:
        connection.execute(
            """
            ALTER TABLE groundloop_m5_schema_bundle
            DISABLE TRIGGER groundloop_m5_schema_bundle_immutable
            """
        )
        connection.execute(
            """
            UPDATE groundloop_m5_schema_bundle
            SET bundle_sha256 = %s
            WHERE bundle_id = %s
            """,
            (_sha("wrong-core-bundle"), M5_BUNDLE_ID),
        )
        connection.execute(
            """
            ALTER TABLE groundloop_m5_schema_bundle
            ENABLE TRIGGER groundloop_m5_schema_bundle_immutable
            """
        )
        connection.commit()

        with pytest.raises(M5PrerequisiteError, match="exact accepted"):
            install_m5_runtime_bundle(connection)
        connection.rollback()
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_runtime_epoch')"
        ).fetchone() == (None,)
        assert connection.execute(
            """
            SELECT count(*) FROM groundloop_m5_schema_bundle
            WHERE bundle_id = %s
            """,
            (M5_RUNTIME_BUNDLE_ID,),
        ).fetchone() == (0,)


def test_populated_install_exact_rerun_and_hash_conflict_preserve_rows() -> None:
    with _isolated_schema() as connection:
        base_epoch = _insert_sealed_epoch(connection, "populated-015")
        connection.execute(
            """
            INSERT INTO groundloop_document (
                document_id, source_uri, authority_class
            ) VALUES ('populated-document', 'test://populated', 'fixture')
            """
        )
        connection.commit()
        before = (
            connection.execute(
                "SELECT to_jsonb(epoch_row) FROM groundloop_epoch AS epoch_row"
            ).fetchall(),
            connection.execute(
                "SELECT to_jsonb(document_row) FROM groundloop_document AS document_row"
            ).fetchall(),
            _protected_runtime_snapshot(connection),
        )

        first = install_m5_runtime_bundle(connection)
        connection.commit()
        replay = install_m5_runtime_bundle(connection)
        connection.commit()

        assert first.applied
        assert not replay.applied
        assert replay.identity == first.identity
        assert connection.execute(
            "SELECT epoch_id FROM groundloop_epoch WHERE epoch_id = %s",
            (base_epoch,),
        ).fetchone() == (base_epoch,)
        assert (
            connection.execute(
                "SELECT to_jsonb(epoch_row) FROM groundloop_epoch AS epoch_row"
            ).fetchall(),
            connection.execute(
                "SELECT to_jsonb(document_row) FROM groundloop_document AS document_row"
            ).fetchall(),
            _protected_runtime_snapshot(connection),
        ) == before

        conflicting_source = M5_RUNTIME_MIGRATION_PATH.read_bytes() + (
            b"\n-- immutable hash-conflict probe\n"
        )
        with pytest.raises(M5BundleHashConflictError, match="different content"):
            install_m5_runtime_bundle(
                connection,
                migration_bytes=conflicting_source,
            )
        connection.rollback()
        assert connection.execute(
            """
            SELECT count(*) FROM groundloop_m5_schema_bundle
            WHERE bundle_id = %s
            """,
            (M5_RUNTIME_BUNDLE_ID,),
        ).fetchone() == (1,)


def test_mid_ddl_failure_and_live_epoch_guard_leave_no_partial_schema() -> None:
    with _isolated_schema() as connection:
        original_guard = connection.execute(
            """
            SELECT pg_get_functiondef('groundloop_m5_guard_v1_open()'::regprocedure)
            """
        ).fetchone()

        def fail_after_schema(point: str) -> None:
            if point == "after_schema":
                raise RuntimeError("injected-after-schema")

        with pytest.raises(RuntimeError, match="injected-after-schema"):
            install_m5_runtime_bundle(
                connection,
                failure_injector=fail_after_schema,
            )
        connection.rollback()
        assert set(RUNTIME_RELATIONS).isdisjoint(_current_tables(connection))
        assert connection.execute(
            """
            SELECT count(*) FROM groundloop_m5_schema_bundle
            WHERE bundle_id = %s
            """,
            (M5_RUNTIME_BUNDLE_ID,),
        ).fetchone() == (0,)
        assert connection.execute(
            """
            SELECT pg_get_functiondef('groundloop_m5_guard_v1_open()'::regprocedure)
            """
        ).fetchone() == original_guard

    with _isolated_schema() as connection:
        connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'live-before-015', %s, 0, 'committed', 'pending',
                'pending', 'provisional', NULL
            )
            """,
            (_sha("live-before-015"),),
        )
        connection.commit()
        with pytest.raises(M5RuntimeBundleError, match="no committed"):
            install_m5_runtime_bundle(connection)
        connection.rollback()
        assert set(RUNTIME_RELATIONS).isdisjoint(_current_tables(connection))


def test_deferred_snapshot_validation_and_payload_immutability_are_enforced() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        epoch_id = _insert_sealed_epoch(connection, "snapshot-constraints")
        connection.commit()

        with pytest.raises(
            errors.RaiseException,
            match="invalid requirement snapshot member cardinality",
        ):
            with connection.transaction():
                connection.execute(
                    """
                    INSERT INTO groundloop_m5_requirement_registry_snapshot (
                        requirement_registry_snapshot_digest,
                        requirement_count, created_epoch_id
                    ) VALUES (%s, 1, %s)
                    """,
                    (_sha("incomplete-requirement-snapshot"), epoch_id),
                )
                connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        with connection.transaction():
            connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_registry_snapshot (
                    requirement_registry_snapshot_digest,
                    requirement_count, created_epoch_id
                ) VALUES (%s, 0, %s)
                """,
                (EMPTY_REQUIREMENT_SNAPSHOT_DIGEST, epoch_id),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        with pytest.raises(errors.RaiseException, match="immutable M5 table"):
            connection.execute(
                """
                UPDATE groundloop_m5_requirement_registry_snapshot
                SET requirement_count = 1
                WHERE requirement_registry_snapshot_digest = %s
                """,
                (EMPTY_REQUIREMENT_SNAPSHOT_DIGEST,),
            )
        connection.rollback()

        with pytest.raises(errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO groundloop_m5_active_chunk_snapshot (
                    active_chunk_snapshot_digest, chunk_count,
                    created_epoch_id, normalizer_id,
                    normalizer_provenance_hash
                ) VALUES (%s, 0, %s, 'wrong-normalizer', %s)
                """,
                (_sha("bad-chunk-snapshot"), epoch_id, _sha("normalizer")),
            )
        connection.rollback()


def test_runtime_catalog_has_transition_guards_and_point_lookup_indexes() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()

        transition_relations = {
            "groundloop_m5_runtime_epoch",
            "groundloop_m5_discovery_scope",
            "groundloop_m5_semantic_job",
            "groundloop_m5_job_attempt",
            "groundloop_m5_requirement_frontier_head",
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        }
        guarded = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT relation.relname
                FROM pg_trigger AS trigger_row
                JOIN pg_class AS relation ON relation.oid = trigger_row.tgrelid
                JOIN pg_proc AS function_row ON function_row.oid = trigger_row.tgfoid
                WHERE NOT trigger_row.tgisinternal
                  AND function_row.proname IN (
                      'groundloop_m5_validate_runtime_epoch_transition',
                      'groundloop_m5_validate_scope_transition',
                      'groundloop_m5_validate_job_transition',
                      'groundloop_m5_validate_attempt_transition',
                      'groundloop_m5_validate_frontier_transition',
                      'groundloop_m5_validate_pending_counter_transition'
                  )
                """
            ).fetchall()
        }
        assert guarded == transition_relations

        immutable_payload_relations = {
            "groundloop_m5_candidate_policy",
            "groundloop_m5_requirement_registry_snapshot",
            "groundloop_m5_requirement_registry_snapshot_member",
            "groundloop_m5_active_chunk_snapshot",
            "groundloop_m5_active_chunk_snapshot_member",
            "groundloop_m5_requirement_channel_hit",
            "groundloop_m5_requirement_scope_selection",
            "groundloop_m5_requirement_discovery_result",
            "groundloop_m5_requirement_admitted_pair",
            "groundloop_m5_requirement_admitted_pair_source",
            "groundloop_m5_job_dependency",
            "groundloop_m5_attempt_result_artifact",
            "groundloop_m5_requirement_pair_input",
            "groundloop_m5_requirement_verifier_artifact",
            "groundloop_m5_requirement_verifier_execution",
            "groundloop_m5_runtime_work",
            "groundloop_m5_event_result",
            "groundloop_m5_event_result_delta",
            "groundloop_m5_event_result_state_reference",
        }
        immutable = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT relation.relname
                FROM pg_trigger AS trigger_row
                JOIN pg_class AS relation ON relation.oid = trigger_row.tgrelid
                JOIN pg_proc AS function_row ON function_row.oid = trigger_row.tgfoid
                WHERE NOT trigger_row.tgisinternal
                  AND function_row.proname = 'groundloop_m5_reject_immutable_row'
                  AND relation.relname = ANY(%s::text[])
                """,
                (list(RUNTIME_RELATIONS),),
            ).fetchall()
        }
        assert immutable == immutable_payload_relations

        expected_indexed_surfaces = {
            "groundloop_m5_runtime_epoch",
            "groundloop_m5_semantic_job",
            "groundloop_m5_discovery_scope",
            "groundloop_m5_job_attempt",
            "groundloop_m5_requirement_admitted_pair",
            "groundloop_m5_requirement_pair_input",
            "groundloop_m5_requirement_frontier_head",
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
            "groundloop_m5_event_result",
        }
        indexed = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT table_row.relname
                FROM pg_index AS index_row
                JOIN pg_class AS table_row ON table_row.oid = index_row.indrelid
                JOIN pg_namespace AS namespace
                  ON namespace.oid = table_row.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND table_row.relname = ANY(%s::text[])
                  AND index_row.indisvalid
                """,
                (list(expected_indexed_surfaces),),
            ).fetchall()
        }
        assert indexed == expected_indexed_surfaces

        check_definitions = "\n".join(
            str(row[0])
            for row in connection.execute(
                """
                SELECT pg_get_constraintdef(constraint_row.oid, true)
                FROM pg_constraint AS constraint_row
                JOIN pg_class AS relation
                  ON relation.oid = constraint_row.conrelid
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND relation.relname = ANY(%s::text[])
                  AND constraint_row.contype = 'c'
                ORDER BY relation.relname, constraint_row.conname
                """,
                (list(RUNTIME_RELATIONS),),
            ).fetchall()
        )
        for enum_value in (
            "forward_requirement",
            "reverse_chunk",
            "lexical",
            "lineage",
            "vector",
            "budget_filled",
            "snapshot_exhausted",
            "verify_requirement_pair",
            "retryable_failed",
            "terminal_failed",
            "root_result_staged",
            "terminal_audit_only",
            "invariant_failure",
            "claim_certificate",
            "answer_state",
        ):
            assert enum_value in check_definitions


def test_candidate_policy_id_must_equal_its_manifest_hash() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        prefix = "candidate-id"
        fixture = _seed_bridge_fixture(
            connection,
            activate=False,
            prefix=prefix,
        )
        artifact_id = f"{prefix}-embedding-artifact"
        requirement_role_hash = _sha(f"{prefix}-requirement-role")
        chunk_role_hash = _sha(f"{prefix}-chunk-role")
        vector_build_hash = _sha(f"{prefix}-vector-build")
        vector_search_hash = _sha(f"{prefix}-vector-search")
        lexical_hash = _sha(f"{prefix}-lexical")
        verifier_hash = _sha(f"{prefix}-verifier")
        expected_manifest_hash = stable_m5_digest(
            "m5-candidate-policy-v2",
            text_field(artifact_id),
            hash_field(requirement_role_hash),
            hash_field(chunk_role_hash),
            text_field("fixture-vector-v1"),
            enum_field("exact"),
            hash_field(vector_build_hash),
            hash_field(vector_search_hash),
            text_field("fixture-lexical-v1"),
            hash_field(lexical_hash),
            text_field("16.4"),
            text_field("simple"),
            text_field("rank-interleave-v1"),
            int_field(4),
            int_field(4),
            hash_field(verifier_hash),
            text_field(fixture.alternate_decision_policy_version),
            bool_field(True),
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_candidate_policy (
                candidate_policy_id, candidate_policy_manifest_hash,
                embedding_model_artifact_id, requirement_role_template_hash,
                chunk_role_template_hash, vector_method_version,
                vector_index_kind, vector_index_build_config_hash,
                vector_search_config_hash, lexical_method_version,
                lexical_config_hash, lexical_postgres_version,
                lexical_regconfig_identity, fusion_version,
                reverse_budget_per_inserted_chunk,
                forward_budget_per_requirement, verifier_execution_spec_hash,
                decision_policy_version, lineage_safety_override
            ) VALUES (
                %s, %s, %s, %s, %s, 'fixture-vector-v1', 'exact', %s, %s,
                'fixture-lexical-v1', %s, '16.4', 'simple',
                'rank-interleave-v1', 4, 4, %s, %s, true
            )
            """,
            (
                _sha("candidate-id-only-mismatch"),
                expected_manifest_hash,
                artifact_id,
                requirement_role_hash,
                chunk_role_hash,
                vector_build_hash,
                vector_search_hash,
                lexical_hash,
                verifier_hash,
                fixture.alternate_decision_policy_version,
            ),
        )
        with pytest.raises(
            errors.RaiseException,
            match="ID and manifest hash must equal",
        ):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_v1_only_accepts_direct_update_without_typed_sidecar() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(connection, activate=False, prefix="v1")
        connection.commit()
        row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'v1-direct-event', %s, 0, 'committed', 'pending',
                'pending', 'provisional', NULL
            ) RETURNING epoch_id
            """,
            (_sha("v1-direct-payload"),),
        ).fetchone()
        assert row is not None
        epoch_id = int(row[0])
        _insert_direct_update(connection, fixture, epoch_id)
        connection.commit()
        assert connection.execute(
            "SELECT epoch_id FROM groundloop_m4_update WHERE epoch_id = %s",
            (epoch_id,),
        ).fetchone() == (epoch_id,)


def test_active_exact_current_transaction_document_bridge_commits() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="exact-bridge",
        )
        connection.commit()
        epoch_id = _insert_typed_header(connection, fixture, prefix="exact-open")
        _insert_direct_update(connection, fixture, epoch_id)
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
        assert connection.execute(
            """
            SELECT runtime_epoch.structural_event_id, direct_update.update_kind
            FROM groundloop_m5_runtime_epoch AS runtime_epoch
            JOIN groundloop_m4_update AS direct_update USING (epoch_id)
            WHERE runtime_epoch.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone() == ("exact-open-event", "insert")


@pytest.mark.parametrize(
    "mismatch",
    (
        "kind",
        "event",
        "runtime_predecessor",
        "direct_predecessor",
        "direct_policy",
        "manifest",
        "decision_policy",
        "registry",
        "revision",
        "state",
    ),
)
def test_active_document_bridge_rejects_each_one_field_mismatch(
    mismatch: str,
) -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix=f"mismatch-{mismatch}",
        )
        connection.commit()
        other_epoch_id = _insert_sealed_epoch(connection, f"other-{mismatch}")
        header_overrides: dict[str, object] = {}
        direct_overrides: dict[str, object] = {}
        if mismatch == "kind":
            direct_overrides["direct_kind"] = "delete"
        elif mismatch == "event":
            header_overrides["runtime_event_id"] = "wrong-event"
        elif mismatch == "runtime_predecessor":
            header_overrides["runtime_previous_epoch_id"] = other_epoch_id
        elif mismatch == "direct_predecessor":
            direct_overrides["direct_previous_epoch_id"] = other_epoch_id
        elif mismatch == "direct_policy":
            direct_overrides["direct_policy_id"] = _sha("missing-direct-policy")
        elif mismatch == "manifest":
            header_overrides["runtime_manifest_hash"] = _sha("wrong-manifest")
        elif mismatch == "decision_policy":
            header_overrides["typed_decision_policy_version"] = (
                fixture.alternate_decision_policy_version
            )
        elif mismatch == "registry":
            direct_overrides["registry_snapshot_id"] = "wrong-registry"
        elif mismatch == "revision":
            header_overrides["runtime_revision"] = 2
        elif mismatch == "state":
            header_overrides["runtime_state"] = "semantic_pending"
        epoch_id = _insert_typed_header(
            connection,
            fixture,
            prefix=f"open-{mismatch}",
            **header_overrides,
        )
        with pytest.raises(errors.RaiseException, match="exact current-transaction"):
            _insert_direct_update(
                connection,
                fixture,
                epoch_id,
                **direct_overrides,
            )
        connection.rollback()
        assert connection.execute(
            "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
            (f"open-{mismatch}-event",),
        ).fetchone() == (0,)


def test_active_bridge_rejects_reordered_and_prior_committed_sidecars() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="bridge-order",
        )
        connection.commit()
        row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (
                'reordered-event', %s, 1, 'committed', 'pending',
                'pending', 'provisional', NULL
            ) RETURNING epoch_id
            """,
            (_sha("reordered-payload"),),
        ).fetchone()
        assert row is not None
        reordered_epoch_id = int(row[0])
        connection.execute(
            """
            INSERT INTO groundloop_m5_update (
                epoch_id, update_kind, previous_published_epoch_id,
                decision_policy_version, manifest
            ) VALUES (%s, 'document_insert', %s, %s, '{}'::jsonb)
            """,
            (
                reordered_epoch_id,
                fixture.base_epoch_id,
                fixture.decision_policy_version,
            ),
        )
        with pytest.raises(errors.RaiseException, match="current-transaction sidecar"):
            _insert_direct_update(connection, fixture, reordered_epoch_id)
        connection.rollback()

        committed_epoch_id = _insert_typed_header(
            connection,
            fixture,
            prefix="committed-sidecar",
        )
        _insert_direct_update(connection, fixture, committed_epoch_id)
        connection.commit()
        with pytest.raises(errors.RaiseException, match="current-transaction sidecar"):
            _insert_direct_update(connection, fixture, committed_epoch_id)
        connection.rollback()


def test_deferred_bridge_enforces_document_bijection_and_rolls_back() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="missing-direct",
        )
        connection.commit()
        _insert_typed_header(connection, fixture, prefix="document-without-direct")
        with pytest.raises(errors.RaiseException, match="exactly one matching M4"):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()

    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="nondocument-direct",
        )
        connection.commit()
        epoch_id = _insert_typed_header(
            connection,
            fixture,
            prefix="nondocument",
            typed_kind="register_group",
        )
        connection.execute(
            """
            ALTER TABLE groundloop_m4_update
            DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard
            """
        )
        _insert_direct_update(connection, fixture, epoch_id)
        connection.execute(
            """
            ALTER TABLE groundloop_m4_update
            ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard
            """
        )
        with pytest.raises(errors.RaiseException, match="non-document"):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()

    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="bridge-rollback",
        )
        connection.commit()
        with pytest.raises(RuntimeError, match="typed-open-rollback"):
            with connection.transaction():
                epoch_id = _insert_typed_header(
                    connection,
                    fixture,
                    prefix="rolled-back-open",
                )
                _insert_direct_update(connection, fixture, epoch_id)
                raise RuntimeError("typed-open-rollback")
        assert connection.execute(
            "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
            ("rolled-back-open-event",),
        ).fetchone() == (0,)


def test_header_first_register_group_then_snapshots_commits_and_rolls_back() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="header-first",
            seed_snapshots=False,
        )
        connection.commit()
        group, requirement_snapshot_digest = _make_staged_group(
            prefix="header-first",
            owner_claim_id=fixture.owner_claim_id,
        )
        epoch_id = _insert_typed_header(
            connection,
            fixture,
            prefix="header-first-open",
            typed_kind="register_group",
            requirement_snapshot_digest=requirement_snapshot_digest,
        )
        _insert_staged_group(connection, group=group, epoch_id=epoch_id)
        _insert_header_snapshots_and_counters(
            connection,
            fixture=fixture,
            group=group,
            requirement_snapshot_digest=requirement_snapshot_digest,
            epoch_id=epoch_id,
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
        assert connection.execute(
            """
            SELECT runtime_epoch.requirement_registry_snapshot_digest,
                   requirement_snapshot.requirement_count,
                   active_snapshot.chunk_count
            FROM groundloop_m5_runtime_epoch AS runtime_epoch
            JOIN groundloop_m5_requirement_registry_snapshot
                 AS requirement_snapshot
              ON requirement_snapshot.requirement_registry_snapshot_digest =
                 runtime_epoch.requirement_registry_snapshot_digest
            JOIN groundloop_m5_active_chunk_snapshot AS active_snapshot
              ON active_snapshot.active_chunk_snapshot_digest =
                 runtime_epoch.active_chunk_snapshot_digest
            WHERE runtime_epoch.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone() == (requirement_snapshot_digest, 1, 0)

    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="header-rollback",
            seed_snapshots=False,
        )
        connection.commit()
        group, requirement_snapshot_digest = _make_staged_group(
            prefix="header-rollback",
            owner_claim_id=fixture.owner_claim_id,
        )
        with pytest.raises(RuntimeError, match="before-snapshots"):
            with connection.transaction():
                epoch_id = _insert_typed_header(
                    connection,
                    fixture,
                    prefix="header-rollback-open",
                    typed_kind="register_group",
                    requirement_snapshot_digest=requirement_snapshot_digest,
                )
                _insert_staged_group(connection, group=group, epoch_id=epoch_id)
                raise RuntimeError("before-snapshots")
        assert connection.execute(
            "SELECT count(*) FROM groundloop_epoch WHERE event_id = %s",
            ("header-rollback-open-event",),
        ).fetchone() == (0,)
        assert connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_group_version
            WHERE group_version_id = %s
            """,
            (group.group_version_id,),
        ).fetchone() == (0,)
        assert connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_requirement_registry_snapshot
            WHERE requirement_registry_snapshot_digest = %s
            """,
            (requirement_snapshot_digest,),
        ).fetchone() == (0,)


def test_deferred_root_bijection_accepts_production_root_declaration() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="root-record-fields",
            seed_snapshots=True,
        )
        connection.commit()
        group, _ = _make_staged_group(
            prefix="root-record-fields-new",
            owner_claim_id=fixture.owner_claim_id,
        )
        requirement = group.requirements[0]
        snapshot = RequirementRegistrySnapshot.build(
            (
                RequirementRegistrySnapshotEntry.build(
                    requirement_version_id=requirement.requirement_version_id,
                    group_version_id=group.group_version_id,
                    group_family_id=group.group_family_id,
                    owner_claim_id=group.owner_claim_id,
                    requirement_text=requirement.requirement_text,
                ),
            )
        )
        event = RegisterGroupEvent("root-record-fields-event", group)
        plan = M5TypedEventPlan(
            structural_event_id=event.event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=fixture.direct_policy_id,
            candidate_policy_manifest_hash=fixture.typed_policy_manifest_hash,
            requirement_registry_snapshot=snapshot,
            active_chunk_snapshot=ActiveChunkSnapshot.build(()),
            expected_previous_published_epoch_id=fixture.base_epoch_id,
        )

        receipt = PostgresM5RuntimeStore(connection).open_typed_event_atomically(plan)

        assert connection.execute(
            """
            SELECT job.job_state, scope.scope_state,
                   runtime.open_work_count, runtime.open_scope_count
            FROM groundloop_m5_runtime_epoch AS runtime
            JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
            JOIN groundloop_m5_discovery_scope AS scope USING (epoch_id)
            WHERE runtime.epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone() == ("declared", "open", 1, 1)


def test_attempt_error_hash_is_separate_and_state_checked() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="attempt-error",
            seed_snapshots=True,
        )
        connection.commit()
        group, _ = _make_staged_group(
            prefix="attempt-error-new",
            owner_claim_id=fixture.owner_claim_id,
        )
        requirement = group.requirements[0]
        snapshot = RequirementRegistrySnapshot.build(
            (
                RequirementRegistrySnapshotEntry.build(
                    requirement_version_id=requirement.requirement_version_id,
                    group_version_id=group.group_version_id,
                    group_family_id=group.group_family_id,
                    owner_claim_id=group.owner_claim_id,
                    requirement_text=requirement.requirement_text,
                ),
            )
        )
        event = RegisterGroupEvent("attempt-error-event", group)
        plan = M5TypedEventPlan(
            structural_event_id=event.event_id,
            event=event,
            payload_hash=m5_event_payload_digest(event),
            direct_plan=None,
            candidate_policy_id=fixture.direct_policy_id,
            candidate_policy_manifest_hash=fixture.typed_policy_manifest_hash,
            requirement_registry_snapshot=snapshot,
            active_chunk_snapshot=ActiveChunkSnapshot.build(()),
            expected_previous_published_epoch_id=fixture.base_epoch_id,
        )
        receipt = PostgresM5RuntimeStore(connection).open_typed_event_atomically(plan)
        job_row = connection.execute(
            """
            SELECT logical_job_id, execution_spec_hash
            FROM groundloop_m5_semantic_job
            WHERE epoch_id = %s
            """,
            (receipt.epoch_id,),
        ).fetchone()
        assert job_row is not None
        job_id = str(job_row[0]).strip()
        execution_spec_hash = str(job_row[1]).strip()
        lease_token_hash = _sha("attempt-error-lease")
        attempt_id = runtime_digests.job_attempt_id(
            job_id, 1, execution_spec_hash
        )
        connection.commit()

        with connection.transaction():
            connection.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, 1)",
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_semantic_job
                SET job_state = 'running'
                WHERE logical_job_id = %s
                """,
                (job_id,),
            )
            connection.execute(
                """
                INSERT INTO groundloop_m5_job_attempt (
                    attempt_id, logical_job_id, attempt_ordinal,
                    execution_spec_hash, lease_token_hash, attempt_state,
                    attempt_output_digest, error_hash, finished_at
                ) VALUES (%s, %s, 1, %s, %s, 'dispatched', NULL, NULL, NULL)
                """,
                (attempt_id, job_id, execution_spec_hash, lease_token_hash),
            )
            connection.execute(
                "UPDATE groundloop_epoch SET revision = 2 WHERE epoch_id = %s",
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_owner_pending_counter
                SET updated_revision = 2 WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_answer_pending_counter
                SET updated_revision = 2 WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_runtime_epoch
                SET runtime_state = 'semantic_pending', revision = 2
                WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        with pytest.raises(errors.CheckViolation):
            with connection.transaction():
                connection.execute(
                    "SELECT groundloop_m5_authorize_checked_transition(%s, 2)",
                    (receipt.epoch_id,),
                )
                connection.execute(
                    """
                    UPDATE groundloop_m5_job_attempt
                    SET attempt_state = 'failed', finished_at = now()
                    WHERE attempt_id = %s
                    """,
                    (attempt_id,),
                )

        error_hash = _sha("attempt-error-value")
        with connection.transaction():
            connection.execute(
                "SELECT groundloop_m5_authorize_checked_transition(%s, 2)",
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_job_attempt
                SET attempt_state = 'failed', error_hash = %s,
                    finished_at = now()
                WHERE attempt_id = %s
                """,
                (error_hash, attempt_id),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_semantic_job
                SET job_state = 'retryable_failed'
                WHERE logical_job_id = %s
                """,
                (job_id,),
            )
            connection.execute(
                "UPDATE groundloop_epoch SET revision = 3 WHERE epoch_id = %s",
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_owner_pending_counter
                SET updated_revision = 3 WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_answer_pending_counter
                SET updated_revision = 3 WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            )
            connection.execute(
                """
                UPDATE groundloop_m5_runtime_epoch
                SET runtime_state = 'semantic_pending', revision = 3
                WHERE epoch_id = %s
                """,
                (receipt.epoch_id,),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

        assert connection.execute(
            """
            SELECT attempt_state, attempt_output_digest, error_hash,
                   finished_at IS NOT NULL
            FROM groundloop_m5_job_attempt
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        ).fetchone() == ("failed", None, error_hash, True)


def test_failed_result_requires_call_work_and_exact_base_payload() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="missing-call",
        )
        connection.commit()
        epoch_id = _insert_typed_header(
            connection,
            fixture,
            prefix="missing-call-open",
        )
        _insert_direct_update(connection, fixture, epoch_id)
        _fail_runtime_epoch(
            connection,
            epoch_id=epoch_id,
            structural_event_id="missing-call-open-event",
            payload_hash=_sha("missing-call-open-payload"),
            include_call_work=False,
        )
        with pytest.raises(errors.RaiseException, match="lacks its call work"):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()

    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="wrong-result-payload",
        )
        connection.commit()
        epoch_id = _insert_typed_header(
            connection,
            fixture,
            prefix="wrong-payload-open",
        )
        _insert_direct_update(connection, fixture, epoch_id)
        _fail_runtime_epoch(
            connection,
            epoch_id=epoch_id,
            structural_event_id="wrong-payload-open-event",
            payload_hash=_sha("self-consistent-but-wrong-payload"),
        )
        with pytest.raises(
            errors.RaiseException,
            match="terminal base/runtime epoch",
        ):
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.rollback()


def test_failed_results_allow_equal_and_repeated_work_digests() -> None:
    with _isolated_schema() as connection:
        install_m5_runtime_bundle(connection)
        connection.commit()
        fixture = _seed_bridge_fixture(
            connection,
            activate=True,
            prefix="repeat-work",
        )
        connection.commit()
        committed_epoch_ids: list[int] = []
        for index in range(2):
            prefix = f"repeat-work-open-{index}"
            epoch_id = _insert_typed_header(connection, fixture, prefix=prefix)
            _insert_direct_update(connection, fixture, epoch_id)
            _fail_runtime_epoch(
                connection,
                epoch_id=epoch_id,
                structural_event_id=f"{prefix}-event",
                payload_hash=_sha(f"{prefix}-payload"),
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.commit()
            committed_epoch_ids.append(epoch_id)

        assert connection.execute(
            """
            SELECT count(*), count(DISTINCT work_digest),
                   count(*) FILTER (WHERE work_kind = 'event'),
                   count(*) FILTER (WHERE work_kind = 'call')
            FROM groundloop_m5_runtime_work
            WHERE epoch_id = ANY(%s::bigint[])
            """,
            (committed_epoch_ids,),
        ).fetchone() == (4, 1, 2, 2)
        assert connection.execute(
            """
            SELECT count(*)
            FROM groundloop_m5_event_result
            WHERE epoch_id = ANY(%s::bigint[])
              AND outcome = 'failed'
            """,
            (committed_epoch_ids,),
        ).fetchone() == (2,)
        assert connection.execute(
            """
            SELECT count(*)
            FROM groundloop_epoch AS base_epoch
            JOIN groundloop_m5_runtime_epoch AS runtime_epoch USING (epoch_id)
            WHERE base_epoch.epoch_id = ANY(%s::bigint[])
              AND base_epoch.structural_status = 'failed'
              AND base_epoch.semantic_status = 'failed'
              AND base_epoch.evaluation_state = 'failed'
              AND runtime_epoch.runtime_state = 'failed'
            """,
            (committed_epoch_ids,),
        ).fetchone() == (2,)
