"""Contract and live PostgreSQL tests for M5-D25 migration 017."""

from __future__ import annotations

import hashlib
import os
import re
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import Connection, IsolationLevel, sql
from psycopg.types.json import Jsonb

from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
from groundloop.m5.digests import (
    bool_field,
    enum_field,
    hash_field,
    int_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimStatus,
    ClaimSupportKind,
    CombinedClaimState,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    GroupState,
    RequirementState,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.contracts import M5RuntimeWork
from groundloop.postgres import migrations as postgres_migrations
from groundloop.postgres.migrations import (
    M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256,
    M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256,
    M5_ACCEPTED_RECOVERY_BUNDLE_ID,
    M5_ACCEPTED_RECOVERY_BUNDLE_SHA256,
    M5_ACCEPTED_RECOVERY_MIGRATION_SHA256,
    M5_ACCEPTED_RECOVERY_ORACLE_SHA256,
    M5_ACCEPTED_RECOVERY_PREREQUISITE_SHA256,
    M5_PERSISTED_MATCHING_BUNDLE_ID,
    M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS,
    M5_PERSISTED_MATCHING_MIGRATION_PATH,
    M5_RUNTIME_BUNDLE_ID,
    M5BundleHashConflictError,
    M5PersistedMatchingBundleError,
    M5PrerequisiteError,
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
    m5_persisted_matching_bundle_identity,
    m5_runtime_recovery_bundle_identity,
)

EXPECTED_LOCKS = (
    "groundloop_runtime_mode",
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_activation",
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_update",
    "groundloop_decision_policy",
    "groundloop_document",
    "groundloop_document_version",
    "groundloop_chunk_version",
    "groundloop_answer_version",
    "groundloop_claim",
    "groundloop_m5_group_family",
    "groundloop_m5_group_version",
    "groundloop_m5_requirement_version",
    "groundloop_m5_group_validity",
    "groundloop_m5_group_deactivation",
    "groundloop_m5_group_family_retirement",
    "groundloop_semantic_subject",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_claim_certificate",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_m5_requirement_state_materialized",
    "groundloop_m5_group_state_materialized",
    "groundloop_m5_claim_state_materialized",
    "groundloop_m5_answer_state_materialized",
    "groundloop_m5_published_requirement_state",
    "groundloop_m5_published_group_state",
    "groundloop_m5_published_claim_state",
    "groundloop_m5_published_answer_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_group_certificate_artifact_row",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_published_group_certificate_binding",
    "groundloop_m5_published_claim_certificate_binding",
)

D25_RELATIONS = (
    "groundloop_m5_matching_image_current",
    "groundloop_m5_matching_image_working",
    "groundloop_m5_matching_observation_current",
    "groundloop_m5_matching_observation_working",
    "groundloop_m5_matching_edge_current",
    "groundloop_m5_matching_edge_working",
    "groundloop_m5_matching_hash_mask_current",
    "groundloop_m5_matching_hash_mask_working",
    "groundloop_m5_matching_hall_current",
    "groundloop_m5_matching_hall_working",
    "groundloop_m5_matching_patch_artifact",
    "groundloop_m5_matching_work_contribution",
    "groundloop_m5_matching_work_accumulator",
)

D26_RETAINED_RESULT_TRIGGERS = (
    "groundloop_m5_event_result_children",
    "groundloop_m5_event_delta_set",
    "groundloop_m5_event_reference_set",
)

_B3_D24_ANCHOR_RELATIONS = (
    "groundloop_m5_runtime_operational_config",
    "groundloop_m5_requirement_root_provenance",
    "groundloop_m5_dispatch_record",
    "groundloop_m5_direct_terminal_projection",
    "groundloop_m5_attempt_execution_evidence",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_expired_attempt_return",
    "groundloop_m5_typed_direct_late_return_envelope",
    "groundloop_m5_post_terminal_attempt_timing",
    "groundloop_m5_post_terminal_attempt_audit",
    "groundloop_m5_postcommit_invocation_telemetry",
    "groundloop_m5_event_timing_coverage",
)

EMPTY_REQUIREMENT_SNAPSHOT_DIGEST = stable_m5_digest(
    "m5-requirement-registry-snapshot-v2", int_field(0), sequence_field(())
)
EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST = stable_m5_digest(
    "m5-active-chunk-snapshot-v2", int_field(0), sequence_field(())
)
EMPTY_REQUIREMENT_ROOT_SET_HASH = stable_m5_digest(
    "m5-requirement-root-set-v2", sequence_field(())
)


def _framed_preimage(domain: str, *fields: str) -> bytes:
    return b"".join(
        len(value.encode()).to_bytes(8, "big") + value.encode()
        for value in (domain, *fields)
    )


def _typed(tag: str, value: object) -> tuple[str, ...]:
    return (
        tag,
        "1"
        if tag == "bool" and value is True
        else "0"
        if tag == "bool"
        else str(value),
    )


def _sequence(*items: tuple[str, ...]) -> tuple[str, ...]:
    return (
        "sequence",
        "int",
        str(len(items)),
        *(field for item in items for field in item),
    )


def _change_bytes(
    domain: str,
    outer: tuple[tuple[str, ...], ...],
    before: tuple[str, ...],
    after: tuple[str, ...],
) -> bytes:
    return _framed_preimage(
        domain, *(field for item in (*outer, before, after) for field in item)
    )


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if value is None:
        pytest.skip("live PostgreSQL test database is not configured")
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _all_table_xmin_count_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, int, tuple[str, ...]], ...]:
    relations = tuple(
        str(row[0])
        for row in connection.execute(
            """SELECT tablename FROM pg_catalog.pg_tables
                WHERE schemaname=current_schema()
                ORDER BY tablename COLLATE \"C\""""
        ).fetchall()
    )
    snapshot: list[tuple[str, int, tuple[str, ...]]] = []
    for relation in relations:
        row = connection.execute(
            sql.SQL(
                "SELECT count(*),coalesce(array_agg(xmin::text ORDER BY ctid),"
                "ARRAY[]::text[]) FROM {}"
            ).format(sql.Identifier(relation))
        ).fetchone()
        assert row is not None
        snapshot.append((relation, int(row[0]), tuple(str(value) for value in row[1])))
    return tuple(snapshot)


@dataclass
class _PausedInstaller:
    thread: threading.Thread
    reached_pause: threading.Event
    resume: threading.Event
    points: list[str]
    applied_results: list[bool]
    errors: list[BaseException]


def _start_paused_installer(
    schema_name: str,
    *,
    pause_at: str,
    migration_bytes: bytes | None = None,
) -> _PausedInstaller:
    reached_pause = threading.Event()
    resume = threading.Event()
    points: list[str] = []
    applied_results: list[bool] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            with psycopg.connect(_database_url()) as connection:
                connection.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(schema_name)
                    )
                )
                connection.commit()

                def pause(point: str) -> None:
                    points.append(point)
                    if point == pause_at:
                        reached_pause.set()
                        if not resume.wait(timeout=30):
                            raise TimeoutError(
                                f"installer was not resumed after {pause_at}"
                            )

                result = install_m5_persisted_matching_bundle(
                    connection,
                    failure_injector=pause,
                    migration_bytes=migration_bytes,
                )
                applied_results.append(result.applied)
        except BaseException as error:  # retained for assertion in the test thread
            errors.append(error)

    thread = threading.Thread(
        target=run,
        name=f"migration-017-{pause_at}",
        daemon=True,
    )
    worker = _PausedInstaller(
        thread=thread,
        reached_pause=reached_pause,
        resume=resume,
        points=points,
        applied_results=applied_results,
        errors=errors,
    )
    thread.start()
    return worker


def _resume_and_join_installer(worker: _PausedInstaller) -> None:
    worker.resume.set()
    worker.thread.join(timeout=60)
    assert not worker.thread.is_alive(), "migration-017 installer did not terminate"


@contextmanager
def _pre017_schema(
    *, install_recovery: bool = True
) -> Iterator[tuple[Connection[Any], str]]:
    schema_name = f"d25_migration_017_{uuid.uuid4().hex}"
    with psycopg.connect(_database_url(), autocommit=True) as admin:
        before = tuple(
            row[0]
            for row in admin.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT LIKE 'pg_temp_%' "
                "AND schema_name NOT LIKE 'pg_toast_temp_%' "
                "ORDER BY schema_name"
            )
        )
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    connection: Connection[Any] = psycopg.connect(_database_url())
    try:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
        )
        connection.commit()
        apply_legacy_migrations(connection)
        connection.commit()
        install_m5_core_bundle(connection)
        connection.commit()
        install_m5_runtime_bundle(connection)
        connection.commit()
        if install_recovery:
            install_m5_runtime_recovery_bundle(connection)
            connection.commit()
        yield connection, schema_name
    finally:
        connection.close()
        with psycopg.connect(_database_url(), autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
            after = tuple(
                row[0]
                for row in admin.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT LIKE 'pg_temp_%' "
                    "AND schema_name NOT LIKE 'pg_toast_temp_%' "
                    "ORDER BY schema_name"
                )
            )
            assert after == before


def _seed_b2_runtime_base(
    connection: Connection[Any], prefix: str, *, revision: int = 0
) -> tuple[int, str]:
    def digest(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    base_row = connection.execute(
        """INSERT INTO groundloop_epoch
           (event_id,payload_hash,revision,structural_status,semantic_status,
            evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,%s,'committed','sealed','complete','strict',now())
           RETURNING epoch_id""",
        (f"{prefix}-base", digest(f"{prefix}-base"), revision),
    ).fetchone()
    assert base_row is not None
    base = int(base_row[0])
    policy = f"{prefix}-policy"
    model = f"{prefix}-model"
    connection.execute(
        """INSERT INTO groundloop_decision_policy
           (policy_version,support_threshold,refute_threshold,tie_rule_version,
            valid_from_epoch) VALUES (%s,.5,.5,'v1',%s)""",
        (policy, base),
    )
    connection.execute(
        """INSERT INTO groundloop_model_artifact
           (model_artifact_id,task,provider,model_id,immutable_revision,
            tokenizer_revision,license_id,config_hash)
           VALUES (%s,'embedding','fixture','fixture','v1','v1','MIT',%s)""",
        (model, digest(f"{prefix}-model")),
    )
    hashes = [
        digest(f"{prefix}-{name}")
        for name in (
            "requirement-role",
            "chunk-role",
            "vector-build",
            "vector-search",
            "lexical",
            "verifier",
        )
    ]
    manifest = stable_m5_digest(
        "m5-candidate-policy-v2",
        text_field(model),
        hash_field(hashes[0]),
        hash_field(hashes[1]),
        text_field("fixture-vector-v1"),
        enum_field("exact"),
        hash_field(hashes[2]),
        hash_field(hashes[3]),
        text_field("fixture-lexical-v1"),
        hash_field(hashes[4]),
        text_field("16.4"),
        text_field("simple"),
        text_field("rank-interleave-v1"),
        int_field(4),
        int_field(4),
        hash_field(hashes[5]),
        text_field(policy),
        bool_field(True),
    )
    connection.execute(
        """INSERT INTO groundloop_candidate_policy (
             candidate_policy_id,policy_hash,embedding_model_artifact_id,
             decision_policy_version,claim_role_template_hash,
             chunk_role_template_hash,vector_method_version,vector_index_kind,
             vector_index_build_config_hash,vector_search_config_hash,
             lexical_method_version,lexical_config_hash,lexical_postgres_version,
             lexical_regconfig_identity,claim_registry_snapshot_id,claim_count,
             fusion_version,approximate_cap_per_inserted_chunk,frontier_depth,manifest)
           VALUES (%s,%s,%s,%s,%s,%s,'fixture-vector-v1','exact',%s,%s,
             'fixture-lexical-v1',%s,'16.4','simple',%s,0,'interleave-v1',4,2,'{}')""",
        (
            manifest,
            digest(f"{prefix}-m4-policy"),
            model,
            policy,
            hashes[0],
            hashes[1],
            hashes[2],
            hashes[3],
            hashes[4],
            f"{prefix}-registry",
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_candidate_policy VALUES
           (%s,%s,%s,%s,%s,'fixture-vector-v1','exact',%s,%s,
            'fixture-lexical-v1',%s,'16.4','simple','rank-interleave-v1',
            4,4,%s,%s,true,DEFAULT)""",
        (
            manifest,
            manifest,
            model,
            hashes[0],
            hashes[1],
            hashes[2],
            hashes[3],
            hashes[4],
            hashes[5],
            policy,
        ),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_requirement_registry_snapshot "
        "VALUES (%s,0,%s,DEFAULT)",
        (EMPTY_REQUIREMENT_SNAPSHOT_DIGEST, base),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_active_chunk_snapshot VALUES
           (%s,0,%s,'m5-normalize-text-v1',%s,DEFAULT)""",
        (
            EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
            base,
            "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb",
        ),
    )
    connection.execute(
        "INSERT INTO groundloop_m4_publication_head VALUES (true,%s,now())", (base,)
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_activation(%s,%s,%s)",
        (base, revision, policy),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_matching_image_current VALUES (true,%s,%s,%s)",
        (policy, base, revision),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_publication_head VALUES (true,%s,%s,now())",
        (base, revision),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_activation VALUES (true,%s,%s,%s,now())",
        (f"{prefix}-activation", digest(f"{prefix}-activation"), base),
    )
    connection.execute(
        "UPDATE groundloop_runtime_mode SET mode='m5_active', "
        "mode_revision=mode_revision+1,updated_at=now() WHERE singleton"
    )
    question = f"{prefix}-question"
    answer = f"{prefix}-answer"
    claim = f"{prefix}-claim"
    connection.execute(
        "INSERT INTO groundloop_question VALUES (%s,'question',%s)", (question, base)
    )
    connection.execute(
        """INSERT INTO groundloop_answer_version
           (answer_version_id,question_id,text,generator_model_id,
            generator_model_version,prompt_version,created_epoch)
           VALUES (%s,%s,'answer','generator','v1','p1',%s)""",
        (answer, question, base),
    )
    connection.execute(
        """INSERT INTO groundloop_claim
           (claim_id,answer_version_id,text,extractor_model_id,
            extractor_model_version,extractor_prompt_version,required)
           VALUES (%s,%s,'claim','extractor','v1','p1',true)""",
        (claim, answer),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    return base, policy


def _open_b2_runtime_epoch(
    connection: Connection[Any],
    prefix: str,
    base: int,
    policy: str,
    *,
    direct_bridge: bool = False,
    update_kind: str | None = None,
    payload_override: str | None = None,
) -> tuple[int, str, str]:
    payload = (
        payload_override or hashlib.sha256(f"{prefix}-payload".encode()).hexdigest()
    )
    event = f"{prefix}-event"
    epoch_row = connection.execute(
        """INSERT INTO groundloop_epoch
           (event_id,payload_hash,revision,structural_status,semantic_status,
            evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,1,'committed','pending','pending','provisional',NULL)
           RETURNING epoch_id""",
        (event, payload),
    ).fetchone()
    assert epoch_row is not None
    epoch = int(epoch_row[0])
    connection.execute(
        "INSERT INTO groundloop_m5_update VALUES (%s,%s,%s,%s,'{}'::jsonb,DEFAULT)",
        (
            epoch,
            update_kind or ("document_insert" if direct_bridge else "policy_change"),
            base,
            policy,
        ),
    )
    manifest_row = connection.execute(
        "SELECT candidate_policy_id FROM groundloop_m5_candidate_policy"
    ).fetchone()
    assert manifest_row is not None
    manifest = str(manifest_row[0])
    connection.execute(
        """INSERT INTO groundloop_m5_requirement_registry_snapshot
           (requirement_registry_snapshot_digest,requirement_count,created_epoch_id)
           VALUES (%s,0,%s) ON CONFLICT DO NOTHING""",
        (EMPTY_REQUIREMENT_SNAPSHOT_DIGEST, epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_active_chunk_snapshot
           (active_chunk_snapshot_digest,chunk_count,created_epoch_id,
            normalizer_id,normalizer_provenance_hash)
           VALUES (%s,0,%s,'m5-normalize-text-v1',%s)
           ON CONFLICT DO NOTHING""",
        (
            EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
            epoch,
            "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb",
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_runtime_epoch
           (epoch_id,structural_event_id,candidate_policy_id,candidate_policy_manifest_hash,
            requirement_registry_snapshot_digest,active_chunk_snapshot_digest,
            expected_previous_published_epoch_id,requirement_root_set_hash,
            runtime_state,revision,open_work_count,open_scope_count,blocking_failure_count)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'structural_committed',1,0,0,0)""",
        (
            epoch,
            event,
            manifest,
            manifest,
            EMPTY_REQUIREMENT_SNAPSHOT_DIGEST,
            EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
            base,
            EMPTY_REQUIREMENT_ROOT_SET_HASH,
        ),
    )
    if direct_bridge:
        direct_update_kind = {
            "document_insert": "insert",
            "document_delete": "delete",
            "document_replace": "replace",
        }[update_kind or "document_insert"]
        registry_row = connection.execute(
            "SELECT claim_registry_snapshot_id FROM groundloop_candidate_policy "
            "WHERE candidate_policy_id=%s",
            (manifest,),
        ).fetchone()
        assert registry_row is not None
        connection.execute(
            """INSERT INTO groundloop_m4_update
               (epoch_id,update_kind,candidate_policy_id,previous_published_epoch_id,
                registry_snapshot_id,manifest)
               VALUES (%s,%s,%s,%s,%s,'{}')""",
            (epoch, direct_update_kind, manifest, base, str(registry_row[0])),
        )
    return epoch, event, payload


def _d26_structural_payload(
    connection: Connection[Any], *, group_id: str, event_id: str, action: str
) -> tuple[str, str | None]:
    if action == "RETIRE":
        return (
            stable_m5_digest(
                "m5-retire-group-event-v1",
                text_field(group_id),
            ),
            None,
        )
    from groundloop.m5.domain import EvidenceGroupVersion, EvidenceRequirementVersion

    family = connection.execute(
        """SELECT version.group_family_id,family.claim_id
             FROM groundloop_m5_group_version version
             JOIN groundloop_m5_group_family family USING(group_family_id)
            WHERE version.group_version_id=%s""",
        (group_id,),
    ).fetchone()
    assert family is not None
    successor_id = f"{group_id}-successor"
    requirement = EvidenceRequirementVersion(
        requirement_version_id=f"{successor_id}-requirement",
        group_version_id=successor_id,
        ordinal=0,
        requirement_text="successor requirement",
    )
    successor = EvidenceGroupVersion(
        group_version_id=successor_id,
        group_family_id=str(family[0]),
        owner_claim_id=str(family[1]),
        requirements=(requirement,),
        construction_source_id=f"{event_id}-source",
        supersedes_group_version_id=group_id,
    )
    return (
        stable_m5_digest(
            "m5-replace-group-event-v1",
            text_field(group_id),
            hash_field(successor.record_payload_hash),
        ),
        successor_id,
    )


def _seed_b2_hall_group(
    connection: Connection[Any], prefix: str, base: int
) -> tuple[str, str]:
    from groundloop.m5.domain import EvidenceGroupVersion, EvidenceRequirementVersion

    claim_row = connection.execute("SELECT claim_id FROM groundloop_claim").fetchone()
    assert claim_row is not None
    family = f"{prefix}-family"
    group = f"{prefix}-group"
    requirement = f"{prefix}-requirement"
    requirement_row = EvidenceRequirementVersion(
        requirement_version_id=requirement,
        group_version_id=group,
        ordinal=0,
        requirement_text="requirement",
    )
    group_row = EvidenceGroupVersion(
        group_version_id=group,
        group_family_id=family,
        owner_claim_id=str(claim_row[0]),
        requirements=(requirement_row,),
        construction_source_id=f"{prefix}-source",
    )
    connection.execute(
        "INSERT INTO groundloop_m5_group_family VALUES (%s,%s,%s,'PUBLISHED')",
        (family, str(claim_row[0]), base),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_group_version VALUES
           (%s,%s,%s,'PUBLISHED','support_conjunction','controlled',%s,
            NULL,NULL,NULL,NULL,%s,%s)""",
        (
            group,
            family,
            base,
            f"{prefix}-source",
            group_row.semantic_structure_hash,
            group_row.record_payload_hash,
        ),
    )
    requirement_text = "requirement"
    connection.execute(
        """INSERT INTO groundloop_m5_requirement_version VALUES
           (%s,%s,%s,'PUBLISHED',0,%s,%s,NULL,NULL,NULL,NULL)""",
        (
            requirement,
            group,
            base,
            requirement_text,
            requirement_row.requirement_text_hash,
        ),
    )
    connection.commit()
    return group, requirement


def _seed_b2_absent_transition_state(
    connection: Connection[Any], prefix: str, base: int, policy: str
) -> tuple[str, str, str, str]:
    from groundloop.m5.runtime import digests as runtime_digests

    group, requirement = _seed_b2_hall_group(connection, prefix, base)
    connection.execute(
        "INSERT INTO groundloop_m5_group_validity "
        "(group_version_id,group_family_id,claim_id,semantic_structure_hash,"
        "supersedes_group_version_id,valid_from_epoch,valid_to_epoch) "
        "VALUES (%s,(SELECT group_family_id FROM groundloop_m5_group_version "
        "WHERE group_version_id=%s),(SELECT family.claim_id FROM "
        "groundloop_m5_group_version version JOIN groundloop_m5_group_family family "
        "USING(group_family_id) WHERE version.group_version_id=%s),"
        "(SELECT semantic_structure_hash FROM groundloop_m5_group_version "
        "WHERE group_version_id=%s),NULL,%s,NULL)",
        (group, group, group, group, base),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_published_requirement_state "
        "VALUES (%s,%s,NULL,0,ARRAY[]::text[],ARRAY[]::text[],0,false,%s)",
        (requirement, base, policy),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_published_group_state "
        "VALUES (%s,%s,NULL,0,1,0,0,false,%s,NULL)",
        (group, base, policy),
    )
    connection.commit()
    requirement_digest = runtime_digests.requirement_state_artifact_digest(
        requirement_version_id=requirement,
        witness_hashes=(),
        supporting_observation_ids=(),
        witness_count=0,
        satisfied=False,
        decision_policy_version=policy,
    )
    group_digest = runtime_digests.group_state_artifact_digest(
        group_version_id=group,
        requirement_count=1,
        satisfied_count=0,
        matching_size=0,
        complete=False,
        decision_policy_version=policy,
        certificate_digest=None,
    )
    return group, requirement, group_digest, requirement_digest


@dataclass(frozen=True, slots=True)
class _D26Predecessor:
    group_id: str
    family_id: str
    requirement_ids: tuple[str, ...]
    requirement_state_hashes: tuple[str, ...]
    group_state_hash: str
    certificate_digest: str | None


@dataclass(frozen=True, slots=True)
class _D26PresentState:
    kind: str
    object_id: str
    before_digest: str | None
    after_digest: str
    after_value: object


def _d26_predecessor_from_published_image(
    connection: Connection[Any],
    *,
    group_id: str,
    base: int,
) -> _D26Predecessor:
    group_row = connection.execute(
        """SELECT version.group_family_id,state.requirement_count,
                  state.satisfied_count,state.matching_size,state.complete,
                  state.decision_policy_version,state.certificate_digest
             FROM groundloop_m5_group_version version
             JOIN groundloop_m5_published_group_state state
               ON state.group_version_id=version.group_version_id
            WHERE version.group_version_id=%s
              AND state.valid_from_epoch<=%s
              AND (state.valid_to_epoch IS NULL OR %s<state.valid_to_epoch)""",
        (group_id, base, base),
    ).fetchone()
    assert group_row is not None
    requirement_rows = connection.execute(
        """SELECT requirement.requirement_version_id,state.witness_hashes,
                  state.supporting_observation_ids,state.witness_count,
                  state.satisfied,state.decision_policy_version
             FROM groundloop_m5_requirement_version requirement
             JOIN groundloop_m5_published_requirement_state state
               ON state.requirement_version_id=requirement.requirement_version_id
            WHERE requirement.group_version_id=%s
              AND state.valid_from_epoch<=%s
              AND (state.valid_to_epoch IS NULL OR %s<state.valid_to_epoch)
            ORDER BY requirement.ordinal""",
        (group_id, base, base),
    ).fetchall()
    assert len(requirement_rows) == int(group_row[1])
    requirement_ids = tuple(str(row[0]) for row in requirement_rows)
    requirement_hashes = tuple(
        runtime_digests.requirement_state_artifact_digest(
            requirement_version_id=str(row[0]),
            witness_hashes=tuple(str(value) for value in row[1]),
            supporting_observation_ids=tuple(str(value) for value in row[2]),
            witness_count=int(row[3]),
            satisfied=bool(row[4]),
            decision_policy_version=str(row[5]),
        )
        for row in requirement_rows
    )
    certificate_digest = None if group_row[6] is None else str(group_row[6])
    binding = connection.execute(
        """SELECT certificate_digest
             FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id=%s AND valid_from_epoch<=%s
              AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)""",
        (group_id, base, base),
    ).fetchone()
    assert (binding is None) == (certificate_digest is None)
    if binding is not None:
        assert str(binding[0]) == certificate_digest
    return _D26Predecessor(
        group_id=group_id,
        family_id=str(group_row[0]),
        requirement_ids=requirement_ids,
        requirement_state_hashes=requirement_hashes,
        group_state_hash=runtime_digests.group_state_artifact_digest(
            group_version_id=group_id,
            requirement_count=int(group_row[1]),
            satisfied_count=int(group_row[2]),
            matching_size=int(group_row[3]),
            complete=bool(group_row[4]),
            decision_policy_version=str(group_row[5]),
            certificate_digest=certificate_digest,
        ),
        certificate_digest=certificate_digest,
    )


def _seed_d26_predecessor(
    connection: Connection[Any],
    *,
    prefix: str,
    base: int,
    base_revision: int,
    policy: str,
    complete: bool,
) -> _D26Predecessor:
    """Install a valid two-requirement historical predecessor over empty D25 state.

    Migration 017 deliberately guards new certificate-artifact inserts through
    transition journalling.  This fixture needs an already-published predecessor,
    so it disables only those two insert-journal triggers while loading the valid
    historical artifact, then restores them before the event under test.  The D26
    validator independently re-proves every artifact row and eligibility input.
    """

    from m5.postgres.helpers import (
        insert_observation,
        insert_published_group,
        install_current_currency,
        make_group,
    )

    claim_row = connection.execute("SELECT claim_id FROM groundloop_claim").fetchone()
    assert claim_row is not None
    group = make_group(
        group_id=f"{prefix}-group",
        family_id=f"{prefix}-family",
        claim_id=str(claim_row[0]),
        texts=("alpha evidence", "beta evidence"),
        source_id=f"{prefix}-source",
    )
    insert_published_group(connection, group=group, epoch_id=base)
    requirement_ids = tuple(
        requirement.requirement_version_id for requirement in group.requirements
    )
    text_hashes = tuple(
        hashlib.sha256(text.encode()).hexdigest()
        for text in ("alpha evidence", "beta evidence")
    )
    observation_ids: tuple[str, ...] = ()
    certificate_digest: str | None = None
    if complete:
        document_id = f"{prefix}-document"
        document_version_id = f"{prefix}-document-v1"
        connection.execute(
            "INSERT INTO groundloop_document VALUES (%s,%s,'fixture')",
            (document_id, f"fixture://{document_id}"),
        )
        connection.execute(
            "INSERT INTO groundloop_document_version VALUES (%s,%s,%s,%s,NULL)",
            (
                document_version_id,
                document_id,
                hashlib.sha256(f"{prefix}-content".encode()).hexdigest(),
                base,
            ),
        )
        chunk_ids = tuple(f"{prefix}-chunk-{ordinal}" for ordinal in range(2))
        observation_ids = tuple(
            f"{prefix}-observation-{ordinal}" for ordinal in range(2)
        )
        fixture_rows = zip(
            chunk_ids,
            ("alpha evidence", "beta evidence"),
            text_hashes,
            requirement_ids,
            observation_ids,
            strict=True,
        )
        for ordinal, (
            chunk_id,
            text,
            text_hash,
            requirement_id,
            observation_id,
        ) in enumerate(fixture_rows):
            connection.execute(
                """INSERT INTO groundloop_chunk_version
                   (chunk_version_id,document_version_id,chunk_index,text,text_hash,
                    chunker_version,valid_from_epoch,valid_to_epoch)
                   VALUES (%s,%s,%s,%s,%s,'fixture-v1',%s,NULL)""",
                (chunk_id, document_version_id, ordinal, text, text_hash, base),
            )
            insert_observation(
                connection,
                observation_id=observation_id,
                subject_kind="requirement",
                subject_id=requirement_id,
                chunk_id=chunk_id,
                produced_epoch=base,
            )
            install_current_currency(
                connection,
                observation_id=observation_id,
                subject_kind="requirement",
                subject_id=requirement_id,
                chunk_id=chunk_id,
                task_type="verify_requirement_v1",
                epoch_id=base,
                revision=base_revision,
            )
        artifact = GroupMatchingCertificateArtifact(
            decision_policy_version=policy,
            group_version_id=group.group_version_id,
            rows=tuple(
                GroupCertificateRow(
                    requirement_ordinal=ordinal,
                    requirement_version_id=requirement_id,
                    text_hash=text_hash,
                    selected_observation_id=observation_id,
                )
                for ordinal, (requirement_id, text_hash, observation_id) in enumerate(
                    zip(
                        requirement_ids,
                        text_hashes,
                        observation_ids,
                        strict=True,
                    )
                )
            ),
        )
        certificate_digest = artifact.certificate_digest
        connection.execute(
            "ALTER TABLE groundloop_m5_group_certificate_artifact DISABLE TRIGGER "
            "groundloop_m5_group_certificate_artifact_d25_journal"
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_group_certificate_artifact_row DISABLE TRIGGER "
            "groundloop_m5_group_certificate_row_d25_journal"
        )
        connection.execute(
            """INSERT INTO groundloop_m5_group_certificate_artifact
               VALUES (%s,%s,'m5-group-certificate-v1',%s,2)""",
            (certificate_digest, policy, group.group_version_id),
        )
        for row in artifact.rows:
            connection.execute(
                "INSERT INTO groundloop_m5_group_certificate_artifact_row "
                "VALUES (%s,%s,%s,%s,%s)",
                (
                    certificate_digest,
                    row.requirement_ordinal,
                    row.requirement_version_id,
                    row.text_hash,
                    row.selected_observation_id,
                ),
            )
        connection.execute(
            "ALTER TABLE groundloop_m5_group_certificate_artifact ENABLE TRIGGER "
            "groundloop_m5_group_certificate_artifact_d25_journal"
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_group_certificate_artifact_row ENABLE TRIGGER "
            "groundloop_m5_group_certificate_row_d25_journal"
        )

    requirement_state_hashes: list[str] = []
    for ordinal, requirement_id in enumerate(requirement_ids):
        witnesses = [text_hashes[ordinal]] if complete else []
        observations = [observation_ids[ordinal]] if complete else []
        connection.execute(
            """INSERT INTO groundloop_m5_published_requirement_state
               (requirement_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                witness_hashes,supporting_observation_ids,witness_count,satisfied,
                decision_policy_version)
               VALUES (%s,%s,NULL,%s,%s,%s,%s,%s,%s)""",
            (
                requirement_id,
                base,
                base_revision,
                witnesses,
                observations,
                int(complete),
                complete,
                policy,
            ),
        )
        requirement_state_hashes.append(
            runtime_digests.requirement_state_artifact_digest(
                requirement_version_id=requirement_id,
                witness_hashes=witnesses,
                supporting_observation_ids=observations,
                witness_count=int(complete),
                satisfied=complete,
                decision_policy_version=policy,
            )
        )
    connection.execute(
        """INSERT INTO groundloop_m5_published_group_state
           (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            requirement_count,satisfied_count,matching_size,complete,
            decision_policy_version,certificate_digest)
           VALUES (%s,%s,NULL,%s,2,%s,%s,%s,%s,%s)""",
        (
            group.group_version_id,
            base,
            base_revision,
            2 if complete else 0,
            2 if complete else 0,
            complete,
            policy,
            certificate_digest,
        ),
    )
    if certificate_digest is not None:
        connection.execute(
            """INSERT INTO groundloop_m5_published_group_certificate_binding
               (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                certificate_digest) VALUES (%s,%s,NULL,%s,%s)""",
            (group.group_version_id, base, base_revision, certificate_digest),
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    group_state_hash = runtime_digests.group_state_artifact_digest(
        group_version_id=group.group_version_id,
        requirement_count=2,
        satisfied_count=2 if complete else 0,
        matching_size=2 if complete else 0,
        complete=complete,
        decision_policy_version=policy,
        certificate_digest=certificate_digest,
    )
    return _D26Predecessor(
        group_id=group.group_version_id,
        family_id=group.group_family_id,
        requirement_ids=requirement_ids,
        requirement_state_hashes=tuple(requirement_state_hashes),
        group_state_hash=group_state_hash,
        certificate_digest=certificate_digest,
    )


@dataclass(frozen=True, slots=True)
class _D26SealedEvent:
    epoch_id: int
    revision: int
    event_id: str
    payload_hash: str
    publication_id: str
    logical_result_hash: str
    references: tuple[tuple[str, str, str, str], ...]


def _insert_d26_zero_runtime_accounting_point(
    connection: Connection[Any], *, epoch: int, revision: int
) -> M5RuntimeWork:
    zero = M5RuntimeWork()
    counter_names = M5RuntimeWork.counter_names()
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_accumulator ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(
                map(
                    sql.Identifier,
                    ("work_digest", "epoch_id", *counter_names, "updated_revision"),
                )
            ),
            sql.SQL(",").join(
                sql.Placeholder()
                for _ in ("work_digest", "epoch_id", *counter_names, "updated_revision")
            ),
        ),
        (zero.work_digest, epoch, *zero.counter_values(), revision),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_runtime_timing_accumulator "
        "(epoch_id,updated_revision) VALUES (%s,%s)",
        (epoch, revision),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    return zero


def _insert_d26_runtime_work_row(
    connection: Connection[Any],
    *,
    work: M5RuntimeWork,
    epoch: int,
    event: str,
    kind: str,
) -> None:
    columns = (
        "work_digest",
        "structural_event_id",
        "epoch_id",
        "work_kind",
        *M5RuntimeWork.counter_names(),
    )
    connection.execute(
        sql.SQL("INSERT INTO groundloop_m5_runtime_work ({}) VALUES ({})").format(
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
        ),
        (work.work_digest, event, epoch, kind, *work.counter_values()),
    )


def _seal_d26_event(
    connection: Connection[Any],
    *,
    prefix: str,
    base: int,
    base_revision: int,
    policy: str,
    predecessor: _D26Predecessor,
    action: str,
    retire_current_physical: bool = False,
    structural_mutation: str | None = None,
    logical_mutation: str | None = None,
    reference_mutation: str | None = None,
    seal_mutation: str | None = None,
    child_mutation: str | None = None,
    alternate_certificate_digest: str | None = None,
    force_result_constraint_only: bool = False,
) -> _D26SealedEvent:
    wrong_close_epoch: int | None = None
    if seal_mutation in {
        "wrong_group_validity_close",
        "wrong_requirement_state_close",
        "wrong_group_state_close",
        "wrong_certificate_binding_close",
    }:
        wrong_close_epoch = _b3_future_epoch(
            connection,
            f"{prefix}-wrong-close-epoch",
        )
        connection.commit()
    not_cover_start_epoch: int | None = None
    if child_mutation == "validity_not_covering_previous_head":
        not_cover_start_epoch = _b3_future_epoch(
            connection,
            f"{prefix}-not-cover-start",
        )
        connection.commit()
    wrong_deactivation_epoch: int | None = None
    if child_mutation == "wrong_deactivation_epoch":
        wrong_deactivation_epoch = _b3_future_epoch(
            connection,
            f"{prefix}-wrong-deactivation-epoch",
        )
        connection.execute(
            "INSERT INTO groundloop_m5_update VALUES "
            "(%s,'policy_change',%s,%s,'{}'::jsonb,DEFAULT)",
            (wrong_deactivation_epoch, base, policy),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
    open_prefix = f"{prefix}-open"
    event = f"{open_prefix}-event"
    payload, successor = _d26_structural_payload(
        connection,
        group_id=predecessor.group_id,
        event_id=event,
        action=action,
    )
    deactivation_group = predecessor.group_id
    if structural_mutation == "wrong_predecessor":
        other_group = connection.execute(
            """SELECT group_version_id FROM groundloop_m5_group_version
                WHERE lifecycle_state='PUBLISHED' AND group_version_id<>%s
                ORDER BY group_version_id COLLATE "C" LIMIT 1""",
            (predecessor.group_id,),
        ).fetchone()
        assert other_group is not None
        deactivation_group = str(other_group[0])
        payload = stable_m5_digest(
            "m5-retire-group-event-v1",
            text_field(deactivation_group),
        )
    elif structural_mutation == "wrong_structural_payload":
        payload = hashlib.sha256(f"{event}-wrong-payload".encode()).hexdigest()
    elif structural_mutation == "wrong_successor_record_payload":
        assert action == "REPLACE"
        payload = stable_m5_digest(
            "m5-replace-group-event-v1",
            text_field(predecessor.group_id),
            hash_field("0" * 64),
        )
    update_kind = "retire_group" if action == "RETIRE" else "replace_group"
    if structural_mutation is not None and structural_mutation.startswith(
        "update_kind:"
    ):
        update_kind = structural_mutation.partition(":")[2]
    epoch, actual_event, actual_payload = _open_b2_runtime_epoch(
        connection,
        open_prefix,
        base,
        policy,
        direct_bridge=False,
        update_kind=update_kind,
        payload_override=payload,
    )
    assert (actual_event, actual_payload) == (event, payload)
    _stage_b2_group_deactivation(
        connection,
        epoch=epoch,
        event=event,
        group=deactivation_group,
        action=action,
        mutation=(
            structural_mutation
            if structural_mutation
            in {
                "wrong_event",
                "wrong_epoch",
                "wrong_action",
                "wrong_successor",
                "second_deactivation",
            }
            else None
        ),
    )
    if structural_mutation == "wrong_successor_record_payload":
        assert successor is not None
        connection.execute(
            "ALTER TABLE groundloop_m5_group_version DISABLE TRIGGER "
            "groundloop_m5_group_version_lifecycle_guard"
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_group_version DISABLE TRIGGER "
            "groundloop_m5_group_version_integrity"
        )
        connection.execute(
            "UPDATE groundloop_m5_group_version SET record_payload_hash=%s "
            "WHERE group_version_id=%s",
            ("0" * 64, successor),
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_group_version ENABLE TRIGGER "
            "groundloop_m5_group_version_lifecycle_guard"
        )
        connection.execute(
            "ALTER TABLE groundloop_m5_group_version ENABLE TRIGGER "
            "groundloop_m5_group_version_integrity"
        )
    absent_states = [
        (
            "requirement_state",
            requirement_id,
            state_hash,
        )
        for requirement_id, state_hash in zip(
            predecessor.requirement_ids,
            predecessor.requirement_state_hashes,
            strict=True,
        )
    ]
    absent_states.append(
        ("group_state", predecessor.group_id, predecessor.group_state_hash)
    )
    if predecessor.certificate_digest is not None:
        absent_states.append(
            (
                "group_certificate",
                predecessor.group_id,
                predecessor.certificate_digest,
            )
        )
    present_states: tuple[_D26PresentState, ...] = ()
    claim_bindings: tuple[WorkingClaimCertificateBinding, ...] = ()
    successor_hall: tuple[str, str] | None = None
    if successor is not None:
        successor_requirement = f"{successor}-requirement"
        requirement_value = RequirementState(
            requirement_version_id=successor_requirement,
            witness_hashes=(),
            supporting_observation_ids=(),
            witness_count=0,
            satisfied=False,
        )
        group_value = GroupState(
            group_version_id=successor,
            requirement_count=1,
            satisfied_count=0,
            matching_size=0,
            complete=False,
        )
        present_states = (
            _D26PresentState(
                kind="requirement_state",
                object_id=successor_requirement,
                before_digest=None,
                after_digest=runtime_digests.requirement_state_artifact_digest(
                    requirement_version_id=successor_requirement,
                    witness_hashes=(),
                    supporting_observation_ids=(),
                    witness_count=0,
                    satisfied=False,
                    decision_policy_version=policy,
                ),
                after_value=requirement_value,
            ),
            _D26PresentState(
                kind="group_state",
                object_id=successor,
                before_digest=None,
                after_digest=runtime_digests.group_state_artifact_digest(
                    group_version_id=successor,
                    requirement_count=1,
                    satisfied_count=0,
                    matching_size=0,
                    complete=False,
                    decision_policy_version=policy,
                    certificate_digest=None,
                ),
                after_value=group_value,
            ),
        )
        successor_hall = (successor, successor_requirement)
    if predecessor.certificate_digest is not None:
        claim_row = connection.execute(
            """SELECT family.claim_id,state.support_count,state.refute_count,
                      state.best_support_score,state.best_refute_score,
                      state.supporting_observation_ids,
                      state.refuting_observation_ids,state.complete_group_count,
                      state.complete_group_ids,state.status,
                      state.decision_policy_version,state.certificate_digest,
                      artifact.support_kind,artifact.group_version_id
                 FROM groundloop_m5_group_version version
                 JOIN groundloop_m5_group_family family USING(group_family_id)
                 JOIN groundloop_m5_published_claim_state state
                   ON state.claim_id=family.claim_id
                 JOIN groundloop_m5_claim_certificate_artifact artifact
                   ON artifact.certificate_digest=state.certificate_digest
                WHERE version.group_version_id=%s
                  AND state.valid_from_epoch<=%s
                  AND (state.valid_to_epoch IS NULL OR %s<state.valid_to_epoch)""",
            (predecessor.group_id, base, base),
        ).fetchone()
        assert claim_row is not None
        claim_id = str(claim_row[0])
        support_count = int(claim_row[1])
        refute_count = int(claim_row[2])
        complete_group_ids = tuple(str(value) for value in claim_row[8])
        assert predecessor.group_id in complete_group_ids
        assert support_count == 0
        assert str(claim_row[12]) == "group"
        assert str(claim_row[13]) == predecessor.group_id
        remaining_group_ids = tuple(
            group_id
            for group_id in complete_group_ids
            if group_id != predecessor.group_id
        )
        assert remaining_group_ids
        selected_group_id = remaining_group_ids[0]
        selected_group_certificate = connection.execute(
            """SELECT binding.certificate_digest
                 FROM groundloop_m5_published_group_certificate_binding binding
                 JOIN groundloop_m5_published_group_state state
                   ON state.group_version_id=binding.group_version_id
                  AND state.valid_from_epoch=binding.valid_from_epoch
                  AND state.certificate_digest=binding.certificate_digest
                WHERE binding.group_version_id=%s
                  AND binding.valid_from_epoch<=%s
                  AND (binding.valid_to_epoch IS NULL OR %s<binding.valid_to_epoch)
                  AND state.complete""",
            (selected_group_id, base, base),
        ).fetchone()
        assert selected_group_certificate is not None
        refuting_observation_ids = tuple(str(value) for value in claim_row[6])
        claim_certificate = ClaimCertificateArtifact(
            claim_id=claim_id,
            decision_policy_version=str(claim_row[10]),
            support_kind=ClaimSupportKind.GROUP,
            group_version_id=selected_group_id,
            group_certificate_digest=str(selected_group_certificate[0]),
            direct_refute_observation_id=(
                refuting_observation_ids[0] if refuting_observation_ids else None
            ),
        )
        old_certificate_digest = str(claim_row[11])
        certificate_digest = claim_certificate.certificate_digest
        assert certificate_digest != old_certificate_digest
        supported = support_count > 0 or bool(remaining_group_ids)
        refuted = refute_count > 0
        if supported and refuted:
            status = ClaimStatus.CONFLICTED
        elif supported:
            status = ClaimStatus.SUPPORTED
        elif refuted:
            status = ClaimStatus.REFUTED
        else:
            status = ClaimStatus.UNSUPPORTED
        claim_state = CombinedClaimState(
            claim_id=claim_id,
            support_count=support_count,
            refute_count=refute_count,
            best_support_score=(None if claim_row[3] is None else float(claim_row[3])),
            best_refute_score=(None if claim_row[4] is None else float(claim_row[4])),
            supporting_observation_ids=tuple(str(value) for value in claim_row[5]),
            refuting_observation_ids=refuting_observation_ids,
            complete_group_count=len(remaining_group_ids),
            complete_group_ids=remaining_group_ids,
            status=status,
        )
        claim_digest_fields = {
            "claim_id": claim_id,
            "support_count": support_count,
            "refute_count": refute_count,
            "best_support_score": claim_state.best_support_score,
            "best_refute_score": claim_state.best_refute_score,
            "supporting_observation_ids": claim_state.supporting_observation_ids,
            "refuting_observation_ids": claim_state.refuting_observation_ids,
            "status": status,
            "decision_policy_version": str(claim_row[10]),
        }
        before_claim_digest = runtime_digests.claim_state_artifact_digest(
            **claim_digest_fields,
            complete_group_count=int(claim_row[7]),
            complete_group_ids=complete_group_ids,
            certificate_digest=old_certificate_digest,
        )
        after_claim_digest = runtime_digests.claim_state_artifact_digest(
            **claim_digest_fields,
            complete_group_count=claim_state.complete_group_count,
            complete_group_ids=claim_state.complete_group_ids,
            certificate_digest=certificate_digest,
        )
        assert before_claim_digest != after_claim_digest
        present_states += (
            _D26PresentState(
                kind="claim_state",
                object_id=claim_id,
                before_digest=before_claim_digest,
                after_digest=after_claim_digest,
                after_value=claim_state,
            ),
            _D26PresentState(
                kind="claim_certificate",
                object_id=claim_id,
                before_digest=old_certificate_digest,
                after_digest=certificate_digest,
                after_value=claim_certificate,
            ),
        )
        claim_bindings = (
            WorkingClaimCertificateBinding(
                epoch_id=epoch,
                claim_id=claim_id,
                valid_from_revision=1,
                valid_to_revision=None,
                certificate_digest=certificate_digest,
            ),
        )
    _apply_empty_b2_structural(
        connection,
        epoch=epoch,
        event=event,
        payload=payload,
        base=base,
        base_revision=base_revision,
        policy=policy,
        hall_group=successor_hall,
        absent_states=tuple(absent_states),
        present_states=present_states,
        claim_bindings=claim_bindings,
        logical_mutation=logical_mutation,
        remove_current_group=(
            predecessor.group_id if retire_current_physical else None
        ),
    )
    connection.commit()

    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,1)", (epoch,)
    )
    connection.execute(
        "UPDATE groundloop_epoch SET revision=2 WHERE epoch_id=%s AND revision=1",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_runtime_epoch "
        "SET runtime_state='semantic_pending',revision=2 "
        "WHERE epoch_id=%s AND revision=1",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_owner_pending_counter "
        "SET updated_revision=2 WHERE epoch_id=%s",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_answer_pending_counter "
        "SET updated_revision=2 WHERE epoch_id=%s",
        (epoch,),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()

    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,2)", (epoch,)
    )
    connection.execute(
        "UPDATE groundloop_epoch "
        "SET revision=3,semantic_status='complete',evaluation_state='complete' "
        "WHERE epoch_id=%s AND revision=2",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_runtime_epoch "
        "SET runtime_state='semantic_complete',revision=3 "
        "WHERE epoch_id=%s AND revision=2",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_owner_pending_counter "
        "SET updated_revision=3 WHERE epoch_id=%s",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_answer_pending_counter "
        "SET updated_revision=3 WHERE epoch_id=%s",
        (epoch,),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    zero_work = _insert_d26_zero_runtime_accounting_point(
        connection,
        epoch=epoch,
        revision=3,
    )

    sealed_revision = 4
    state_rows = [
        (
            kind,
            object_id,
            stable_m5_digest(
                "m5-changed-state-absence-artifact-v1",
                enum_field(kind),
                text_field(object_id),
            ),
        )
        for kind, object_id, _ in absent_states
    ]
    state_rows.extend(
        (state.kind, state.object_id, state.after_digest) for state in present_states
    )
    if reference_mutation == "fabricated_certificate_absence":
        state_rows.append(
            (
                "group_certificate",
                predecessor.group_id,
                stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    enum_field("group_certificate"),
                    text_field(predecessor.group_id),
                ),
            )
        )
    elif reference_mutation in {
        "excluded_claim_state",
        "excluded_answer_state",
        "excluded_claim_certificate",
    }:
        excluded_kind = reference_mutation.removeprefix("excluded_")
        if excluded_kind == "answer_state":
            excluded_row = connection.execute(
                "SELECT answer_version_id FROM groundloop_answer_version "
                'ORDER BY answer_version_id COLLATE "C" LIMIT 1'
            ).fetchone()
        else:
            excluded_row = connection.execute(
                "SELECT claim_id FROM groundloop_claim "
                'ORDER BY claim_id COLLATE "C" LIMIT 1'
            ).fetchone()
        assert excluded_row is not None
        excluded_object_id = str(excluded_row[0])
        state_rows.append(
            (
                excluded_kind,
                excluded_object_id,
                stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    enum_field(excluded_kind),
                    text_field(excluded_object_id),
                ),
            )
        )
    elif reference_mutation == "extra_absence":
        extra_row = connection.execute(
            """SELECT requirement_version_id
                 FROM groundloop_m5_requirement_version
                WHERE lifecycle_state='PUBLISHED'
                  AND group_version_id<>%s
                ORDER BY requirement_version_id COLLATE "C" LIMIT 1""",
            (predecessor.group_id,),
        ).fetchone()
        assert extra_row is not None
        extra_requirement = str(extra_row[0])
        state_rows.append(
            (
                "requirement_state",
                extra_requirement,
                stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    enum_field("requirement_state"),
                    text_field(extra_requirement),
                ),
            )
        )
    ordered_state_rows = list(sorted(state_rows, key=lambda row: (row[0], row[1])))
    if reference_mutation == "missing_absence":
        ordered_state_rows.pop(0)
    elif reference_mutation == "reordered":
        ordered_state_rows.reverse()
    elif reference_mutation == "duplicate":
        ordered_state_rows.append(ordered_state_rows[0])
    stored_references: list[tuple[str, str, int, int, str, str]] = []
    for position, (kind, object_id, original_state_hash) in enumerate(
        ordered_state_rows
    ):
        reference_epoch = epoch
        reference_revision = sealed_revision
        state_hash = original_state_hash
        if position == 0 and reference_mutation == "wrong_state_hash":
            state_hash = "0" * 64
        if position == 0 and reference_mutation == "wrong_epoch":
            reference_epoch = base
        if position == 0 and reference_mutation == "wrong_revision":
            reference_revision = sealed_revision - 1
        reference_digest = runtime_digests.changed_state_reference_digest(
            (kind, object_id, reference_epoch, reference_revision, state_hash)
        )
        if position == 0 and reference_mutation == "wrong_reference_digest":
            reference_digest = "f" * 64
        stored_references.append(
            (
                kind,
                object_id,
                reference_epoch,
                reference_revision,
                state_hash,
                reference_digest,
            )
        )
    references = tuple(
        (
            kind,
            object_id,
            state_hash,
            reference_digest,
        )
        for kind, object_id, _, _, state_hash, reference_digest in stored_references
    )
    changed_state_hash = runtime_digests.changed_state_set_digest(
        reference[3] for reference in references
    )
    combined_hash = stable_m5_digest(
        "m5-combined-status-delta-set-v2", sequence_field(())
    )
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch))
    open_receipt = runtime_digests.open_event_receipt_binding_digest(
        epoch_id=epoch,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    publication_receipt = runtime_digests.publication_receipt_binding_digest(
        epoch_id=epoch,
        publication_id=publication_id,
        replayed=False,
    )
    logical_result = runtime_digests.event_run_logical_result_digest(
        event_id=event,
        payload_hash=payload,
        epoch_id=epoch,
        sealed_or_failed_outcome="sealed",
        original_open_receipt_binding_hash=open_receipt,
        original_publication_receipt_binding_hash=publication_receipt,
        event_work_digest=zero_work.work_digest,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_state_hash,
        failure_reason=None,
    )
    seal_source = runtime_digests.seal_contribution_source_digest(
        structural_event_id=event,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_state_hash,
        publication_id=publication_id,
    )
    contribution_key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=epoch,
        contribution_kind="seal",
        source_id=event,
    )

    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,3)", (epoch,)
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_seal(%s,3,%s)",
        (epoch, sealed_revision),
    )
    contribution_columns = (
        "work_digest",
        "epoch_id",
        *M5RuntimeWork.counter_names(),
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, contribution_columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in contribution_columns),
        ),
        (
            zero_work.work_digest,
            epoch,
            *zero_work.counter_values(),
            "seal",
            event,
            seal_source,
            contribution_key,
            sealed_revision,
        ),
    )
    if seal_mutation != "open_group_validity":
        connection.execute(
            "UPDATE groundloop_m5_group_validity SET valid_to_epoch=%s "
            "WHERE group_version_id=%s AND valid_to_epoch IS NULL",
            (
                wrong_close_epoch
                if seal_mutation == "wrong_group_validity_close"
                else epoch,
                predecessor.group_id,
            ),
        )
    if seal_mutation != "open_requirement_state":
        connection.execute(
            "UPDATE groundloop_m5_published_requirement_state "
            "SET valid_to_epoch=%s "
            "WHERE requirement_version_id=ANY(%s) AND valid_to_epoch IS NULL",
            (
                wrong_close_epoch
                if seal_mutation == "wrong_requirement_state_close"
                else epoch,
                list(predecessor.requirement_ids),
            ),
        )
    connection.execute(
        "DELETE FROM groundloop_m5_requirement_state_materialized "
        "WHERE requirement_version_id=ANY(%s)",
        (list(predecessor.requirement_ids),),
    )
    connection.execute(
        "DELETE FROM groundloop_m5_group_state_materialized WHERE group_version_id=%s",
        (predecessor.group_id,),
    )
    claim_states = tuple(
        state for state in present_states if state.kind == "claim_state"
    )
    for state in claim_states:
        claim_state = state.after_value
        assert isinstance(claim_state, CombinedClaimState)
        connection.execute(
            "UPDATE groundloop_m5_published_claim_state SET valid_to_epoch=%s "
            "WHERE claim_id=%s AND valid_to_epoch IS NULL",
            (epoch, claim_state.claim_id),
        )
        connection.execute(
            """UPDATE groundloop_m5_claim_state_materialized
                  SET support_count=%s,refute_count=%s,best_support_score=%s,
                      best_refute_score=%s,supporting_observation_ids=%s,
                      refuting_observation_ids=%s,complete_group_count=%s,
                      complete_group_ids=%s,status=%s,
                      decision_policy_version=%s,certificate_digest=%s,
                      updated_epoch=%s,updated_revision=%s
                WHERE claim_id=%s""",
            (
                claim_state.support_count,
                claim_state.refute_count,
                claim_state.best_support_score,
                claim_state.best_refute_score,
                list(claim_state.supporting_observation_ids),
                list(claim_state.refuting_observation_ids),
                claim_state.complete_group_count,
                list(claim_state.complete_group_ids),
                claim_state.status.value,
                policy,
                next(
                    state.after_digest
                    for state in present_states
                    if state.kind == "claim_certificate"
                    and state.object_id == claim_state.claim_id
                ),
                epoch,
                sealed_revision,
                claim_state.claim_id,
            ),
        )
        connection.execute(
            "UPDATE groundloop_m5_published_claim_certificate_binding "
            "SET valid_to_epoch=%s WHERE claim_id=%s AND valid_to_epoch IS NULL",
            (epoch, claim_state.claim_id),
        )
    if seal_mutation != "open_group_state":
        connection.execute(
            "UPDATE groundloop_m5_published_group_state SET valid_to_epoch=%s "
            "WHERE group_version_id=%s AND valid_to_epoch IS NULL",
            (
                wrong_close_epoch
                if seal_mutation == "wrong_group_state_close"
                else epoch,
                predecessor.group_id,
            ),
        )
    if predecessor.certificate_digest is not None:
        if seal_mutation != "open_certificate_binding":
            connection.execute(
                "UPDATE groundloop_m5_published_group_certificate_binding "
                "SET valid_to_epoch=%s "
                "WHERE group_version_id=%s AND valid_to_epoch IS NULL",
                (
                    wrong_close_epoch
                    if seal_mutation == "wrong_certificate_binding_close"
                    else epoch,
                    predecessor.group_id,
                ),
            )
    if retire_current_physical:
        for relation in (
            "groundloop_m5_matching_observation_current",
            "groundloop_m5_matching_edge_current",
            "groundloop_m5_matching_hash_mask_current",
            "groundloop_m5_matching_hall_current",
        ):
            connection.execute(
                sql.SQL("DELETE FROM {} WHERE group_version_id=%s").format(
                    sql.Identifier(relation)
                ),
                (predecessor.group_id,),
            )
    if successor is not None:
        connection.execute(
            """INSERT INTO groundloop_m5_matching_hall_current
               (group_version_id,requirement_count,mask_histogram,
                neighbor_counts,deficiencies,maximum_deficiency,matching_size,
                distinct_hash_count,installed_epoch_id,installed_revision)
               SELECT group_version_id,requirement_count,mask_histogram,
                      neighbor_counts,deficiencies,maximum_deficiency,matching_size,
                      distinct_hash_count,%s,%s
                 FROM groundloop_m5_matching_hall_working
                WHERE epoch_id=%s AND group_version_id=%s AND present""",
            (epoch, sealed_revision, epoch, successor),
        )
    if action == "RETIRE":
        connection.execute(
            "INSERT INTO groundloop_m5_group_family_retirement VALUES (%s,%s,%s)",
            (predecessor.family_id, epoch, event),
        )
    else:
        assert successor is not None
        successor_requirement = f"{successor}-requirement"
        connection.execute(
            "UPDATE groundloop_m5_group_version SET lifecycle_state='PUBLISHED' "
            "WHERE group_version_id=%s",
            (successor,),
        )
        connection.execute(
            "UPDATE groundloop_m5_requirement_version SET lifecycle_state='PUBLISHED' "
            "WHERE requirement_version_id=%s",
            (successor_requirement,),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_group_validity
               (group_version_id,group_family_id,claim_id,semantic_structure_hash,
                supersedes_group_version_id,valid_from_epoch,valid_to_epoch)
               SELECT version.group_version_id,version.group_family_id,family.claim_id,
                      version.semantic_structure_hash,version.supersedes_group_version_id,
                      %s,NULL
                 FROM groundloop_m5_group_version version
                 JOIN groundloop_m5_group_family family USING(group_family_id)
                WHERE version.group_version_id=%s""",
            (epoch, successor),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_published_requirement_state
               (requirement_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                witness_hashes,supporting_observation_ids,witness_count,satisfied,
                decision_policy_version)
               SELECT requirement_version_id,%s,NULL,%s,witness_hashes,
                      supporting_observation_ids,witness_count,satisfied,
                      decision_policy_version
                 FROM groundloop_m5_working_requirement_state
                WHERE epoch_id=%s AND requirement_version_id=%s""",
            (epoch, sealed_revision, epoch, successor_requirement),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_published_group_state
               (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                requirement_count,satisfied_count,matching_size,complete,
                decision_policy_version,certificate_digest)
               SELECT group_version_id,%s,NULL,%s,requirement_count,
                      satisfied_count,matching_size,complete,
                      decision_policy_version,certificate_digest
                 FROM groundloop_m5_working_group_state
                WHERE epoch_id=%s AND group_version_id=%s""",
            (epoch, sealed_revision, epoch, successor),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_requirement_state_materialized
               (requirement_version_id,witness_hashes,supporting_observation_ids,
                witness_count,satisfied,decision_policy_version,updated_epoch,
                updated_revision)
               SELECT requirement_version_id,witness_hashes,
                      supporting_observation_ids,witness_count,satisfied,
                      decision_policy_version,%s,%s
                 FROM groundloop_m5_working_requirement_state
                WHERE epoch_id=%s AND requirement_version_id=%s""",
            (epoch, sealed_revision, epoch, successor_requirement),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_group_state_materialized
               (group_version_id,requirement_count,satisfied_count,matching_size,
                complete,decision_policy_version,certificate_digest,
                updated_epoch,updated_revision)
               SELECT group_version_id,requirement_count,satisfied_count,
                      matching_size,complete,decision_policy_version,
                      certificate_digest,%s,%s
                 FROM groundloop_m5_working_group_state
                WHERE epoch_id=%s AND group_version_id=%s""",
            (epoch, sealed_revision, epoch, successor),
        )

    for state in claim_states:
        claim_state = state.after_value
        assert isinstance(claim_state, CombinedClaimState)
        connection.execute(
            """INSERT INTO groundloop_m5_published_claim_state
               (claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                support_count,refute_count,best_support_score,best_refute_score,
                supporting_observation_ids,refuting_observation_ids,
                complete_group_count,complete_group_ids,status,
                decision_policy_version,certificate_digest)
               SELECT claim_id,%s,NULL,%s,support_count,refute_count,
                      best_support_score,best_refute_score,
                      supporting_observation_ids,refuting_observation_ids,
                      complete_group_count,complete_group_ids,status,
                      decision_policy_version,certificate_digest
                 FROM groundloop_m5_working_claim_state
                WHERE epoch_id=%s AND claim_id=%s""",
            (epoch, sealed_revision, epoch, claim_state.claim_id),
        )
    for binding in claim_bindings:
        connection.execute(
            """INSERT INTO groundloop_m5_published_claim_certificate_binding
               (claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                certificate_digest)
               VALUES (%s,%s,NULL,%s,%s)""",
            (
                binding.claim_id,
                epoch,
                sealed_revision,
                binding.certificate_digest,
            ),
        )

    connection.execute(
        "UPDATE groundloop_m5_matching_image_current "
        "SET decision_policy_version=%s,installed_epoch_id=%s,installed_revision=%s "
        "WHERE singleton",
        (policy, epoch, sealed_revision),
    )
    connection.execute(
        "UPDATE groundloop_m4_publication_head SET epoch_id=%s,updated_at=now() "
        "WHERE singleton",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_publication_head "
        "SET epoch_id=%s,sealed_revision=%s,updated_at=now() WHERE singleton",
        (epoch, sealed_revision),
    )
    _insert_d26_runtime_work_row(
        connection,
        work=zero_work,
        epoch=epoch,
        event=event,
        kind="event",
    )
    _insert_d26_runtime_work_row(
        connection,
        work=zero_work,
        epoch=epoch,
        event=event,
        kind="call",
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result
           (structural_event_id,payload_hash,epoch_id,outcome,
            original_open_receipt_binding_hash,publication_id,
            original_publication_receipt_binding_hash,event_work_kind,
            event_work_digest,combined_status_delta_set_hash,changed_state_set_hash,
            failure_reason,logical_result_hash,delta_count,state_reference_count,
            coordinator_non_db_non_neural_ns,neural_wall_ns,
            postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns,
            postgres_server_execution_ns,postgres_lock_wait_ns,postgres_wal_bytes,
            postgres_shared_block_reads)
           VALUES (%s,%s,%s,'sealed',%s,%s,%s,'event',%s,%s,%s,NULL,%s,0,%s,
                   0,0,0,0,0,NULL,NULL,NULL,NULL)""",
        (
            event,
            payload,
            epoch,
            open_receipt,
            publication_id,
            publication_receipt,
            zero_work.work_digest,
            combined_hash,
            changed_state_hash,
            logical_result,
            len(references),
        ),
    )
    for ordinal, (
        kind,
        object_id,
        reference_epoch,
        reference_revision,
        state_hash,
        reference_digest,
    ) in enumerate(stored_references):
        connection.execute(
            """INSERT INTO groundloop_m5_event_result_state_reference
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                event,
                ordinal,
                kind,
                object_id,
                reference_epoch,
                reference_revision,
                state_hash,
                reference_digest,
            ),
        )
    connection.execute(
        """INSERT INTO groundloop_m5_event_timing_coverage
           (structural_event_id,epoch_id,required_expected_count,
            required_observed_count,required_missing_count,
            postgres_server_execution_expected_count,
            postgres_server_execution_observed_count,
            postgres_server_execution_missing_count,
            postgres_lock_wait_expected_count,postgres_lock_wait_observed_count,
            postgres_lock_wait_missing_count,postgres_wal_bytes_expected_count,
            postgres_wal_bytes_observed_count,postgres_wal_bytes_missing_count,
            postgres_shared_block_reads_expected_count,
            postgres_shared_block_reads_observed_count,
            postgres_shared_block_reads_missing_count,
            terminal_client_roundtrip_included)
           VALUES (%s,%s,1,0,1,1,0,1,1,0,1,1,0,1,1,0,1,false)""",
        (event, epoch),
    )
    connection.execute(
        """UPDATE groundloop_epoch
              SET revision=%s,semantic_status='sealed',evaluation_state='complete',
                  publication_mode='strict',sealed_at=clock_timestamp()
            WHERE epoch_id=%s AND revision=3""",
        (sealed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_epoch
              SET runtime_state='sealed',revision=%s,terminal_at=clock_timestamp()
            WHERE epoch_id=%s AND revision=3""",
        (sealed_revision, epoch),
    )
    work_assignments = [
        sql.SQL("{}={}").format(sql.Identifier("work_digest"), sql.Placeholder()),
        *(
            sql.SQL("{}={}").format(sql.Identifier(name), sql.Placeholder())
            for name in M5RuntimeWork.counter_names()
        ),
        sql.SQL("updated_revision={}").format(sql.Placeholder()),
        sql.SQL("terminalized=true"),
    ]
    connection.execute(
        sql.SQL(
            "UPDATE groundloop_m5_runtime_work_accumulator SET {} WHERE epoch_id=%s"
        ).format(sql.SQL(",").join(work_assignments)),
        (
            zero_work.work_digest,
            *zero_work.counter_values(),
            sealed_revision,
            epoch,
        ),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_timing_accumulator
              SET required_expected_count=1,required_observed_count=0,
                  required_missing_count=1,
                  postgres_server_execution_expected_count=1,
                  postgres_server_execution_observed_count=0,
                  postgres_server_execution_missing_count=1,
                  postgres_lock_wait_expected_count=1,
                  postgres_lock_wait_observed_count=0,
                  postgres_lock_wait_missing_count=1,
                  postgres_wal_bytes_expected_count=1,
                  postgres_wal_bytes_observed_count=0,
                  postgres_wal_bytes_missing_count=1,
                  postgres_shared_block_reads_expected_count=1,
                  postgres_shared_block_reads_observed_count=0,
                  postgres_shared_block_reads_missing_count=1,
                  updated_revision=%s,terminalized=true
            WHERE epoch_id=%s""",
        (sealed_revision, epoch),
    )
    if seal_mutation == "m4_head":
        connection.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (base,),
        )
    elif seal_mutation == "m5_head":
        connection.execute(
            "UPDATE groundloop_m5_publication_head "
            "SET epoch_id=%s,sealed_revision=%s WHERE singleton",
            (base, base_revision),
        )
    elif seal_mutation == "reopen_group_state":
        connection.execute(
            """INSERT INTO groundloop_m5_published_group_state
               (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                requirement_count,satisfied_count,matching_size,complete,
                decision_policy_version,certificate_digest)
               SELECT group_version_id,%s,NULL,%s,requirement_count,
                      satisfied_count,matching_size,complete,
                      decision_policy_version,certificate_digest
                 FROM groundloop_m5_published_group_state
                WHERE group_version_id=%s AND valid_to_epoch=%s""",
            (epoch, sealed_revision, predecessor.group_id, epoch),
        )
    elif seal_mutation == "reopen_requirement_state":
        connection.execute(
            """INSERT INTO groundloop_m5_published_requirement_state
               (requirement_version_id,valid_from_epoch,valid_to_epoch,
                sealed_revision,witness_hashes,supporting_observation_ids,
                witness_count,satisfied,decision_policy_version)
               SELECT requirement_version_id,%s,NULL,%s,witness_hashes,
                      supporting_observation_ids,witness_count,satisfied,
                      decision_policy_version
                 FROM groundloop_m5_published_requirement_state
                WHERE requirement_version_id=%s AND valid_to_epoch=%s""",
            (
                epoch,
                sealed_revision,
                predecessor.requirement_ids[0],
                epoch,
            ),
        )
    elif seal_mutation == "reopen_complete_state_and_binding":
        assert predecessor.certificate_digest is not None
        connection.execute(
            """INSERT INTO groundloop_m5_published_group_state
               (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                requirement_count,satisfied_count,matching_size,complete,
                decision_policy_version,certificate_digest)
               SELECT group_version_id,%s,NULL,%s,requirement_count,
                      satisfied_count,matching_size,complete,
                      decision_policy_version,certificate_digest
                 FROM groundloop_m5_published_group_state
                WHERE group_version_id=%s AND valid_to_epoch=%s""",
            (epoch, sealed_revision, predecessor.group_id, epoch),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_published_group_certificate_binding
               (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                certificate_digest)
               SELECT group_version_id,%s,NULL,%s,certificate_digest
                 FROM groundloop_m5_published_group_certificate_binding
                WHERE group_version_id=%s AND valid_to_epoch=%s""",
            (epoch, sealed_revision, predecessor.group_id, epoch),
        )
    if child_mutation is not None:
        _apply_d26_child_only_mutation(
            connection,
            mutation=child_mutation,
            epoch=epoch,
            event=event,
            base=base,
            predecessor=predecessor,
            action=action,
            successor=successor,
            alternate_certificate_digest=alternate_certificate_digest,
            not_cover_start_epoch=not_cover_start_epoch,
            wrong_deactivation_epoch=wrong_deactivation_epoch,
        )
    if force_result_constraint_only:
        connection.execute(
            "SET CONSTRAINTS groundloop_m5_event_result_children IMMEDIATE"
        )
    else:
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
    return _D26SealedEvent(
        epoch_id=epoch,
        revision=sealed_revision,
        event_id=event,
        payload_hash=payload,
        publication_id=publication_id,
        logical_result_hash=logical_result,
        references=references,
    )


def _seal_d26_present_reference_validator_fixture(
    connection: Connection[Any],
    *,
    prefix: str,
    snapshot: _B3ActivatedSnapshot,
    update_kind: str = "policy_change",
    wrong_state_hash_kind: str | None = None,
    absence_hash_kind: str | None = None,
) -> _D26SealedEvent:
    """Exercise the retained present-reference branches after migration 017.

    This is deliberately a database-validator fixture, not evidence that the
    still-unimplemented D25 store emits this transition.  It republishes one
    already-valid row or binding of every frozen kind at a new terminal point,
    then forces the retained migration-015 child constraint before any other
    deferred constraint when a one-field negative is requested.
    """

    assert not (wrong_state_hash_kind is not None and absence_hash_kind is not None)
    epoch, event, payload = _open_b2_runtime_epoch(
        connection,
        f"{prefix}-open",
        snapshot.epoch_id,
        snapshot.policy_version,
        direct_bridge=update_kind.startswith("document_"),
        update_kind=update_kind,
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()

    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,1)", (epoch,)
    )
    connection.execute(
        "UPDATE groundloop_epoch SET revision=2 WHERE epoch_id=%s AND revision=1",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_runtime_epoch "
        "SET runtime_state='semantic_pending',revision=2 "
        "WHERE epoch_id=%s AND revision=1",
        (epoch,),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()

    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,2)", (epoch,)
    )
    connection.execute(
        "UPDATE groundloop_epoch "
        "SET revision=3,semantic_status='complete',evaluation_state='complete' "
        "WHERE epoch_id=%s AND revision=2",
        (epoch,),
    )
    connection.execute(
        "UPDATE groundloop_m5_runtime_epoch "
        "SET runtime_state='semantic_complete',revision=3 "
        "WHERE epoch_id=%s AND revision=2",
        (epoch,),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    zero_work = _insert_d26_zero_runtime_accounting_point(
        connection,
        epoch=epoch,
        revision=3,
    )

    sealed_revision = 4
    claim_id = snapshot.claim_ids[0]
    requirement_id = snapshot.complete_requirement_ids[0]
    group_id = snapshot.complete_group_id
    answer_id = snapshot.answer_id
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,3)", (epoch,)
    )

    for relation, key_column, key_value in (
        (
            "groundloop_m5_published_requirement_state",
            "requirement_version_id",
            requirement_id,
        ),
        ("groundloop_m5_published_group_state", "group_version_id", group_id),
        ("groundloop_m5_published_claim_state", "claim_id", claim_id),
        ("groundloop_m5_published_answer_state", "answer_version_id", answer_id),
        (
            "groundloop_m5_published_group_certificate_binding",
            "group_version_id",
            group_id,
        ),
        (
            "groundloop_m5_published_claim_certificate_binding",
            "claim_id",
            claim_id,
        ),
    ):
        connection.execute(
            sql.SQL(
                "UPDATE {} SET valid_to_epoch=%s WHERE {}=%s AND valid_to_epoch IS NULL"
            ).format(sql.Identifier(relation), sql.Identifier(key_column)),
            (epoch, key_value),
        )

    connection.execute(
        """INSERT INTO groundloop_m5_published_requirement_state
           (requirement_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            witness_hashes,supporting_observation_ids,witness_count,satisfied,
            decision_policy_version)
           SELECT requirement_version_id,%s,NULL,%s,witness_hashes,
                  supporting_observation_ids,witness_count,satisfied,
                  decision_policy_version
             FROM groundloop_m5_published_requirement_state
            WHERE requirement_version_id=%s AND valid_to_epoch=%s""",
        (epoch, sealed_revision, requirement_id, epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_published_group_state
           (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            requirement_count,satisfied_count,matching_size,complete,
            decision_policy_version,certificate_digest)
           SELECT group_version_id,%s,NULL,%s,requirement_count,satisfied_count,
                  matching_size,complete,decision_policy_version,certificate_digest
             FROM groundloop_m5_published_group_state
            WHERE group_version_id=%s AND valid_to_epoch=%s""",
        (epoch, sealed_revision, group_id, epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_published_claim_state
           (claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            support_count,refute_count,best_support_score,best_refute_score,
            supporting_observation_ids,refuting_observation_ids,
            complete_group_count,complete_group_ids,status,
            decision_policy_version,certificate_digest)
           SELECT claim_id,%s,NULL,%s,support_count,refute_count,
                  best_support_score,best_refute_score,supporting_observation_ids,
                  refuting_observation_ids,complete_group_count,complete_group_ids,
                  status,decision_policy_version,certificate_digest
             FROM groundloop_m5_published_claim_state
            WHERE claim_id=%s AND valid_to_epoch=%s""",
        (epoch, sealed_revision, claim_id, epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_published_answer_state
           (answer_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            required_claim_count,supported_count,unsupported_count,refuted_count,
            conflicted_count,status)
           SELECT answer_version_id,%s,NULL,%s,required_claim_count,
                  supported_count,unsupported_count,refuted_count,
                  conflicted_count,status
             FROM groundloop_m5_published_answer_state
            WHERE answer_version_id=%s AND valid_to_epoch=%s""",
        (epoch, sealed_revision, answer_id, epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_published_group_certificate_binding
           (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            certificate_digest)
           SELECT group_version_id,%s,NULL,%s,certificate_digest
             FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id=%s AND valid_to_epoch=%s""",
        (epoch, sealed_revision, group_id, epoch),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_published_claim_certificate_binding
           (claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            certificate_digest)
           SELECT claim_id,%s,NULL,%s,certificate_digest
             FROM groundloop_m5_published_claim_certificate_binding
            WHERE claim_id=%s AND valid_to_epoch=%s""",
        (epoch, sealed_revision, claim_id, epoch),
    )

    requirement_row = connection.execute(
        """SELECT requirement_version_id,witness_hashes,
                  supporting_observation_ids,witness_count,satisfied,
                  decision_policy_version
             FROM groundloop_m5_published_requirement_state
            WHERE requirement_version_id=%s AND valid_from_epoch=%s
              AND sealed_revision=%s""",
        (requirement_id, epoch, sealed_revision),
    ).fetchone()
    group_row = connection.execute(
        """SELECT group_version_id,requirement_count,satisfied_count,
                  matching_size,complete,decision_policy_version,
                  certificate_digest
             FROM groundloop_m5_published_group_state
            WHERE group_version_id=%s AND valid_from_epoch=%s
              AND sealed_revision=%s""",
        (group_id, epoch, sealed_revision),
    ).fetchone()
    claim_row = connection.execute(
        """SELECT claim_id,support_count,refute_count,best_support_score,
                  best_refute_score,supporting_observation_ids,
                  refuting_observation_ids,complete_group_count,
                  complete_group_ids,status,decision_policy_version,
                  certificate_digest
             FROM groundloop_m5_published_claim_state
            WHERE claim_id=%s AND valid_from_epoch=%s AND sealed_revision=%s""",
        (claim_id, epoch, sealed_revision),
    ).fetchone()
    answer_row = connection.execute(
        """SELECT answer_version_id,required_claim_count,supported_count,
                  unsupported_count,refuted_count,conflicted_count,status
             FROM groundloop_m5_published_answer_state
            WHERE answer_version_id=%s AND valid_from_epoch=%s
              AND sealed_revision=%s""",
        (answer_id, epoch, sealed_revision),
    ).fetchone()
    group_binding = connection.execute(
        """SELECT group_version_id,certificate_digest
             FROM groundloop_m5_published_group_certificate_binding
            WHERE group_version_id=%s AND valid_from_epoch=%s
              AND sealed_revision=%s""",
        (group_id, epoch, sealed_revision),
    ).fetchone()
    claim_binding = connection.execute(
        """SELECT claim_id,certificate_digest
             FROM groundloop_m5_published_claim_certificate_binding
            WHERE claim_id=%s AND valid_from_epoch=%s AND sealed_revision=%s""",
        (claim_id, epoch, sealed_revision),
    ).fetchone()
    assert all(
        row is not None
        for row in (
            requirement_row,
            group_row,
            claim_row,
            answer_row,
            group_binding,
            claim_binding,
        )
    )
    assert requirement_row is not None
    assert group_row is not None
    assert claim_row is not None
    assert answer_row is not None
    assert group_binding is not None
    assert claim_binding is not None
    raw_references = [
        (
            "requirement_state",
            str(requirement_row[0]),
            runtime_digests.requirement_state_artifact_digest(
                requirement_version_id=str(requirement_row[0]),
                witness_hashes=tuple(requirement_row[1]),
                supporting_observation_ids=tuple(requirement_row[2]),
                witness_count=int(requirement_row[3]),
                satisfied=bool(requirement_row[4]),
                decision_policy_version=str(requirement_row[5]),
            ),
        ),
        (
            "group_state",
            str(group_row[0]),
            runtime_digests.group_state_artifact_digest(
                group_version_id=str(group_row[0]),
                requirement_count=int(group_row[1]),
                satisfied_count=int(group_row[2]),
                matching_size=int(group_row[3]),
                complete=bool(group_row[4]),
                decision_policy_version=str(group_row[5]),
                certificate_digest=(
                    None if group_row[6] is None else str(group_row[6])
                ),
            ),
        ),
        ("group_certificate", str(group_binding[0]), str(group_binding[1])),
        (
            "claim_state",
            str(claim_row[0]),
            runtime_digests.claim_state_artifact_digest(
                claim_id=str(claim_row[0]),
                support_count=int(claim_row[1]),
                refute_count=int(claim_row[2]),
                best_support_score=(
                    None if claim_row[3] is None else float(claim_row[3])
                ),
                best_refute_score=(
                    None if claim_row[4] is None else float(claim_row[4])
                ),
                supporting_observation_ids=tuple(claim_row[5]),
                refuting_observation_ids=tuple(claim_row[6]),
                complete_group_count=int(claim_row[7]),
                complete_group_ids=tuple(claim_row[8]),
                status=str(claim_row[9]),
                decision_policy_version=str(claim_row[10]),
                certificate_digest=str(claim_row[11]),
            ),
        ),
        ("claim_certificate", str(claim_binding[0]), str(claim_binding[1])),
        (
            "answer_state",
            str(answer_row[0]),
            runtime_digests.answer_state_artifact_digest(
                answer_version_id=str(answer_row[0]),
                required_claim_count=int(answer_row[1]),
                supported_count=int(answer_row[2]),
                unsupported_count=int(answer_row[3]),
                refuted_count=int(answer_row[4]),
                conflicted_count=int(answer_row[5]),
                status=str(answer_row[6]),
            ),
        ),
    ]
    assert len(raw_references) == 6
    stored_references: list[tuple[str, str, int, int, str, str]] = []
    for kind_value, object_value, state_hash_value in sorted(raw_references):
        kind = str(kind_value)
        object_id = str(object_value)
        state_hash = str(state_hash_value)
        if kind == wrong_state_hash_kind:
            state_hash = "0" * 64
        elif kind == absence_hash_kind:
            state_hash = stable_m5_digest(
                "m5-changed-state-absence-artifact-v1",
                enum_field(kind),
                text_field(object_id),
            )
        reference_digest = runtime_digests.changed_state_reference_digest(
            (kind, object_id, epoch, sealed_revision, state_hash)
        )
        stored_references.append(
            (
                kind,
                object_id,
                epoch,
                sealed_revision,
                state_hash,
                reference_digest,
            )
        )
    stored_references.sort(key=lambda row: (row[0], row[1], row[5]))
    references = tuple(
        (kind, object_id, state_hash, reference_digest)
        for kind, object_id, _, _, state_hash, reference_digest in stored_references
    )
    changed_state_hash = runtime_digests.changed_state_set_digest(
        reference_digest for *_, reference_digest in references
    )
    combined_hash = stable_m5_digest(
        "m5-combined-status-delta-set-v2", sequence_field(())
    )
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch))
    open_receipt = runtime_digests.open_event_receipt_binding_digest(
        epoch_id=epoch,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    publication_receipt = runtime_digests.publication_receipt_binding_digest(
        epoch_id=epoch,
        publication_id=publication_id,
        replayed=False,
    )
    logical_result = runtime_digests.event_run_logical_result_digest(
        event_id=event,
        payload_hash=payload,
        epoch_id=epoch,
        sealed_or_failed_outcome="sealed",
        original_open_receipt_binding_hash=open_receipt,
        original_publication_receipt_binding_hash=publication_receipt,
        event_work_digest=zero_work.work_digest,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_state_hash,
        failure_reason=None,
    )
    seal_source = runtime_digests.seal_contribution_source_digest(
        structural_event_id=event,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_state_hash,
        publication_id=publication_id,
    )
    contribution_key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=epoch,
        contribution_kind="seal",
        source_id=event,
    )
    contribution_columns = (
        "work_digest",
        "epoch_id",
        *M5RuntimeWork.counter_names(),
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, contribution_columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in contribution_columns),
        ),
        (
            zero_work.work_digest,
            epoch,
            *zero_work.counter_values(),
            "seal",
            event,
            seal_source,
            contribution_key,
            sealed_revision,
        ),
    )
    _insert_d26_runtime_work_row(
        connection,
        work=zero_work,
        epoch=epoch,
        event=event,
        kind="event",
    )
    _insert_d26_runtime_work_row(
        connection,
        work=zero_work,
        epoch=epoch,
        event=event,
        kind="call",
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result
           (structural_event_id,payload_hash,epoch_id,outcome,
            original_open_receipt_binding_hash,publication_id,
            original_publication_receipt_binding_hash,event_work_kind,
            event_work_digest,combined_status_delta_set_hash,changed_state_set_hash,
            failure_reason,logical_result_hash,delta_count,state_reference_count,
            coordinator_non_db_non_neural_ns,neural_wall_ns,
            postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns,
            postgres_server_execution_ns,postgres_lock_wait_ns,postgres_wal_bytes,
            postgres_shared_block_reads)
           VALUES (%s,%s,%s,'sealed',%s,%s,%s,'event',%s,%s,%s,NULL,%s,0,%s,
                   0,0,0,0,0,NULL,NULL,NULL,NULL)""",
        (
            event,
            payload,
            epoch,
            open_receipt,
            publication_id,
            publication_receipt,
            zero_work.work_digest,
            combined_hash,
            changed_state_hash,
            logical_result,
            len(references),
        ),
    )
    for ordinal, row in enumerate(stored_references):
        connection.execute(
            """INSERT INTO groundloop_m5_event_result_state_reference
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (event, ordinal, *row),
        )
    connection.execute(
        """INSERT INTO groundloop_m5_event_timing_coverage
           (structural_event_id,epoch_id,required_expected_count,
            required_observed_count,required_missing_count,
            postgres_server_execution_expected_count,
            postgres_server_execution_observed_count,
            postgres_server_execution_missing_count,
            postgres_lock_wait_expected_count,postgres_lock_wait_observed_count,
            postgres_lock_wait_missing_count,postgres_wal_bytes_expected_count,
            postgres_wal_bytes_observed_count,postgres_wal_bytes_missing_count,
            postgres_shared_block_reads_expected_count,
            postgres_shared_block_reads_observed_count,
            postgres_shared_block_reads_missing_count,
            terminal_client_roundtrip_included)
           VALUES (%s,%s,1,0,1,1,0,1,1,0,1,1,0,1,1,0,1,false)""",
        (event, epoch),
    )
    connection.execute(
        """UPDATE groundloop_epoch
              SET revision=%s,semantic_status='sealed',evaluation_state='complete',
                  publication_mode='strict',sealed_at=clock_timestamp()
            WHERE epoch_id=%s AND revision=3""",
        (sealed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_epoch
              SET runtime_state='sealed',revision=%s,terminal_at=clock_timestamp()
            WHERE epoch_id=%s AND revision=3""",
        (sealed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_work_accumulator
              SET updated_revision=%s,terminalized=true WHERE epoch_id=%s""",
        (sealed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_timing_accumulator
              SET required_expected_count=1,required_observed_count=0,
                  required_missing_count=1,
                  postgres_server_execution_expected_count=1,
                  postgres_server_execution_observed_count=0,
                  postgres_server_execution_missing_count=1,
                  postgres_lock_wait_expected_count=1,
                  postgres_lock_wait_observed_count=0,
                  postgres_lock_wait_missing_count=1,
                  postgres_wal_bytes_expected_count=1,
                  postgres_wal_bytes_observed_count=0,
                  postgres_wal_bytes_missing_count=1,
                  postgres_shared_block_reads_expected_count=1,
                  postgres_shared_block_reads_observed_count=0,
                  postgres_shared_block_reads_missing_count=1,
                  updated_revision=%s,terminalized=true
            WHERE epoch_id=%s""",
        (sealed_revision, epoch),
    )
    if wrong_state_hash_kind is not None or absence_hash_kind is not None:
        connection.execute(
            "SET CONSTRAINTS groundloop_m5_event_result_children IMMEDIATE"
        )
    else:
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
    return _D26SealedEvent(
        epoch_id=epoch,
        revision=sealed_revision,
        event_id=event,
        payload_hash=payload,
        publication_id=publication_id,
        logical_result_hash=logical_result,
        references=references,
    )


def _fail_d26_absence_reference_validator_fixture(
    connection: Connection[Any],
    *,
    prefix: str,
    snapshot: _B3ActivatedSnapshot,
) -> None:
    """Build a valid failed envelope, then inject one absence child on INSERT."""

    epoch, event, payload = _open_b2_runtime_epoch(
        connection,
        f"{prefix}-open",
        snapshot.epoch_id,
        snapshot.policy_version,
        update_kind="policy_change",
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.commit()
    zero_work = _insert_d26_zero_runtime_accounting_point(
        connection,
        epoch=epoch,
        revision=1,
    )

    failed_revision = 2
    failure_reason = "invariant_failure"
    kind = "requirement_state"
    object_id = snapshot.complete_requirement_ids[0]
    state_hash = stable_m5_digest(
        "m5-changed-state-absence-artifact-v1",
        enum_field(kind),
        text_field(object_id),
    )
    reference_digest = runtime_digests.changed_state_reference_digest(
        (kind, object_id, epoch, failed_revision, state_hash)
    )
    changed_state_hash = runtime_digests.changed_state_set_digest((reference_digest,))
    combined_hash = stable_m5_digest(
        "m5-combined-status-delta-set-v2", sequence_field(())
    )
    open_receipt = runtime_digests.open_event_receipt_binding_digest(
        epoch_id=epoch,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    logical_result = runtime_digests.event_run_logical_result_digest(
        event_id=event,
        payload_hash=payload,
        epoch_id=epoch,
        sealed_or_failed_outcome="failed",
        original_open_receipt_binding_hash=open_receipt,
        original_publication_receipt_binding_hash=None,
        event_work_digest=zero_work.work_digest,
        combined_status_delta_set_hash=combined_hash,
        changed_state_set_hash=changed_state_hash,
        failure_reason=failure_reason,
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,1)", (epoch,)
    )
    _insert_d26_runtime_work_row(
        connection,
        work=zero_work,
        epoch=epoch,
        event=event,
        kind="event",
    )
    _insert_d26_runtime_work_row(
        connection,
        work=zero_work,
        epoch=epoch,
        event=event,
        kind="call",
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result
           (structural_event_id,payload_hash,epoch_id,outcome,
            original_open_receipt_binding_hash,publication_id,
            original_publication_receipt_binding_hash,event_work_kind,
            event_work_digest,combined_status_delta_set_hash,changed_state_set_hash,
            failure_reason,logical_result_hash,delta_count,state_reference_count,
            coordinator_non_db_non_neural_ns,neural_wall_ns,
            postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns,
            postgres_server_execution_ns,postgres_lock_wait_ns,postgres_wal_bytes,
            postgres_shared_block_reads)
           VALUES (%s,%s,%s,'failed',%s,NULL,NULL,'event',%s,%s,%s,%s,%s,0,1,
                   0,0,0,0,0,NULL,NULL,NULL,NULL)""",
        (
            event,
            payload,
            epoch,
            open_receipt,
            zero_work.work_digest,
            combined_hash,
            changed_state_hash,
            failure_reason,
            logical_result,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_result_state_reference
           VALUES (%s,0,%s,%s,%s,%s,%s,%s)""",
        (
            event,
            kind,
            object_id,
            epoch,
            failed_revision,
            state_hash,
            reference_digest,
        ),
    )
    failure_source = runtime_digests.epoch_failure_contribution_source_digest(
        structural_event_id=event,
        failure_reason=failure_reason,
    )
    contribution_key = runtime_digests.runtime_work_contribution_key_digest(
        epoch_id=epoch,
        contribution_kind="epoch_failure",
        source_id=event,
    )
    contribution_columns = (
        "work_digest",
        "epoch_id",
        *M5RuntimeWork.counter_names(),
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, contribution_columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in contribution_columns),
        ),
        (
            zero_work.work_digest,
            epoch,
            *zero_work.counter_values(),
            "epoch_failure",
            event,
            failure_source,
            contribution_key,
            failed_revision,
        ),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_event_timing_coverage
           (structural_event_id,epoch_id,required_expected_count,
            required_observed_count,required_missing_count,
            postgres_server_execution_expected_count,
            postgres_server_execution_observed_count,
            postgres_server_execution_missing_count,
            postgres_lock_wait_expected_count,postgres_lock_wait_observed_count,
            postgres_lock_wait_missing_count,postgres_wal_bytes_expected_count,
            postgres_wal_bytes_observed_count,postgres_wal_bytes_missing_count,
            postgres_shared_block_reads_expected_count,
            postgres_shared_block_reads_observed_count,
            postgres_shared_block_reads_missing_count,
            terminal_client_roundtrip_included)
           VALUES (%s,%s,1,0,1,1,0,1,1,0,1,1,0,1,1,0,1,false)""",
        (event, epoch),
    )
    connection.execute(
        """UPDATE groundloop_epoch
              SET revision=%s,structural_status='failed',semantic_status='failed',
                  evaluation_state='failed',publication_mode='provisional',
                  sealed_at=NULL
            WHERE epoch_id=%s AND revision=1""",
        (failed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_epoch
              SET runtime_state='failed',revision=%s,terminal_at=clock_timestamp()
            WHERE epoch_id=%s AND revision=1""",
        (failed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_work_accumulator
              SET updated_revision=%s,terminalized=true WHERE epoch_id=%s""",
        (failed_revision, epoch),
    )
    connection.execute(
        """UPDATE groundloop_m5_runtime_timing_accumulator
              SET required_expected_count=1,required_observed_count=0,
                  required_missing_count=1,
                  postgres_server_execution_expected_count=1,
                  postgres_server_execution_observed_count=0,
                  postgres_server_execution_missing_count=1,
                  postgres_lock_wait_expected_count=1,
                  postgres_lock_wait_observed_count=0,
                  postgres_lock_wait_missing_count=1,
                  postgres_wal_bytes_expected_count=1,
                  postgres_wal_bytes_observed_count=0,
                  postgres_wal_bytes_missing_count=1,
                  postgres_shared_block_reads_expected_count=1,
                  postgres_shared_block_reads_observed_count=0,
                  postgres_shared_block_reads_missing_count=1,
                  updated_revision=%s,terminalized=true
            WHERE epoch_id=%s""",
        (failed_revision, epoch),
    )
    connection.execute("SET CONSTRAINTS groundloop_m5_event_result_children IMMEDIATE")


def _stage_b2_group_deactivation(
    connection: Connection[Any],
    *,
    epoch: int,
    event: str,
    group: str,
    action: str,
    mutation: str | None = None,
) -> None:
    deactivation_epoch = epoch
    deactivation_event = event
    if mutation == "wrong_epoch":
        update = connection.execute(
            "SELECT previous_published_epoch_id,decision_policy_version "
            "FROM groundloop_m5_update WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        assert update is not None
        deactivation_event = f"{event}-decoy"
        row = connection.execute(
            "INSERT INTO groundloop_epoch "
            "(event_id,payload_hash,revision,structural_status,semantic_status,"
            "evaluation_state,publication_mode,sealed_at) VALUES (%s,%s,1,'committed',"
            "'failed','failed','provisional',NULL) RETURNING epoch_id",
            (
                deactivation_event,
                hashlib.sha256(deactivation_event.encode()).hexdigest(),
            ),
        ).fetchone()
        assert row is not None
        deactivation_epoch = int(row[0])
        connection.execute(
            "INSERT INTO groundloop_m5_update VALUES "
            "(%s,'retire_group',%s,%s,'{}'::jsonb,DEFAULT)",
            (deactivation_epoch, int(update[0]), str(update[1])),
        )
    elif mutation == "wrong_event":
        row = connection.execute(
            "SELECT event_id FROM groundloop_epoch WHERE epoch_id<>%s "
            "ORDER BY epoch_id LIMIT 1",
            (epoch,),
        ).fetchone()
        assert row is not None
        deactivation_event = str(row[0])
    if mutation != "missing_deactivation":
        successor: str | None = None
        selected_action = action
        if mutation == "wrong_deactivation":
            selected_action = "RETIRE"
        elif mutation == "wrong_action":
            selected_action = "REPLACE" if action == "RETIRE" else "RETIRE"
        if selected_action == "REPLACE" or mutation == "wrong_deactivation":
            from groundloop.m5.domain import (
                EvidenceGroupVersion,
                EvidenceRequirementVersion,
            )

            successor = (
                f"{group}-wrong-successor"
                if mutation == "wrong_successor"
                else f"{group}-successor"
            )
            family_row = connection.execute(
                "SELECT version.group_family_id,family.claim_id FROM "
                "groundloop_m5_group_version version "
                "JOIN groundloop_m5_group_family family USING(group_family_id) "
                "WHERE version.group_version_id=%s",
                (group,),
            ).fetchone()
            assert family_row is not None
            successor_requirement = EvidenceRequirementVersion(
                requirement_version_id=f"{successor}-requirement",
                group_version_id=successor,
                ordinal=0,
                requirement_text="successor requirement",
            )
            successor_group = EvidenceGroupVersion(
                group_version_id=successor,
                group_family_id=str(family_row[0]),
                owner_claim_id=str(family_row[1]),
                requirements=(successor_requirement,),
                construction_source_id=f"{event}-source",
                supersedes_group_version_id=group,
            )
            connection.execute(
                "INSERT INTO groundloop_m5_group_version VALUES "
                "(%s,%s,%s,'STAGED','support_conjunction','controlled',%s,"
                "NULL,NULL,NULL,%s,%s,%s)",
                (
                    successor,
                    str(family_row[0]),
                    epoch,
                    f"{event}-source",
                    group,
                    successor_group.semantic_structure_hash,
                    successor_group.record_payload_hash,
                ),
            )
            connection.execute(
                "INSERT INTO groundloop_m5_requirement_version VALUES "
                "(%s,%s,%s,'STAGED',0,%s,%s,NULL,NULL,NULL,NULL)",
                (
                    successor_requirement.requirement_version_id,
                    successor,
                    epoch,
                    successor_requirement.requirement_text,
                    successor_requirement.requirement_text_hash,
                ),
            )
        deactivated_group = successor if mutation == "wrong_deactivation" else group
        replacement = successor if selected_action == "REPLACE" else None
        connection.execute(
            "INSERT INTO groundloop_m5_group_deactivation VALUES (%s,%s,%s,%s,%s)",
            (
                deactivation_epoch,
                deactivated_group,
                selected_action,
                replacement,
                deactivation_event,
            ),
        )
        if mutation == "second_deactivation":
            second_group = connection.execute(
                """SELECT group_version_id FROM groundloop_m5_group_version
                    WHERE lifecycle_state='PUBLISHED' AND group_version_id<>%s
                    ORDER BY group_version_id COLLATE "C" LIMIT 1""",
                (group,),
            ).fetchone()
            assert second_group is not None
            connection.execute(
                "INSERT INTO groundloop_m5_group_deactivation "
                "VALUES (%s,%s,'RETIRE',NULL,%s)",
                (epoch, str(second_group[0]), event),
            )


@dataclass(frozen=True, slots=True)
class _D26RetirementPhysicalPatch:
    observation_rows: tuple[tuple[Any, ...], ...]
    edge_rows: tuple[tuple[Any, ...], ...]
    mask_rows: tuple[tuple[Any, ...], ...]
    hall_rows: tuple[tuple[Any, ...], ...]
    observation_changes: tuple[bytes, ...]
    edge_changes: tuple[bytes, ...]
    mask_changes: tuple[bytes, ...]
    hall_changes: tuple[bytes, ...]


def _d26_retirement_physical_patch(
    connection: Connection[Any], *, epoch: int, group_id: str
) -> _D26RetirementPhysicalPatch:
    observation_rows = tuple(
        tuple(row)
        for row in connection.execute(
            """SELECT observation_id,requirement_version_id,group_version_id,
                      requirement_ordinal,text_hash::text,installed_epoch_id,
                      installed_revision
                 FROM groundloop_m5_matching_observation_current
                WHERE group_version_id=%s
                ORDER BY observation_id COLLATE "C"
            """,
            (group_id,),
        ).fetchall()
    )
    edge_rows = tuple(
        tuple(row)
        for row in connection.execute(
            """SELECT requirement_version_id,text_hash::text,group_version_id,
                      requirement_ordinal,refcount,installed_epoch_id,
                      installed_revision
                 FROM groundloop_m5_matching_edge_current
                WHERE group_version_id=%s
                ORDER BY group_version_id COLLATE "C",requirement_ordinal,
                         text_hash,requirement_version_id COLLATE "C"
            """,
            (group_id,),
        ).fetchall()
    )
    mask_rows = tuple(
        tuple(row)
        for row in connection.execute(
            """SELECT group_version_id,text_hash::text,mask,
                      installed_epoch_id,installed_revision
                 FROM groundloop_m5_matching_hash_mask_current
                WHERE group_version_id=%s
                ORDER BY group_version_id COLLATE "C",text_hash""",
            (group_id,),
        ).fetchall()
    )
    hall_rows = tuple(
        tuple(row)
        for row in connection.execute(
            """SELECT group_version_id,requirement_count,mask_histogram,
                      neighbor_counts,deficiencies,maximum_deficiency,
                      matching_size,distinct_hash_count,installed_epoch_id,
                      installed_revision
                 FROM groundloop_m5_matching_hall_current
                WHERE group_version_id=%s""",
            (group_id,),
        ).fetchall()
    )
    assert len(hall_rows) == 1
    observation_changes = tuple(
        _change_bytes(
            "m5-persisted-matching-observation-change-v1",
            (_typed("text", row[0]),),
            _sequence(
                _typed("enum", "current"),
                _typed("text", row[0]),
                _typed("text", row[1]),
                _typed("text", row[2]),
                _typed("int", row[3]),
                _typed("sha256", row[4]),
                _typed("int", row[5]),
                _typed("int", row[6]),
            ),
            _sequence(
                _typed("enum", "working"),
                _typed("int", epoch),
                _typed("text", row[0]),
                _typed("text", row[1]),
                _typed("text", row[2]),
                _typed("int", row[3]),
                _typed("sha256", row[4]),
                _typed("bool", False),
                _typed("int", 1),
            ),
        )
        for row in observation_rows
    )
    edge_changes = tuple(
        _change_bytes(
            "m5-persisted-matching-edge-change-v1",
            (_typed("text", row[0]), _typed("sha256", row[1])),
            _sequence(
                _typed("enum", "current"),
                _typed("text", row[0]),
                _typed("sha256", row[1]),
                _typed("text", row[2]),
                _typed("int", row[3]),
                _typed("int", row[4]),
                _typed("int", row[5]),
                _typed("int", row[6]),
            ),
            _sequence(
                _typed("enum", "working"),
                _typed("int", epoch),
                _typed("text", row[0]),
                _typed("sha256", row[1]),
                _typed("text", row[2]),
                _typed("int", row[3]),
                _typed("int", 0),
                _typed("int", 1),
            ),
        )
        for row in edge_rows
    )
    mask_changes = tuple(
        _change_bytes(
            "m5-persisted-matching-mask-change-v1",
            (_typed("text", row[0]), _typed("sha256", row[1])),
            _sequence(
                _typed("enum", "current"),
                _typed("text", row[0]),
                _typed("sha256", row[1]),
                _typed("int", row[2]),
                _typed("int", row[3]),
                _typed("int", row[4]),
            ),
            _sequence(
                _typed("enum", "working"),
                _typed("int", epoch),
                _typed("text", row[0]),
                _typed("sha256", row[1]),
                _typed("int", 0),
                _typed("int", 1),
            ),
        )
        for row in mask_rows
    )
    hall_changes = tuple(
        _change_bytes(
            "m5-persisted-matching-hall-change-v1",
            (_typed("text", row[0]),),
            _sequence(
                _typed("enum", "current"),
                _typed("text", row[0]),
                _typed("int", row[1]),
                _sequence(*(_typed("int", value) for value in row[2])),
                _sequence(*(_typed("int", value) for value in row[3])),
                _sequence(*(_typed("int", value) for value in row[4])),
                _typed("int", row[5]),
                _typed("int", row[6]),
                _typed("int", row[7]),
                _typed("int", row[8]),
                _typed("int", row[9]),
            ),
            _sequence(
                _typed("enum", "working"),
                _typed("int", epoch),
                _typed("text", row[0]),
                _typed("bool", False),
                *(("null",) for _ in range(7)),
                _typed("int", 1),
            ),
        )
        for row in hall_rows
    )
    return _D26RetirementPhysicalPatch(
        observation_rows=observation_rows,
        edge_rows=edge_rows,
        mask_rows=mask_rows,
        hall_rows=hall_rows,
        observation_changes=observation_changes,
        edge_changes=edge_changes,
        mask_changes=mask_changes,
        hall_changes=hall_changes,
    )


def _insert_d26_retirement_tombstones(
    connection: Connection[Any],
    *,
    epoch: int,
    patch: _D26RetirementPhysicalPatch,
) -> None:
    for row in patch.observation_rows:
        connection.execute(
            """INSERT INTO groundloop_m5_matching_observation_working
               VALUES (%s,%s,%s,%s,%s,%s,false,1)""",
            (epoch, *row[:5]),
        )
    for row in patch.edge_rows:
        connection.execute(
            """INSERT INTO groundloop_m5_matching_edge_working
               VALUES (%s,%s,%s,%s,%s,0,1)""",
            (epoch, *row[:4]),
        )
    for row in patch.mask_rows:
        connection.execute(
            """INSERT INTO groundloop_m5_matching_hash_mask_working
               VALUES (%s,%s,%s,0,1)""",
            (epoch, *row[:2]),
        )
    for row in patch.hall_rows:
        connection.execute(
            """INSERT INTO groundloop_m5_matching_hall_working
               VALUES (%s,%s,false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1)""",
            (epoch, row[0]),
        )


def _apply_empty_b2_structural(
    connection: Connection[Any],
    *,
    epoch: int,
    event: str,
    payload: str,
    base: int,
    policy: str,
    base_revision: int = 0,
    mutate: str | None = None,
    source_kind: str = "structural_open",
    hall_group: tuple[str, str] | None = None,
    absent_states: tuple[tuple[str, str, str], ...] = (),
    present_states: tuple[_D26PresentState, ...] = (),
    claim_bindings: tuple[WorkingClaimCertificateBinding, ...] = (),
    logical_mutation: str | None = None,
    forge_absent_working: str | None = None,
    encoded_source_kind: str | None = None,
    transition_before_revision: int = 1,
    transition_resulting_revision: int = 2,
    transition_runtime_state: str = "semantic_pending",
    remove_current_group: str | None = None,
) -> None:
    is_structural = source_kind == "structural_open"
    assert remove_current_group is None or is_structural
    physical_patch = (
        None
        if remove_current_group is None
        else _d26_retirement_physical_patch(
            connection,
            epoch=epoch,
            group_id=remove_current_group,
        )
    )
    artifact_source_kind = encoded_source_kind or source_kind
    before_epoch = base if is_structural else epoch
    before_revision = base_revision if is_structural else transition_before_revision
    runtime_expected_revision = 1 if is_structural else transition_before_revision
    resulting_revision = 1 if is_structural else transition_resulting_revision
    logical_absent_states = list(absent_states)
    if logical_mutation == "missing":
        logical_absent_states.pop(0)
    elif logical_mutation == "other_group_requirement":
        predecessor_group = next(
            object_id for kind, object_id, _ in absent_states if kind == "group_state"
        )
        other_row = connection.execute(
            """SELECT state.requirement_version_id,state.witness_hashes,
                      state.supporting_observation_ids,state.witness_count,
                      state.satisfied,state.decision_policy_version
                 FROM groundloop_m5_published_requirement_state state
                 JOIN groundloop_m5_requirement_version requirement
                   USING(requirement_version_id)
                WHERE requirement.group_version_id<>%s
                  AND state.valid_from_epoch<=%s
                  AND (state.valid_to_epoch IS NULL OR %s<state.valid_to_epoch)
                ORDER BY state.requirement_version_id COLLATE "C" LIMIT 1""",
            (predecessor_group, before_epoch, before_epoch),
        ).fetchone()
        assert other_row is not None
        other_requirement = str(other_row[0])
        other_digest = runtime_digests.requirement_state_artifact_digest(
            requirement_version_id=other_requirement,
            witness_hashes=tuple(other_row[1]),
            supporting_observation_ids=tuple(other_row[2]),
            witness_count=int(other_row[3]),
            satisfied=bool(other_row[4]),
            decision_policy_version=str(other_row[5]),
        )
        requirement_position = next(
            index
            for index, (kind, _, _) in enumerate(logical_absent_states)
            if kind == "requirement_state"
        )
        logical_absent_states[requirement_position] = (
            "requirement_state",
            other_requirement,
            other_digest,
        )
    if absent_states or present_states:
        from groundloop.m5.incremental_overlay import _logical_output_image

        output_order = {
            "requirement_state": 0,
            "group_state": 1,
            "claim_state": 2,
            "answer_state": 3,
            "group_certificate": 4,
            "claim_certificate": 5,
            "group_binding": 6,
            "claim_binding": 7,
        }
        output_records = [
            (kind, object_id, None) for kind, object_id, _ in logical_absent_states
        ]
        output_records.extend(
            (state.kind, state.object_id, state.after_value) for state in present_states
        )
        output_records.extend(
            ("claim_binding", binding.claim_id, binding) for binding in claim_bindings
        )
        output = _logical_output_image(
            tuple(
                sorted(
                    output_records,
                    key=lambda row: (output_order[row[0]], row[1]),
                )
            )
        )
    else:
        output = bytes.fromhex(
            "710000000000000002000000000000002573000000000000001c"
            "6d352d6f7665726c61792d6c6f676963616c2d6f75747075742d7632"
            "0000000000000009710000000000000000"
        )
    observation_preimages = (
        () if physical_patch is None else physical_patch.observation_changes
    )
    edge_preimages = () if physical_patch is None else physical_patch.edge_changes
    mask_preimages = () if physical_patch is None else physical_patch.mask_changes
    hall_changes_by_group = (
        []
        if physical_patch is None
        else list(
            zip(
                (str(row[0]) for row in physical_patch.hall_rows),
                physical_patch.hall_changes,
                strict=True,
            )
        )
    )
    absent_group = next(
        (object_id for kind, object_id, _ in absent_states if kind == "group_state"),
        None,
    )
    absent_requirements = (
        tuple(
            object_id
            for row in connection.execute(
                """SELECT requirement.requirement_version_id
                 FROM groundloop_m5_requirement_version requirement
                WHERE requirement.group_version_id=%s
                  AND requirement.requirement_version_id=ANY(%s)
                ORDER BY requirement.ordinal""",
                (
                    absent_group,
                    [
                        object_id
                        for kind, object_id, _ in absent_states
                        if kind == "requirement_state"
                    ],
                ),
            ).fetchall()
            for object_id in (str(row[0]),)
        )
        if absent_group is not None
        else ()
    )
    shape_rows: list[tuple[str, tuple[str, ...]]] = []
    if absent_group is not None:
        assert absent_requirements
        shape_rows.append((absent_group, absent_requirements))
    if hall_group is not None:
        group_id, requirement_id = hall_group
        shape_rows.append((group_id, (requirement_id,)))
        hall_after = _sequence(
            _typed("enum", "working"),
            _typed("int", epoch),
            _typed("text", group_id),
            _typed("bool", True),
            _typed("int", 1),
            _sequence(_typed("int", 0), _typed("int", 0)),
            _sequence(_typed("int", 0), _typed("int", 0)),
            _sequence(_typed("int", 0), _typed("int", 1)),
            _typed("int", 1),
            _typed("int", 0),
            _typed("int", 0),
            _typed("int", 1),
        )
        hall_preimage = _change_bytes(
            "m5-persisted-matching-hall-change-v1",
            (_typed("text", group_id),),
            ("null",),
            hall_after,
        )
        hall_changes_by_group.append((group_id, hall_preimage))
    shapes = _framed_preimage(
        "m5-persisted-matching-group-shape-set-v1",
        *_sequence(
            *(
                _sequence(
                    _typed("text", group_id),
                    _typed("int", len(requirement_ids)),
                    _sequence(
                        *(
                            _sequence(
                                _typed("int", ordinal),
                                _typed("text", requirement_id),
                            )
                            for ordinal, requirement_id in enumerate(requirement_ids)
                        )
                    ),
                )
                for group_id, requirement_ids in sorted(shape_rows)
            )
        ),
    )
    hall_preimages = tuple(preimage for _, preimage in sorted(hall_changes_by_group))
    logical_change_rows = [
        (kind, object_id, before_digest, None)
        for kind, object_id, before_digest in logical_absent_states
    ]
    logical_change_rows.extend(
        (
            state.kind,
            state.object_id,
            state.before_digest,
            state.after_digest,
        )
        for state in present_states
    )
    wrong_before_kind = {
        "wrong_requirement_before": "requirement_state",
        "wrong_group_before": "group_state",
        "wrong_certificate_before": "group_certificate",
    }.get(logical_mutation)
    if logical_mutation == "wrong_before":
        wrong_before_kind = logical_change_rows[0][0]
    if wrong_before_kind is not None:
        position = next(
            index
            for index, (kind, _, _, _) in enumerate(logical_change_rows)
            if kind == wrong_before_kind
        )
        kind, object_id, _, after_digest = logical_change_rows[position]
        logical_change_rows[position] = (
            kind,
            object_id,
            "0" * 64,
            after_digest,
        )
    elif logical_mutation == "before_none":
        kind, object_id, _, after_digest = logical_change_rows[0]
        logical_change_rows[0] = (kind, object_id, None, after_digest)
    elif logical_mutation == "present_after":
        kind, object_id, before_digest, _ = logical_change_rows[0]
        logical_change_rows[0] = (
            kind,
            object_id,
            before_digest,
            before_digest,
        )
    ordered_logical_rows = sorted(logical_change_rows)
    if logical_mutation == "duplicate":
        ordered_logical_rows.insert(1, ordered_logical_rows[0])
    elif logical_mutation == "reordered":
        ordered_logical_rows.reverse()
    encoded_logical_rows = [
        _sequence(
            _typed("enum", kind),
            _typed("text", object_id),
            ("null",) if before_digest is None else _typed("sha256", before_digest),
            ("null",) if after_digest is None else _typed("sha256", after_digest),
        )
        for kind, object_id, before_digest, after_digest in ordered_logical_rows
    ]
    if logical_mutation == "malformed":
        encoded_logical_rows[0] = _sequence(
            _typed("enum", ordered_logical_rows[0][0]),
            _typed("text", ordered_logical_rows[0][1]),
            ("null",),
        )
    logical_changes = _sequence(*encoded_logical_rows)
    binding_digests = tuple(
        hashlib.sha256(
            _framed_preimage(
                "m5-persisted-certificate-binding-row-v1",
                *_typed("enum", "claim"),
                *_typed("int", binding.epoch_id),
                *_typed("text", binding.claim_id),
                *_typed("int", binding.valid_from_revision),
                *(
                    ("null",)
                    if binding.valid_to_revision is None
                    else _typed("int", binding.valid_to_revision)
                ),
                *_typed("sha256", binding.certificate_digest),
            )
        ).hexdigest()
        for binding in claim_bindings
    )
    logical = _framed_preimage(
        "m5-persisted-logical-overlay-patch-v1",
        *logical_changes,
        *_sequence(*(_typed("sha256", value) for value in binding_digests)),
        *_typed("sha256", hashlib.sha256(output).hexdigest()),
        *_typed("int", len(output)),
    )
    if logical_mutation == "noncanonical":
        logical += b"\x00"
    work = [0] * 37
    if hall_group is not None:
        work[11] += 1
        work[12] += 1
        work[14] += 1
    if physical_patch is not None:
        work[1] = len(physical_patch.observation_rows)
        work[2] = len(physical_patch.observation_rows)
        work[5] = 3 * len(physical_patch.observation_rows) + len(
            physical_patch.mask_rows
        )
        work[7] = len(physical_patch.edge_rows)
        work[8] = len(physical_patch.edge_rows)
        work[9] = len(physical_patch.mask_rows)
        work[12] += sum(
            (1 << int(hall_row[1])) - 1 for hall_row in physical_patch.hall_rows
        ) * len(physical_patch.mask_rows)
        work[13] = sum(
            sum(
                1
                for subset_mask in range(1, 1 << int(hall_row[1]))
                if int(mask_row[2]) & subset_mask
            )
            for hall_row in physical_patch.hall_rows
            for mask_row in physical_patch.mask_rows
        )
        work[14] += sum(
            (1 << int(hall_row[1])) - 1 for hall_row in physical_patch.hall_rows
        ) * len(physical_patch.mask_rows)
        work[24] = len(physical_patch.hall_rows)
    work[30] = len(output)
    work[25] = len(
        {
            object_id
            for kind, object_id, _, _ in logical_change_rows
            if kind in ("group_state", "group_certificate")
        }
    )
    work[26] = len(
        {
            object_id
            for kind, object_id, _, _ in logical_change_rows
            if kind in ("claim_state", "claim_certificate")
        }
        | {binding.claim_id for binding in claim_bindings}
    )
    work[33] = sum(
        kind == "claim_state"
        and before_digest is not None
        and after_digest is not None
        and isinstance(state.after_value, CombinedClaimState)
        for kind, object_id, before_digest, after_digest in logical_change_rows
        for state in present_states
        if state.kind == kind and state.object_id == object_id
    )
    if mutate == "output_bytes":
        work[30] += 1
    work_preimage = _framed_preimage(
        "m5-matching-work-v1",
        *(field for value in work for field in _typed("int", value)),
    )
    work_digest = hashlib.sha256(work_preimage).hexdigest()
    patch_preimage = _framed_preimage(
        "m5-persisted-matching-patch-v1",
        *_typed("enum", artifact_source_kind),
        *_typed("text", event),
        *_typed("sha256", payload),
        *_typed("int", before_epoch),
        *_typed("int", before_revision),
        *_typed("int", epoch),
        *_typed("int", resulting_revision),
        *_typed("text", policy),
        *_typed("sha256", hashlib.sha256(shapes).hexdigest()),
        *_sequence(
            *(
                _typed("sha256", hashlib.sha256(value).hexdigest())
                for value in observation_preimages
            )
        ),
        *_sequence(
            *(
                _typed("sha256", hashlib.sha256(value).hexdigest())
                for value in edge_preimages
            )
        ),
        *_sequence(
            *(
                _typed("sha256", hashlib.sha256(value).hexdigest())
                for value in mask_preimages
            )
        ),
        *_sequence(
            *(
                _typed("sha256", hashlib.sha256(value).hexdigest())
                for value in hall_preimages
            )
        ),
        *_typed("sha256", hashlib.sha256(logical).hexdigest()),
        *_typed("sha256", work_digest),
    )
    patch_digest = hashlib.sha256(patch_preimage).hexdigest()
    contribution_preimage = _framed_preimage(
        "m5-matching-work-contribution-v1",
        *_typed("int", epoch),
        *_typed("enum", artifact_source_kind),
        *_typed("text", event),
        *_typed("sha256", payload),
        *_typed("int", before_epoch),
        *_typed("int", before_revision),
        *_typed("int", resulting_revision),
        *_typed("sha256", patch_digest),
        *_typed("sha256", work_digest),
    )
    contribution_digest = hashlib.sha256(contribution_preimage).hexdigest()
    connection.execute(
        "SELECT groundloop_m5_authorize_checked_transition(%s,%s)",
        (epoch, runtime_expected_revision),
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_transition(%s,%s,%s,%s,%s)",
        (
            epoch,
            runtime_expected_revision,
            resulting_revision,
            source_kind,
            event,
        ),
    )
    if not is_structural:
        semantic_status = (
            "complete" if transition_runtime_state == "semantic_complete" else "pending"
        )
        evaluation_state = (
            "complete" if transition_runtime_state == "semantic_complete" else "pending"
        )
        connection.execute(
            "UPDATE groundloop_epoch SET revision=%s,semantic_status=%s,"
            "evaluation_state=%s WHERE epoch_id=%s",
            (resulting_revision, semantic_status, evaluation_state, epoch),
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch "
            "SET runtime_state=%s,revision=%s WHERE epoch_id=%s",
            (transition_runtime_state, resulting_revision, epoch),
        )
    if mutate == "clear_mode":
        connection.execute("SELECT set_config('groundloop.m5_matching_mode','',true)")
    if is_structural:
        connection.execute(
            "INSERT INTO groundloop_m5_matching_image_working VALUES (%s,%s,%s,%s,1)",
            (epoch, base, base_revision, policy),
        )
        if physical_patch is not None:
            _insert_d26_retirement_tombstones(
                connection,
                epoch=epoch,
                patch=physical_patch,
            )
    else:
        if mutate == "base_mutation":
            connection.execute(
                "UPDATE groundloop_m5_matching_image_working "
                "SET base_revision=1,updated_revision=2 WHERE epoch_id=%s",
                (epoch,),
            )
        elif mutate == "policy_mutation":
            connection.execute(
                "UPDATE groundloop_m5_matching_image_working "
                "SET decision_policy_version=%s,updated_revision=2 WHERE epoch_id=%s",
                (policy, epoch),
            )
        else:
            connection.execute(
                "UPDATE groundloop_m5_matching_image_working "
                "SET updated_revision=%s WHERE epoch_id=%s",
                (resulting_revision, epoch),
            )
    if mutate == "duplicate_image":
        connection.execute(
            "UPDATE groundloop_m5_matching_image_working "
            "SET updated_revision=1 WHERE epoch_id=%s",
            (epoch,),
        )
    if hall_group is not None:
        connection.execute(
            """INSERT INTO groundloop_m5_matching_hall_working VALUES
               (%s,%s,true,1,ARRAY[0,0]::bigint[],ARRAY[0,0]::bigint[],
                ARRAY[0,1]::bigint[],1,0,0,1)""",
            (epoch, hall_group[0]),
        )
    for state in present_states:
        if state.kind != "claim_certificate":
            continue
        artifact = state.after_value
        assert isinstance(artifact, ClaimCertificateArtifact)
        connection.execute(
            """INSERT INTO groundloop_m5_claim_certificate_artifact
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                artifact.certificate_digest,
                artifact.certificate_version,
                artifact.claim_id,
                artifact.decision_policy_version,
                artifact.support_kind.value,
                artifact.direct_support_observation_id,
                artifact.group_version_id,
                artifact.group_certificate_digest,
                artifact.direct_refute_observation_id,
            ),
        )
    for state in present_states:
        if state.kind == "requirement_state":
            value = state.after_value
            assert isinstance(value, RequirementState)
            connection.execute(
                """INSERT INTO groundloop_m5_working_requirement_state
                   VALUES (%s,%s,%s,%s,%s,%s,%s,1)""",
                (
                    epoch,
                    value.requirement_version_id,
                    list(value.witness_hashes),
                    list(value.supporting_observation_ids),
                    value.witness_count,
                    value.satisfied,
                    policy,
                ),
            )
        elif state.kind == "group_state":
            value = state.after_value
            assert isinstance(value, GroupState)
            connection.execute(
                """INSERT INTO groundloop_m5_working_group_state
                   VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,1)""",
                (
                    epoch,
                    value.group_version_id,
                    value.requirement_count,
                    value.satisfied_count,
                    value.matching_size,
                    value.complete,
                    policy,
                ),
            )
        elif state.kind == "claim_state":
            value = state.after_value
            assert isinstance(value, CombinedClaimState)
            claim_binding = next(
                binding
                for binding in claim_bindings
                if binding.claim_id == value.claim_id
            )
            connection.execute(
                """INSERT INTO groundloop_m5_working_claim_state
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1)""",
                (
                    epoch,
                    value.claim_id,
                    value.support_count,
                    value.refute_count,
                    value.best_support_score,
                    value.best_refute_score,
                    list(value.supporting_observation_ids),
                    list(value.refuting_observation_ids),
                    value.complete_group_count,
                    list(value.complete_group_ids),
                    value.status.value,
                    policy,
                    claim_binding.certificate_digest,
                ),
            )
        elif state.kind == "claim_certificate":
            pass
        else:
            raise AssertionError(f"unsupported D26 present-state fixture: {state.kind}")
    for binding in claim_bindings:
        connection.execute(
            """INSERT INTO groundloop_m5_working_claim_certificate_binding
               VALUES (%s,%s,%s,%s,%s)""",
            (
                binding.epoch_id,
                binding.claim_id,
                binding.valid_from_revision,
                binding.valid_to_revision,
                binding.certificate_digest,
            ),
        )
    if forge_absent_working:
        for kind, object_id, _ in absent_states:
            if kind != forge_absent_working:
                continue
            if kind == "requirement_state":
                connection.execute(
                    "INSERT INTO groundloop_m5_working_requirement_state "
                    "VALUES (%s,%s,ARRAY[]::text[],ARRAY[]::text[],0,false,%s,1)",
                    (epoch, object_id, policy),
                )
            elif kind == "group_state":
                connection.execute(
                    "INSERT INTO groundloop_m5_working_group_state "
                    "VALUES (%s,%s,1,0,0,false,%s,NULL,1)",
                    (epoch, object_id, policy),
                )
    artifact_columns = (
        "patch_digest",
        "source_kind",
        "source_id",
        "source_identity_hash",
        "before_epoch_id",
        "before_revision",
        "resulting_epoch_id",
        "resulting_revision",
        "decision_policy_version",
        "group_shape_set_digest",
        "group_shape_set_preimage",
        "observation_change_digests",
        "observation_change_preimages",
        "edge_change_digests",
        "edge_change_preimages",
        "mask_change_digests",
        "mask_change_preimages",
        "hall_change_digests",
        "hall_change_preimages",
        "logical_overlay_patch_digest",
        "logical_overlay_patch_preimage",
        "logical_output_preimage",
        "matching_work_digest",
        "canonical_patch_preimage",
    )
    connection.execute(
        sql.SQL(
            "INSERT INTO groundloop_m5_matching_patch_artifact ({}) VALUES ({})"
        ).format(
            sql.SQL(",").join(map(sql.Identifier, artifact_columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in artifact_columns),
        ),
        (
            patch_digest,
            artifact_source_kind,
            event,
            payload,
            before_epoch,
            before_revision,
            epoch,
            resulting_revision,
            policy,
            hashlib.sha256(shapes).hexdigest(),
            shapes,
            [hashlib.sha256(value).hexdigest() for value in observation_preimages],
            list(observation_preimages),
            [hashlib.sha256(value).hexdigest() for value in edge_preimages],
            list(edge_preimages),
            [hashlib.sha256(value).hexdigest() for value in mask_preimages],
            list(mask_preimages),
            [hashlib.sha256(value).hexdigest() for value in hall_preimages],
            list(hall_preimages),
            hashlib.sha256(logical).hexdigest(),
            logical,
            output,
            work_digest,
            patch_preimage,
        ),
    )
    counter_columns = [
        str(row[0])
        for row in connection.execute(
            """SELECT column_name FROM information_schema.columns
           WHERE table_schema=current_schema()
             AND table_name='groundloop_m5_matching_work_contribution'
             AND ordinal_position BETWEEN 9 AND 45 ORDER BY ordinal_position"""
        ).fetchall()
    ]
    columns = [
        "epoch_id",
        "source_kind",
        "source_id",
        "source_identity_hash",
        "before_epoch_id",
        "before_revision",
        "resulting_revision",
        "patch_digest",
        *counter_columns,
        "matching_work_digest",
        "contribution_digest",
    ]
    insert = sql.SQL(
        "INSERT INTO groundloop_m5_matching_work_contribution ({}) VALUES ({})"
    ).format(
        sql.SQL(",").join(map(sql.Identifier, columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
    )
    contribution_work = list(work)
    connection.execute(
        insert,
        [
            epoch,
            artifact_source_kind,
            event,
            payload,
            before_epoch,
            before_revision,
            resulting_revision,
            patch_digest,
            *contribution_work,
            work_digest,
            contribution_digest,
        ],
    )
    accumulator_columns = [
        "epoch_id",
        *counter_columns,
        "matching_work_digest",
        "updated_revision",
    ]
    accumulator_insert = sql.SQL(
        "INSERT INTO groundloop_m5_matching_work_accumulator ({}) VALUES ({})"
    ).format(
        sql.SQL(",").join(map(sql.Identifier, accumulator_columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in accumulator_columns),
    )
    accumulator_work = list(work)
    if mutate == "accumulator":
        accumulator_work[0] = 1
    accumulator_digest = hashlib.sha256(
        _framed_preimage(
            "m5-matching-work-v1",
            *(field for value in accumulator_work for field in _typed("int", value)),
        )
    ).hexdigest()
    if is_structural:
        connection.execute(
            accumulator_insert,
            [epoch, *accumulator_work, accumulator_digest, resulting_revision],
        )
    else:
        prior_row = connection.execute(
            sql.SQL(
                "SELECT {} FROM groundloop_m5_matching_work_accumulator "
                "WHERE epoch_id=%s"
            ).format(sql.SQL(",").join(map(sql.Identifier, counter_columns))),
            (epoch,),
        ).fetchone()
        assert prior_row is not None
        accumulated_work = [
            int(prior) + current
            for prior, current in zip(prior_row, accumulator_work, strict=True)
        ]
        accumulated_digest = hashlib.sha256(
            _framed_preimage(
                "m5-matching-work-v1",
                *(
                    field
                    for value in accumulated_work
                    for field in _typed("int", value)
                ),
            )
        ).hexdigest()
        assignments = sql.SQL(",").join(
            sql.SQL("{}={}").format(sql.Identifier(column), sql.Placeholder())
            for column in counter_columns
        )
        connection.execute(
            sql.SQL(
                "UPDATE groundloop_m5_matching_work_accumulator SET {},"
                "matching_work_digest=%s,updated_revision=%s WHERE epoch_id=%s"
            ).format(assignments),
            [*accumulated_work, accumulated_digest, resulting_revision, epoch],
        )
    if mutate == "unconsumed":
        claim_row = connection.execute(
            "SELECT claim_id FROM groundloop_claim"
        ).fetchone()
        assert claim_row is not None
        artifact = ClaimCertificateArtifact(
            str(claim_row[0]), policy, ClaimSupportKind.NONE
        )
        connection.execute(
            """INSERT INTO groundloop_m5_claim_certificate_artifact
               VALUES (%s,'m5-claim-certificate-v2',%s,%s,'none',
                       NULL,NULL,NULL,NULL)""",
            (artifact.certificate_digest, str(claim_row[0]), policy),
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def _seed_b2_direct_transition_authority(
    connection: Connection[Any],
    epoch: int,
    prefix: str,
    *,
    from_revision: int = 1,
    to_revision: int = 2,
) -> tuple[str, str]:
    transition_id = f"{prefix}-transition"
    transition_hash = hashlib.sha256(transition_id.encode()).hexdigest()
    if (
        connection.execute(
            "SELECT 1 FROM groundloop_m4_evaluation_epoch_counter WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        is None
    ):
        connection.execute(
            """INSERT INTO groundloop_m4_evaluation_epoch_counter
               VALUES (%s,%s,'active','complete',NULL,0,1)""",
            (epoch, hashlib.sha256(f"{prefix}-declaration".encode()).hexdigest()),
        )
    connection.execute(
        """INSERT INTO groundloop_m4_evaluation_counter_transition
           (epoch_id,transition_id,payload_hash,transition_kind,
            from_revision,to_revision,override_rows_written)
           VALUES (%s,%s,%s,'delta',%s,%s,0)""",
        (epoch, transition_id, transition_hash, from_revision, to_revision),
    )
    return transition_id, transition_hash


@dataclass(frozen=True, slots=True)
class _B3ActivatedSnapshot:
    epoch_id: int
    revision: int
    policy_version: str
    answer_id: str
    second_answer_id: str
    claim_ids: tuple[str, ...]
    complete_group_id: str
    partial_group_id: str
    zero_hash_group_id: str
    complete_requirement_ids: tuple[str, ...]
    partial_requirement_ids: tuple[str, ...]
    zero_hash_requirement_id: str
    requirement_observations: tuple[tuple[str, str, str, int, str], ...]
    direct_support_observations: tuple[str, ...]
    refuted_only_observation: str
    conflicted_support_observation: str
    conflicted_refute_observation: str
    optional_support_observation: str
    currency_alternate_observation: str
    currency_extra_observation: str
    complete_group_alternate_observation: str


def _seed_b3_direct_m4_snapshot(
    connection: Connection[Any], *, epoch_id: int, revision: int
) -> None:
    """Persist the independently derived direct-v1 image before M5 activation."""
    from groundloop.m4.pipeline import bootstrap_m4_publication

    connection.execute(
        """INSERT INTO groundloop_claim_state_materialized (
             claim_id,support_count,refute_count,best_support_score,
             best_refute_score,supporting_observation_ids,
             refuting_observation_ids,status,updated_epoch,updated_revision)
           SELECT claim_id,support_count,refute_count,best_support_score,
                  best_refute_score,supporting_observation_ids,
                  refuting_observation_ids,
                  CASE WHEN support_count>0 AND refute_count>0 THEN 'conflicted'
                       WHEN support_count>0 THEN 'supported'
                       WHEN refute_count>0 THEN 'refuted'
                       ELSE 'unsupported'
                   END::groundloop_claim_status,%s,%s
             FROM groundloop_m5_direct_claim_state_oracle""",
        (epoch_id, revision),
    )
    connection.execute(
        """INSERT INTO groundloop_answer_state_materialized (
             answer_version_id,required_claim_count,supported_count,
             unsupported_count,refuted_count,conflicted_count,status,
             updated_epoch,updated_revision)
           WITH direct_claim AS (
             SELECT claim_id,
               CASE WHEN support_count>0 AND refute_count>0 THEN 'conflicted'
                    WHEN support_count>0 THEN 'supported'
                    WHEN refute_count>0 THEN 'refuted'
                    ELSE 'unsupported' END status
               FROM groundloop_m5_direct_claim_state_oracle),
           aggregate AS (
             SELECT answer.answer_version_id,
               count(*) FILTER (WHERE claim.required)::integer
                 required_claim_count,
               count(*) FILTER (WHERE claim.required
                                  AND state.status='supported')::integer
                 supported_count,
               count(*) FILTER (WHERE claim.required
                                  AND state.status='unsupported')::integer
                 unsupported_count,
               count(*) FILTER (WHERE claim.required
                                  AND state.status='refuted')::integer
                 refuted_count,
               count(*) FILTER (WHERE claim.required
                                  AND state.status='conflicted')::integer
                 conflicted_count
               FROM groundloop_answer_version answer
               JOIN groundloop_claim claim
                 ON claim.answer_version_id=answer.answer_version_id
               JOIN direct_claim state ON state.claim_id=claim.claim_id
              GROUP BY answer.answer_version_id)
           SELECT aggregate.*,
             CASE WHEN refuted_count>0 THEN 'contradicted'
                  WHEN conflicted_count>0 THEN 'conflicted'
                  WHEN required_claim_count>0
                   AND supported_count=required_claim_count THEN 'valid'
                  WHEN supported_count>0 THEN 'partially_supported'
                  ELSE 'unsupported'
               END::groundloop_answer_status,%s,%s
             FROM aggregate""",
        (epoch_id, revision),
    )
    connection.execute(
        """INSERT INTO groundloop_claim_certificate (
             claim_id,support_observation_id,refute_observation_id,
             repaired_epoch,repaired_revision)
           SELECT claim_id,supporting_observation_ids[1],
                  refuting_observation_ids[1],%s,%s
             FROM groundloop_m5_direct_claim_state_oracle""",
        (epoch_id, revision),
    )
    bootstrap_m4_publication(connection, sealed_epoch_id=epoch_id)


def _seed_b3_activated_snapshot(
    connection: Connection[Any],
    prefix: str,
    *,
    inconsistent_activation_head: bool = False,
    complete_owner_survivor: bool = False,
) -> _B3ActivatedSnapshot:
    from m5.postgres.helpers import (
        force_deferred_checks,
        insert_observation,
        insert_published_group,
        install_current_currency,
        install_test_activation_barrier,
        make_group,
        seed_base,
    )

    revision = 7
    chunk_texts = (
        "direct-only alpha",
        "direct-only beta",
        "refuted-only",
        "conflicted support",
        "conflicted refute",
        "optional direct",
        "group shared",
        "group shared",
        "group beta",
        "partial alpha",
    )
    base = seed_base(
        connection,
        prefix=prefix,
        claim_count=6,
        chunk_texts=chunk_texts,
        epoch_revision=revision,
    )
    second_answer_id = f"{prefix}-second-answer"
    connection.execute(
        """INSERT INTO groundloop_answer_version (
             answer_version_id,question_id,text,generator_model_id,
             generator_model_version,prompt_version,created_epoch)
           VALUES (%s,%s,'second fixture answer','generator','v1','p1',%s)""",
        (second_answer_id, base.question_id, base.epoch_id),
    )
    connection.execute(
        "UPDATE groundloop_claim SET answer_version_id=%s WHERE claim_id=%s",
        (second_answer_id, base.claim_ids[5]),
    )
    # Answer A requires only the group-only claim, retaining the decisive v1
    # UNSUPPORTED -> combined-M5 VALID contrast. Its other claims exercise
    # direct-only, refuted-only, conflicted, and ignored optional-direct shapes.
    # Answer B has one required unsupported claim with incomplete/empty groups.
    connection.execute(
        "UPDATE groundloop_claim SET required=false WHERE claim_id=ANY(%s)",
        (list(base.claim_ids[1:5]),),
    )
    complete = make_group(
        group_id=f"{prefix}-complete-group",
        family_id=f"{prefix}-complete-family",
        claim_id=base.claim_ids[0],
        texts=("complete first", "complete second"),
        source_id=f"{prefix}-complete-source",
    )
    partial = make_group(
        group_id=f"{prefix}-partial-group",
        family_id=f"{prefix}-partial-family",
        claim_id=base.claim_ids[5],
        texts=("partial first", "partial missing"),
        source_id=f"{prefix}-partial-source",
    )
    zero_hash = make_group(
        group_id=f"{prefix}-zero-hash-group",
        family_id=f"{prefix}-zero-hash-family",
        claim_id=base.claim_ids[5],
        texts=("zero hash",),
        source_id=f"{prefix}-zero-hash-source",
    )
    survivor = (
        make_group(
            group_id=f"{prefix}-survivor-group",
            family_id=f"{prefix}-survivor-family",
            claim_id=base.claim_ids[0],
            texts=("survivor proof",),
            source_id=f"{prefix}-survivor-source",
        )
        if complete_owner_survivor
        else None
    )
    groups = (complete, partial, zero_hash) + (() if survivor is None else (survivor,))
    for group in groups:
        insert_published_group(connection, group=group, epoch_id=base.epoch_id)

    direct_support = (
        f"{prefix}-direct-only-a-support",
        f"{prefix}-direct-only-z-support",
    )
    refuted_only = f"{prefix}-refuted-only"
    conflicted_support = f"{prefix}-conflicted-support"
    conflicted_refute = f"{prefix}-conflicted-refute"
    optional_support = f"{prefix}-optional-support"
    direct_observations = [
        (
            direct_support[0],
            base.claim_ids[1],
            base.chunk_ids[0],
            (0.95, 0.02, 0.03),
        ),
        (
            direct_support[1],
            base.claim_ids[1],
            base.chunk_ids[1],
            (0.91, 0.04, 0.05),
        ),
        (
            refuted_only,
            base.claim_ids[2],
            base.chunk_ids[2],
            (0.02, 0.95, 0.03),
        ),
        (
            conflicted_support,
            base.claim_ids[3],
            base.chunk_ids[3],
            (0.94, 0.03, 0.03),
        ),
        (
            conflicted_refute,
            base.claim_ids[3],
            base.chunk_ids[4],
            (0.03, 0.94, 0.03),
        ),
        (
            optional_support,
            base.claim_ids[4],
            base.chunk_ids[5],
            (0.93, 0.03, 0.04),
        ),
    ]
    for observation_id, claim_id, chunk_id, scores in direct_observations:
        insert_observation(
            connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=claim_id,
            chunk_id=chunk_id,
            produced_epoch=base.epoch_id,
            task_type="claim-verification-v1",
            scores=scores,
        )
        install_current_currency(
            connection,
            observation_id=observation_id,
            subject_kind="claim",
            subject_id=claim_id,
            chunk_id=chunk_id,
            task_type="claim-verification-v1",
            epoch_id=base.epoch_id,
            revision=revision,
        )

    currency_alternate = f"{prefix}-currency-alternate"
    insert_observation(
        connection,
        observation_id=currency_alternate,
        subject_kind="claim",
        subject_id=base.claim_ids[1],
        chunk_id=base.chunk_ids[0],
        produced_epoch=base.epoch_id,
        task_type="claim-verification-v1",
        scores=(0.95, 0.02, 0.03),
    )
    currency_extra = f"{prefix}-currency-extra"
    insert_observation(
        connection,
        observation_id=currency_extra,
        subject_kind="claim",
        subject_id=base.claim_ids[5],
        chunk_id=base.chunk_ids[0],
        produced_epoch=base.epoch_id,
        task_type="claim-verification-v1",
        scores=(0.1, 0.1, 0.8),
    )

    requirement_specs = [
        (
            f"{prefix}-complete-a-observation",
            complete.requirements[0].requirement_version_id,
            base.chunk_ids[6],
            complete.group_version_id,
            0,
            chunk_texts[6],
        ),
        (
            f"{prefix}-complete-z-observation",
            complete.requirements[0].requirement_version_id,
            base.chunk_ids[7],
            complete.group_version_id,
            0,
            chunk_texts[7],
        ),
        (
            f"{prefix}-complete-second-observation",
            complete.requirements[1].requirement_version_id,
            base.chunk_ids[8],
            complete.group_version_id,
            1,
            chunk_texts[8],
        ),
        (
            f"{prefix}-partial-observation",
            partial.requirements[0].requirement_version_id,
            base.chunk_ids[9],
            partial.group_version_id,
            0,
            chunk_texts[9],
        ),
    ]
    if survivor is not None:
        requirement_specs.append(
            (
                f"{prefix}-survivor-observation",
                survivor.requirements[0].requirement_version_id,
                base.chunk_ids[6],
                survivor.group_version_id,
                0,
                chunk_texts[6],
            )
        )
    for observation_id, requirement_id, chunk_id, _, _, _ in requirement_specs:
        insert_observation(
            connection,
            observation_id=observation_id,
            subject_kind="requirement",
            subject_id=requirement_id,
            chunk_id=chunk_id,
            produced_epoch=base.epoch_id,
        )
        install_current_currency(
            connection,
            observation_id=observation_id,
            subject_kind="requirement",
            subject_id=requirement_id,
            chunk_id=chunk_id,
            task_type="verify_requirement_v1",
            epoch_id=base.epoch_id,
            revision=revision,
        )

    force_deferred_checks(connection)
    _seed_b3_direct_m4_snapshot(connection, epoch_id=base.epoch_id, revision=revision)
    assert connection.execute(
        """SELECT status::text FROM groundloop_answer_state_materialized
            WHERE answer_version_id=%s""",
        (base.answer_id,),
    ).fetchone() == ("unsupported",)
    if inconsistent_activation_head:
        from groundloop.postgres.m5 import (
            build_m5_bootstrap_projection,
            write_m5_materialized_states,
        )

        projection = build_m5_bootstrap_projection(connection)
        write_m5_materialized_states(
            connection,
            states=projection.states,
            decision_policy_version=projection.decision_policy_version,
            epoch_id=projection.epoch_id,
            revision=projection.revision,
            group_certificates=projection.group_certificates,
            claim_certificates=projection.claim_certificates,
            publish=True,
        )
        connection.execute(
            """INSERT INTO groundloop_m5_publication_head
               (singleton,epoch_id,sealed_revision,updated_at)
               VALUES (true,%s,%s,now())""",
            (projection.epoch_id, projection.revision),
        )
        inconsistent_base = _b3_future_epoch(
            connection, f"{prefix}-inconsistent-activation-base"
        )
        connection.execute(
            """INSERT INTO groundloop_m5_activation
               (singleton,activation_id,payload_hash,base_m4_epoch_id,activated_at)
               VALUES (true,%s,%s,%s,now())""",
            (
                f"{prefix}-activation",
                hashlib.sha256(f"{prefix}-activation".encode()).hexdigest(),
                inconsistent_base,
            ),
        )
        changed = connection.execute(
            """UPDATE groundloop_runtime_mode
                  SET mode='m5_active',mode_revision=mode_revision+1,updated_at=now()
                WHERE singleton AND mode='v1_only' AND mode_revision=0"""
        ).rowcount
        assert changed == 1
        force_deferred_checks(connection)
    else:
        install_test_activation_barrier(
            connection, base, activation_id=f"{prefix}-activation"
        )
    assert connection.execute(
        """SELECT status::text FROM groundloop_m5_answer_state_materialized
            WHERE answer_version_id=%s""",
        (base.answer_id,),
    ).fetchone() == ("valid",)
    connection.commit()
    return _B3ActivatedSnapshot(
        epoch_id=base.epoch_id,
        revision=revision,
        policy_version=base.policy_version,
        answer_id=base.answer_id,
        second_answer_id=second_answer_id,
        claim_ids=base.claim_ids,
        complete_group_id=complete.group_version_id,
        partial_group_id=partial.group_version_id,
        zero_hash_group_id=zero_hash.group_version_id,
        complete_requirement_ids=tuple(
            value.requirement_version_id for value in complete.requirements
        ),
        partial_requirement_ids=tuple(
            value.requirement_version_id for value in partial.requirements
        ),
        zero_hash_requirement_id=(zero_hash.requirements[0].requirement_version_id),
        requirement_observations=tuple(
            (
                observation_id,
                requirement_id,
                group_id,
                ordinal,
                hashlib.sha256(text.encode()).hexdigest(),
            )
            for (
                observation_id,
                requirement_id,
                _,
                group_id,
                ordinal,
                text,
            ) in requirement_specs
        ),
        direct_support_observations=direct_support,
        refuted_only_observation=refuted_only,
        conflicted_support_observation=conflicted_support,
        conflicted_refute_observation=conflicted_refute,
        optional_support_observation=optional_support,
        currency_alternate_observation=currency_alternate,
        currency_extra_observation=currency_extra,
        complete_group_alternate_observation=requirement_specs[1][0],
    )


def _b3_future_epoch(connection: Connection[Any], name: str) -> int:
    row = connection.execute(
        """INSERT INTO groundloop_epoch (
             event_id,payload_hash,revision,structural_status,semantic_status,
             evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
           RETURNING epoch_id""",
        (name, hashlib.sha256(name.encode()).hexdigest()),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _b3_install_catalog_inventory(connection: Connection[Any]) -> tuple[Any, ...]:
    relations = tuple(
        connection.execute(
            """SELECT c.relkind,c.relname
                 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=current_schema()
                ORDER BY c.relkind,c.relname COLLATE \"C\""""
        ).fetchall()
    )
    routines = tuple(
        connection.execute(
            """SELECT p.proname,pg_get_function_identity_arguments(p.oid)
                 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname=current_schema()
                ORDER BY p.proname COLLATE \"C\",
                         pg_get_function_identity_arguments(p.oid) COLLATE \"C\""""
        ).fetchall()
    )
    triggers = tuple(
        connection.execute(
            """SELECT c.relname,t.tgname
                 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                 JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=current_schema() AND NOT t.tgisinternal
                ORDER BY c.relname COLLATE \"C\",t.tgname COLLATE \"C\""""
        ).fetchall()
    )
    ledger = connection.execute(
        "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
        (M5_PERSISTED_MATCHING_BUNDLE_ID,),
    ).fetchone()
    return (
        relations,
        routines,
        triggers,
        ledger,
        _d26_validator_catalog_image(connection),
    )


def _d26_validator_catalog_image(connection: Connection[Any]) -> tuple[Any, ...]:
    validator = connection.execute(
        """SELECT p.oid,pg_get_functiondef(p.oid),owner.rolname,p.proacl,
                  language.lanname,format_type(p.prorettype,NULL),p.provolatile,
                  p.prosecdef,p.proparallel,p.proconfig
             FROM pg_proc p
             JOIN pg_namespace namespace ON namespace.oid=p.pronamespace
             JOIN pg_roles owner ON owner.oid=p.proowner
             JOIN pg_language language ON language.oid=p.prolang
            WHERE namespace.nspname=current_schema()
              AND p.proname='groundloop_m5_validate_event_result_children'
              AND pg_get_function_identity_arguments(p.oid)=''"""
    ).fetchone()
    assert validator is not None
    triggers = tuple(
        connection.execute(
            """SELECT trigger.oid,trigger.tgname,relation.relname,
                      trigger.tgfoid,pg_get_triggerdef(trigger.oid,true),
                      trigger.tgenabled,trigger.tgdeferrable,
                      trigger.tginitdeferred
                 FROM pg_trigger trigger
                 JOIN pg_class relation ON relation.oid=trigger.tgrelid
                 JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                WHERE namespace.nspname=current_schema()
                  AND trigger.tgname=ANY(%s)
                ORDER BY trigger.tgname COLLATE \"C\"""",
            (list(D26_RETAINED_RESULT_TRIGGERS),),
        ).fetchall()
    )
    assert tuple(str(row[1]) for row in triggers) == tuple(
        sorted(D26_RETAINED_RESULT_TRIGGERS)
    )
    return tuple(validator), triggers


def _b3_relation_counts(
    connection: Connection[Any], relations: tuple[str, ...]
) -> tuple[tuple[str, int], ...]:
    return tuple(
        (
            relation,
            int(connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]),
        )
        for relation in relations
    )


def _b3_close_current_rows(
    connection: Connection[Any], *, name: str, tables: tuple[str, ...]
) -> None:
    future = _b3_future_epoch(connection, f"b3-close-{name}")
    for table in tables:
        connection.execute(
            sql.SQL(
                "UPDATE {} SET valid_to_epoch=%s WHERE valid_to_epoch IS NULL"
            ).format(sql.Identifier(table)),
            (future,),
        )


@contextmanager
def _b3_user_triggers_disabled(
    connection: Connection[Any], table: str
) -> Iterator[None]:
    connection.execute(
        sql.SQL("ALTER TABLE {} DISABLE TRIGGER USER").format(sql.Identifier(table))
    )
    try:
        yield
    finally:
        connection.execute(
            sql.SQL("ALTER TABLE {} ENABLE TRIGGER USER").format(sql.Identifier(table))
        )


@contextmanager
def _d26_replica_trigger_window(connection: Connection[Any]) -> Iterator[None]:
    """Suppress triggers for exactly one isolated late-fault statement."""

    connection.execute("SET LOCAL session_replication_role='replica'")
    try:
        yield
    finally:
        connection.execute("SET LOCAL session_replication_role='origin'")


def _apply_d26_child_only_mutation(
    connection: Connection[Any],
    *,
    mutation: str,
    epoch: int,
    event: str,
    base: int,
    predecessor: _D26Predecessor,
    action: str,
    successor: str | None,
    alternate_certificate_digest: str | None,
    not_cover_start_epoch: int | None,
    wrong_deactivation_epoch: int | None,
) -> None:
    """Inject one late fault after the valid D25 structural transaction.

    Trigger execution is suppressed only around the selected adversarial field
    update, then restored before the migration-017 child constraint is forced.
    This isolates D26's independent database validator without changing the
    production schema or mistaking an earlier D25 rejection for D26 evidence.
    """

    if mutation in {
        "wrong_requirement_before_digest",
        "wrong_group_before_digest",
    }:
        alternate_policy = f"{event}-alternate-policy"
        connection.execute(
            """INSERT INTO groundloop_decision_policy
               (policy_version,support_threshold,refute_threshold,
                tie_rule_version,valid_from_epoch,valid_to_epoch)
               VALUES (%s,.51,.51,'v1',%s,%s)""",
            (alternate_policy, base, epoch),
        )
        relation = (
            "groundloop_m5_published_requirement_state"
            if mutation == "wrong_requirement_before_digest"
            else "groundloop_m5_published_group_state"
        )
        object_column = (
            "requirement_version_id"
            if mutation == "wrong_requirement_before_digest"
            else "group_version_id"
        )
        object_id = (
            predecessor.requirement_ids[0]
            if mutation == "wrong_requirement_before_digest"
            else predecessor.group_id
        )
        with _d26_replica_trigger_window(connection):
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET decision_policy_version=%s "
                    "WHERE {}=%s AND valid_to_epoch=%s"
                ).format(sql.Identifier(relation), sql.Identifier(object_column)),
                (alternate_policy, object_id, epoch),
            )
    elif mutation == "wrong_certificate_before_digest":
        assert alternate_certificate_digest is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_published_group_state "
                "SET certificate_digest=%s "
                "WHERE group_version_id=%s AND valid_to_epoch=%s",
                (alternate_certificate_digest, predecessor.group_id, epoch),
            )
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_published_group_certificate_binding "
                "SET certificate_digest=%s "
                "WHERE group_version_id=%s AND valid_to_epoch=%s",
                (alternate_certificate_digest, predecessor.group_id, epoch),
            )
    elif mutation == "validity_not_covering_previous_head":
        assert not_cover_start_epoch is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_validity SET valid_from_epoch=%s "
                "WHERE group_version_id=%s",
                (not_cover_start_epoch, predecessor.group_id),
            )
    elif mutation == "predecessor_group_not_published":
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_version SET lifecycle_state='FAILED' "
                "WHERE group_version_id=%s",
                (predecessor.group_id,),
            )
    elif mutation == "predecessor_requirement_not_published":
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_requirement_version "
                "SET lifecycle_state='FAILED' WHERE requirement_version_id=%s",
                (predecessor.requirement_ids[0],),
            )
    elif mutation == "predecessor_requirement_wrong_owner":
        owner_row = connection.execute(
            """SELECT requirement.requirement_version_id,other.group_version_id
                 FROM groundloop_m5_requirement_version AS requirement
                 JOIN groundloop_m5_group_version AS other
                   ON other.group_version_id<>requirement.group_version_id
                  AND other.lifecycle_state='PUBLISHED'
                WHERE requirement.group_version_id=%s
                  AND NOT EXISTS (
                      SELECT 1
                        FROM groundloop_m5_requirement_version AS collision
                       WHERE collision.group_version_id=other.group_version_id
                         AND (collision.ordinal=requirement.ordinal
                              OR collision.requirement_text_hash=
                                 requirement.requirement_text_hash)
                  )
                ORDER BY requirement.ordinal DESC,
                         other.group_version_id COLLATE "C"
                LIMIT 1""",
            (predecessor.group_id,),
        ).fetchone()
        assert owner_row is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_requirement_version SET group_version_id=%s "
                "WHERE requirement_version_id=%s",
                (str(owner_row[1]), str(owner_row[0])),
            )
    elif mutation == "wrong_update_kind":
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_update SET update_kind='policy_change' "
                "WHERE epoch_id=%s",
                (epoch,),
            )
    elif mutation == "wrong_deactivation_event":
        base_event = connection.execute(
            "SELECT event_id FROM groundloop_epoch WHERE epoch_id=%s", (base,)
        ).fetchone()
        assert base_event is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_deactivation SET event_id=%s "
                "WHERE epoch_id=%s AND group_version_id=%s",
                (str(base_event[0]), epoch, predecessor.group_id),
            )
    elif mutation == "wrong_deactivation_epoch":
        assert wrong_deactivation_epoch is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_deactivation SET epoch_id=%s "
                "WHERE epoch_id=%s AND group_version_id=%s",
                (wrong_deactivation_epoch, epoch, predecessor.group_id),
            )
    elif mutation == "wrong_predecessor_id":
        other = connection.execute(
            """SELECT group_version_id FROM groundloop_m5_group_version
                WHERE lifecycle_state='PUBLISHED' AND group_version_id<>%s
                ORDER BY group_version_id COLLATE "C" LIMIT 1""",
            (predecessor.group_id,),
        ).fetchone()
        assert other is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_deactivation SET group_version_id=%s "
                "WHERE epoch_id=%s AND group_version_id=%s",
                (str(other[0]), epoch, predecessor.group_id),
            )
    elif mutation == "wrong_deactivation_action":
        assert action == "REPLACE" and successor is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_deactivation "
                "SET action='RETIRE',successor_group_version_id=NULL "
                "WHERE epoch_id=%s AND group_version_id=%s",
                (epoch, predecessor.group_id),
            )
    elif mutation == "retire_with_successor":
        assert action == "RETIRE" and successor is None
        other = connection.execute(
            """SELECT group_version_id FROM groundloop_m5_group_version
                WHERE lifecycle_state='PUBLISHED' AND group_version_id<>%s
                ORDER BY group_version_id COLLATE "C" LIMIT 1""",
            (predecessor.group_id,),
        ).fetchone()
        assert other is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_deactivation "
                "SET action='REPLACE',successor_group_version_id=%s "
                "WHERE epoch_id=%s AND group_version_id=%s",
                (str(other[0]), epoch, predecessor.group_id),
            )
    elif mutation == "wrong_successor_id":
        assert action == "REPLACE" and successor is not None
        other = connection.execute(
            """SELECT group_version_id FROM groundloop_m5_group_version
                WHERE lifecycle_state='PUBLISHED'
                  AND group_version_id<>ALL(%s)
                ORDER BY group_version_id COLLATE "C" LIMIT 1""",
            ([predecessor.group_id, successor],),
        ).fetchone()
        assert other is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_deactivation "
                "SET successor_group_version_id=%s "
                "WHERE epoch_id=%s AND group_version_id=%s",
                (str(other[0]), epoch, predecessor.group_id),
            )
    elif mutation == "wrong_successor_record_hash":
        assert action == "REPLACE" and successor is not None
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_m5_group_version SET record_payload_hash=%s "
                "WHERE group_version_id=%s",
                ("0" * 64, successor),
            )
    elif mutation == "second_deactivation":
        other = connection.execute(
            """SELECT group_version_id FROM groundloop_m5_group_version
                WHERE lifecycle_state='PUBLISHED' AND group_version_id<>%s
                ORDER BY group_version_id COLLATE "C" LIMIT 1""",
            (predecessor.group_id,),
        ).fetchone()
        assert other is not None
        connection.execute(
            "INSERT INTO groundloop_m5_group_deactivation "
            "VALUES (%s,%s,'RETIRE',NULL,%s)",
            (epoch, str(other[0]), event),
        )
    elif mutation == "epoch_nonsealed":
        with _d26_replica_trigger_window(connection):
            connection.execute(
                "UPDATE groundloop_epoch SET semantic_status='complete',"
                "publication_mode='provisional',sealed_at=NULL WHERE epoch_id=%s",
                (epoch,),
            )
    else:
        raise AssertionError(f"unsupported D26 child-only mutation: {mutation}")


def _seed_b3_runtime_history(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot
) -> None:
    from groundloop.m4.contracts import VectorIndexKind
    from groundloop.m5.runtime.contracts import M5CandidatePolicyManifest
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

    model_id = f"{snapshot.policy_version}-runtime-model"
    connection.execute(
        """INSERT INTO groundloop_model_artifact (
             model_artifact_id,task,provider,model_id,immutable_revision,
             tokenizer_revision,license_id,config_hash)
           VALUES (%s,'embedding','fixture','embedder','v1','v1','MIT',%s)""",
        (model_id, hashlib.sha256(model_id.encode()).hexdigest()),
    )
    manifest = M5CandidatePolicyManifest.build(
        embedding_model_artifact_id=model_id,
        requirement_role_template_hash=hashlib.sha256(b"b3-role").hexdigest(),
        chunk_role_template_hash=hashlib.sha256(b"b3-chunk-role").hexdigest(),
        vector_method_version="b3-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=hashlib.sha256(b"b3-build").hexdigest(),
        vector_search_config_hash=hashlib.sha256(b"b3-search").hexdigest(),
        lexical_method_version="b3-lexical-v1",
        lexical_config_hash=hashlib.sha256(b"b3-lexical").hexdigest(),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=hashlib.sha256(b"b3-verifier").hexdigest(),
        decision_policy_version=snapshot.policy_version,
        lineage_safety_override=True,
    )
    PostgresM5RuntimeStore(connection).register_candidate_policy(manifest)
    connection.execute(
        """INSERT INTO groundloop_m5_requirement_registry_snapshot
           VALUES (%s,0,%s,DEFAULT)""",
        (EMPTY_REQUIREMENT_SNAPSHOT_DIGEST, snapshot.epoch_id),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_active_chunk_snapshot
           VALUES (%s,0,%s,'m5-normalize-text-v1',%s,DEFAULT)""",
        (
            EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
            snapshot.epoch_id,
            "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb",
        ),
    )
    event_id = f"{snapshot.policy_version}-runtime-history"
    history_epoch = connection.execute(
        """INSERT INTO groundloop_epoch (
             event_id,payload_hash,revision,structural_status,semantic_status,
             evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,1,'committed','pending','pending','provisional',NULL)
           RETURNING epoch_id""",
        (event_id, hashlib.sha256(event_id.encode()).hexdigest()),
    ).fetchone()
    assert history_epoch is not None
    epoch_id = int(history_epoch[0])
    connection.execute(
        """INSERT INTO groundloop_m5_update (
             epoch_id,update_kind,previous_published_epoch_id,
             decision_policy_version,manifest)
           VALUES (%s,'policy_change',%s,%s,'{}'::jsonb)""",
        (epoch_id, snapshot.epoch_id, snapshot.policy_version),
    )
    connection.execute(
        """INSERT INTO groundloop_m5_runtime_epoch (
             epoch_id,structural_event_id,candidate_policy_id,
             candidate_policy_manifest_hash,requirement_registry_snapshot_digest,
             active_chunk_snapshot_digest,expected_previous_published_epoch_id,
             requirement_root_set_hash,runtime_state,revision,open_work_count,
             open_scope_count,blocking_failure_count)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'structural_committed',1,0,0,0)""",
        (
            epoch_id,
            event_id,
            manifest.candidate_policy_id,
            manifest.manifest_hash,
            EMPTY_REQUIREMENT_SNAPSHOT_DIGEST,
            EMPTY_ACTIVE_CHUNK_SNAPSHOT_DIGEST,
            snapshot.epoch_id,
            EMPTY_REQUIREMENT_ROOT_SET_HASH,
        ),
    )


def _register_d26_candidate_policy(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot
) -> None:
    from groundloop.m4.contracts import VectorIndexKind
    from groundloop.m5.runtime.contracts import M5CandidatePolicyManifest
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

    model_id = f"{snapshot.policy_version}-d26-runtime-model"
    connection.execute(
        """INSERT INTO groundloop_model_artifact (
             model_artifact_id,task,provider,model_id,immutable_revision,
             tokenizer_revision,license_id,config_hash)
           VALUES (%s,'embedding','fixture','embedder','v1','v1','MIT',%s)""",
        (model_id, hashlib.sha256(model_id.encode()).hexdigest()),
    )
    manifest = M5CandidatePolicyManifest.build(
        embedding_model_artifact_id=model_id,
        requirement_role_template_hash=hashlib.sha256(b"d26-role").hexdigest(),
        chunk_role_template_hash=hashlib.sha256(b"d26-chunk-role").hexdigest(),
        vector_method_version="d26-vector-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config_hash=hashlib.sha256(b"d26-build").hexdigest(),
        vector_search_config_hash=hashlib.sha256(b"d26-search").hexdigest(),
        lexical_method_version="d26-lexical-v1",
        lexical_config_hash=hashlib.sha256(b"d26-lexical").hexdigest(),
        lexical_postgres_version="16.14",
        lexical_regconfig_identity="simple",
        fusion_version="rank-interleave-v1",
        reverse_budget_per_inserted_chunk=2,
        forward_budget_per_requirement=2,
        verifier_execution_spec_hash=hashlib.sha256(b"d26-verifier").hexdigest(),
        decision_policy_version=snapshot.policy_version,
        lineage_safety_override=True,
    )
    PostgresM5RuntimeStore(connection).register_candidate_policy(manifest)
    connection.execute(
        """INSERT INTO groundloop_candidate_policy (
             candidate_policy_id,policy_hash,embedding_model_artifact_id,
             decision_policy_version,claim_role_template_hash,
             chunk_role_template_hash,vector_method_version,vector_index_kind,
             vector_index_build_config_hash,vector_search_config_hash,
             lexical_method_version,lexical_config_hash,lexical_postgres_version,
             lexical_regconfig_identity,claim_registry_snapshot_id,claim_count,
             fusion_version,approximate_cap_per_inserted_chunk,frontier_depth,manifest)
           SELECT candidate_policy_id,%s,embedding_model_artifact_id,
                  decision_policy_version,requirement_role_template_hash,
                  chunk_role_template_hash,vector_method_version,vector_index_kind,
                  vector_index_build_config_hash,vector_search_config_hash,
                  lexical_method_version,lexical_config_hash,
                  lexical_postgres_version,lexical_regconfig_identity,%s,0,
                  fusion_version,reverse_budget_per_inserted_chunk,
                  forward_budget_per_requirement,'{}'::jsonb
             FROM groundloop_m5_candidate_policy
            WHERE candidate_policy_id=%s""",
        (
            hashlib.sha256(
                f"{snapshot.policy_version}-d26-m4-policy".encode()
            ).hexdigest(),
            f"{snapshot.policy_version}-d26-registry",
            manifest.candidate_policy_id,
        ),
    )


def _b3_alternate_group_certificate(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot
) -> str:
    from groundloop.postgres.m5 import persist_group_certificate

    current = connection.execute(
        """SELECT artifact.certificate_digest::text
             FROM groundloop_m5_group_certificate_artifact artifact
             JOIN groundloop_m5_published_group_certificate_binding binding
               USING (certificate_digest)
            WHERE binding.group_version_id=%s AND binding.valid_to_epoch IS NULL""",
        (snapshot.complete_group_id,),
    ).fetchone()
    assert current is not None
    rows = tuple(
        GroupCertificateRow(
            requirement_ordinal=int(row[0]),
            requirement_version_id=str(row[1]),
            text_hash=str(row[2]),
            selected_observation_id=(
                snapshot.complete_group_alternate_observation
                if int(row[0]) == 0
                else str(row[3])
            ),
        )
        for row in connection.execute(
            """SELECT row.requirement_ordinal,row.requirement_version_id,
                      row.text_hash::text,row.selected_observation_id
                 FROM groundloop_m5_group_certificate_artifact_row row
                 JOIN groundloop_m5_published_group_certificate_binding binding
                   USING (certificate_digest)
                WHERE binding.group_version_id=%s
                  AND binding.valid_to_epoch IS NULL
                ORDER BY row.requirement_ordinal""",
            (snapshot.complete_group_id,),
        ).fetchall()
    )
    alternate = GroupMatchingCertificateArtifact(
        decision_policy_version=snapshot.policy_version,
        group_version_id=snapshot.complete_group_id,
        rows=rows,
    )
    assert alternate.certificate_digest != str(current[0])
    persist_group_certificate(connection, alternate)
    return alternate.certificate_digest


def _b3_add_catalog_only_claim_binding(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot
) -> None:
    """Reach an extra claim binding after explicitly isolating the sole FK drop."""
    table = "groundloop_m5_published_claim_certificate_binding"
    constraints_before = tuple(
        connection.execute(
            """SELECT conname,contype,pg_get_constraintdef(oid)
                 FROM pg_constraint
                WHERE conrelid=%s::regclass
                ORDER BY conname COLLATE \"C\"""",
            (table,),
        ).fetchall()
    )
    claim_fkeys = tuple(
        row
        for row in constraints_before
        if row[1] == "f" and "FOREIGN KEY (claim_id)" in str(row[2])
    )
    assert len(claim_fkeys) == 1
    removed = claim_fkeys[0]
    connection.execute(
        sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
            sql.Identifier(table), sql.Identifier(str(removed[0]))
        )
    )
    constraints_after = tuple(
        connection.execute(
            """SELECT conname,contype,pg_get_constraintdef(oid)
                 FROM pg_constraint
                WHERE conrelid=%s::regclass
                ORDER BY conname COLLATE \"C\"""",
            (table,),
        ).fetchall()
    )
    assert constraints_after == tuple(
        row for row in constraints_before if row != removed
    )
    connection.execute(
        """INSERT INTO groundloop_m5_published_claim_certificate_binding
           (claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
            certificate_digest)
           SELECT 'b3-catalog-only-extra-claim',valid_from_epoch,valid_to_epoch,
                  sealed_revision,certificate_digest
             FROM groundloop_m5_published_claim_certificate_binding
            WHERE claim_id=%s AND valid_to_epoch IS NULL""",
        (snapshot.claim_ids[1],),
    )


def _b3_reseed_published_group_semantic_mismatch(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot
) -> None:
    connection.execute(
        """CREATE TEMP TABLE b3_saved_published_group_state ON COMMIT DROP AS
           TABLE groundloop_m5_published_group_state"""
    )
    connection.execute("TRUNCATE groundloop_m5_published_group_state")
    connection.execute(
        """INSERT INTO groundloop_m5_published_group_state (
             group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
             requirement_count,satisfied_count,matching_size,complete,
             decision_policy_version,certificate_digest)
           SELECT group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                  requirement_count,
                  CASE WHEN group_version_id=%s THEN satisfied_count-1
                       ELSE satisfied_count END,
                  matching_size,complete,decision_policy_version,certificate_digest
             FROM b3_saved_published_group_state""",
        (snapshot.complete_group_id,),
    )


def _b3_reseed_published_claim_semantic_mismatch(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot
) -> None:
    connection.execute(
        """CREATE TEMP TABLE b3_saved_published_claim_state ON COMMIT DROP AS
           TABLE groundloop_m5_published_claim_state"""
    )
    connection.execute("TRUNCATE groundloop_m5_published_claim_state")
    connection.execute(
        """INSERT INTO groundloop_m5_published_claim_state (
             claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
             support_count,refute_count,best_support_score,best_refute_score,
             supporting_observation_ids,refuting_observation_ids,
             complete_group_count,complete_group_ids,status,
             decision_policy_version,certificate_digest)
           SELECT claim_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                  support_count,refute_count,
                  CASE WHEN claim_id=%s THEN 0.9 ELSE best_support_score END,
                  best_refute_score,supporting_observation_ids,
                  refuting_observation_ids,complete_group_count,complete_group_ids,
                  status,decision_policy_version,certificate_digest
             FROM b3_saved_published_claim_state""",
        (snapshot.claim_ids[1],),
    )


_B3_M5_MATERIALIZED_MUTATIONS = {
    "m5_requirement_materialized": "groundloop_m5_requirement_state_materialized",
    "m5_group_materialized": "groundloop_m5_group_state_materialized",
    "m5_claim_materialized": "groundloop_m5_claim_state_materialized",
    "m5_answer_materialized": "groundloop_m5_answer_state_materialized",
}

_B3_M5_PUBLISHED_MUTATIONS = {
    "m5_requirement_published": ("groundloop_m5_published_requirement_state",),
    "m5_answer_published": ("groundloop_m5_published_answer_state",),
}


def _apply_b3_preddl_mutation(
    connection: Connection[Any], snapshot: _B3ActivatedSnapshot, mutation: str
) -> None:
    if mutation == "epoch_structural":
        connection.execute(
            """UPDATE groundloop_epoch SET structural_status='received'
                WHERE epoch_id=%s""",
            (snapshot.epoch_id,),
        )
    elif mutation == "epoch_semantic":
        connection.execute(
            """UPDATE groundloop_epoch SET semantic_status='complete',sealed_at=NULL
                WHERE epoch_id=%s""",
            (snapshot.epoch_id,),
        )
    elif mutation == "epoch_evaluation":
        connection.execute(
            "UPDATE groundloop_epoch SET evaluation_state='pending' WHERE epoch_id=%s",
            (snapshot.epoch_id,),
        )
    elif mutation == "epoch_publication_mode":
        connection.execute(
            """UPDATE groundloop_epoch SET publication_mode='provisional'
                WHERE epoch_id=%s""",
            (snapshot.epoch_id,),
        )
    elif mutation == "epoch_revision":
        connection.execute(
            "UPDATE groundloop_epoch SET revision=revision+1 WHERE epoch_id=%s",
            (snapshot.epoch_id,),
        )
    elif mutation == "m4_head":
        future = _b3_future_epoch(connection, "b3-m4-head")
        connection.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (future,),
        )
    elif mutation == "m5_head":
        connection.execute(
            """UPDATE groundloop_m5_publication_head
                  SET sealed_revision=sealed_revision+1""",
        )
    elif mutation == "overlapping_policy":
        future = _b3_future_epoch(connection, "b3-overlapping-policy-end")
        connection.execute(
            """INSERT INTO groundloop_decision_policy (
                 policy_version,support_threshold,refute_threshold,tie_rule_version,
                 valid_from_epoch,valid_to_epoch)
               VALUES ('b3-overlap',.8,.8,'v1',%s,%s)""",
            (snapshot.epoch_id, future),
        )
    elif mutation == "currency_future_close":
        _b3_close_current_rows(
            connection,
            name="currency",
            tables=("groundloop_published_observation_currency",),
        )
    elif mutation == "m4_claim_materialized":
        connection.execute(
            """UPDATE groundloop_claim_state_materialized
                  SET best_support_score=0.9 WHERE claim_id=%s""",
            (snapshot.claim_ids[1],),
        )
    elif mutation == "m4_answer_materialized":
        connection.execute(
            """UPDATE groundloop_answer_state_materialized
                  SET status='partially_supported' WHERE answer_version_id=%s""",
            (snapshot.answer_id,),
        )
    elif mutation == "m4_claim_certificate":
        connection.execute(
            """UPDATE groundloop_claim_certificate
                  SET support_observation_id=%s WHERE claim_id=%s""",
            (snapshot.direct_support_observations[1], snapshot.claim_ids[1]),
        )
    elif mutation == "m4_claim_published":
        connection.execute(
            """UPDATE groundloop_published_claim_state SET certificate_digest=%s
                WHERE claim_id=%s AND valid_to_epoch IS NULL""",
            (
                hashlib.sha256(b"b3-wrong-m4-certificate").hexdigest(),
                snapshot.claim_ids[1],
            ),
        )
    elif mutation == "m4_answer_published":
        connection.execute(
            """UPDATE groundloop_published_answer_state SET status='partially_supported'
                WHERE answer_version_id=%s AND valid_to_epoch IS NULL""",
            (snapshot.answer_id,),
        )
    elif mutation in _B3_M5_MATERIALIZED_MUTATIONS:
        connection.execute(
            sql.SQL("UPDATE {} SET updated_revision=updated_revision+1").format(
                sql.Identifier(_B3_M5_MATERIALIZED_MUTATIONS[mutation])
            )
        )
    elif mutation in _B3_M5_PUBLISHED_MUTATIONS:
        _b3_close_current_rows(
            connection,
            name=mutation,
            tables=_B3_M5_PUBLISHED_MUTATIONS[mutation],
        )
    elif mutation == "m5_group_published":
        _b3_reseed_published_group_semantic_mismatch(connection, snapshot)
    elif mutation == "m5_claim_published":
        _b3_reseed_published_claim_semantic_mismatch(connection, snapshot)
    elif mutation == "m5_group_binding_missing":
        table = "groundloop_m5_published_group_certificate_binding"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """DELETE FROM groundloop_m5_published_group_certificate_binding
                    WHERE group_version_id=%s AND valid_to_epoch IS NULL""",
                (snapshot.complete_group_id,),
            )
    elif mutation == "m5_group_binding_extra":
        table = "groundloop_m5_published_group_certificate_binding"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """INSERT INTO groundloop_m5_published_group_certificate_binding
                   (group_version_id,valid_from_epoch,valid_to_epoch,sealed_revision,
                    certificate_digest)
                   SELECT %s,valid_from_epoch,valid_to_epoch,sealed_revision,
                          certificate_digest
                     FROM groundloop_m5_published_group_certificate_binding
                    WHERE group_version_id=%s AND valid_to_epoch IS NULL""",
                (snapshot.partial_group_id, snapshot.complete_group_id),
            )
    elif mutation == "m5_group_binding_wrong":
        alternate_digest = _b3_alternate_group_certificate(connection, snapshot)
        table = "groundloop_m5_published_group_certificate_binding"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """UPDATE groundloop_m5_published_group_certificate_binding
                      SET certificate_digest=%s
                    WHERE group_version_id=%s AND valid_to_epoch IS NULL""",
                (alternate_digest, snapshot.complete_group_id),
            )
    elif mutation == "m5_claim_binding_missing":
        table = "groundloop_m5_published_claim_certificate_binding"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """DELETE FROM groundloop_m5_published_claim_certificate_binding
                    WHERE claim_id=%s AND valid_to_epoch IS NULL""",
                (snapshot.claim_ids[2],),
            )
    elif mutation == "m5_claim_binding_wrong":
        table = "groundloop_m5_published_claim_certificate_binding"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """UPDATE groundloop_m5_published_claim_certificate_binding target
                      SET certificate_digest=source.certificate_digest
                     FROM groundloop_m5_published_claim_certificate_binding source
                    WHERE target.claim_id=%s AND target.valid_to_epoch IS NULL
                      AND source.claim_id=%s AND source.valid_to_epoch IS NULL""",
                (snapshot.claim_ids[1], snapshot.claim_ids[4]),
            )
    elif mutation == "m5_claim_binding_extra_catalog_corruption":
        _b3_add_catalog_only_claim_binding(connection, snapshot)
    elif mutation == "m5_group_artifact_header":
        table = "groundloop_m5_group_certificate_artifact"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """UPDATE groundloop_m5_group_certificate_artifact artifact
                      SET requirement_count=requirement_count-1
                     FROM groundloop_m5_published_group_certificate_binding binding
                    WHERE artifact.certificate_digest=binding.certificate_digest
                      AND binding.group_version_id=%s""",
                (snapshot.complete_group_id,),
            )
    elif mutation == "m5_group_artifact_row":
        table = "groundloop_m5_group_certificate_artifact_row"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """UPDATE groundloop_m5_group_certificate_artifact_row row
                      SET selected_observation_id=%s
                     FROM groundloop_m5_published_group_certificate_binding binding
                    WHERE row.certificate_digest=binding.certificate_digest
                      AND binding.group_version_id=%s
                      AND row.requirement_ordinal=0""",
                (
                    snapshot.complete_group_alternate_observation,
                    snapshot.complete_group_id,
                ),
            )
    elif mutation == "m5_claim_artifact":
        table = "groundloop_m5_claim_certificate_artifact"
        with _b3_user_triggers_disabled(connection, table):
            connection.execute(
                """UPDATE groundloop_m5_claim_certificate_artifact
                      SET direct_support_observation_id=%s WHERE claim_id=%s""",
                (snapshot.direct_support_observations[1], snapshot.claim_ids[1]),
            )
    elif mutation == "currency_missing_current":
        connection.execute(
            "DELETE FROM groundloop_observation_currency WHERE observation_id=%s",
            (snapshot.direct_support_observations[0],),
        )
    elif mutation == "currency_current_extra":
        connection.execute(
            """INSERT INTO groundloop_observation_currency (
                 subject_kind,subject_id,chunk_version_id,task_type,
                 observation_id,installed_revision)
               SELECT subject_kind,subject_id,chunk_version_id,task_type,
                      observation_id,%s
                 FROM groundloop_semantic_observation WHERE observation_id=%s""",
            (snapshot.revision, snapshot.currency_extra_observation),
        )
    elif mutation == "currency_current_different":
        connection.execute(
            """UPDATE groundloop_observation_currency SET observation_id=%s
                WHERE observation_id=%s""",
            (
                snapshot.currency_alternate_observation,
                snapshot.direct_support_observations[0],
            ),
        )
    elif mutation == "typed_update_history":
        history_epoch = connection.execute(
            """INSERT INTO groundloop_epoch (
                 event_id,payload_hash,revision,structural_status,semantic_status,
                 evaluation_state,publication_mode,sealed_at)
               VALUES ('b3-history',%s,0,'committed','pending','pending',
                       'provisional',NULL) RETURNING epoch_id""",
            (hashlib.sha256(b"b3-history").hexdigest(),),
        ).fetchone()
        assert history_epoch is not None
        connection.execute(
            """INSERT INTO groundloop_m5_update (
                 epoch_id,update_kind,previous_published_epoch_id,
                 decision_policy_version,manifest)
               VALUES (%s,'observe_requirement',%s,%s,'{}'::jsonb)""",
            (int(history_epoch[0]), snapshot.epoch_id, snapshot.policy_version),
        )
    elif mutation == "runtime_epoch_history":
        _seed_b3_runtime_history(connection, snapshot)
    else:  # pragma: no cover - the parameter table is closed below.
        raise AssertionError(f"unknown B3 mutation: {mutation}")
    connection.commit()


@pytest.fixture(scope="module")
def installed() -> Iterator[Connection[Any]]:
    schema_name = f"d25_migration_017_{uuid.uuid4().hex}"
    with psycopg.connect(_database_url(), autocommit=True) as admin:
        before = tuple(
            row[0]
            for row in admin.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT LIKE 'pg_temp_%' "
                "AND schema_name NOT LIKE 'pg_toast_temp_%' "
                "ORDER BY schema_name"
            )
        )
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    connection: Connection[Any] = psycopg.connect(_database_url())
    try:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
        )
        connection.commit()
        apply_legacy_migrations(connection)
        connection.commit()
        install_m5_core_bundle(connection)
        connection.commit()
        install_m5_runtime_bundle(connection)
        connection.commit()
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        yield connection
    finally:
        connection.close()
        with psycopg.connect(_database_url(), autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
            after = tuple(
                row[0]
                for row in admin.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT LIKE 'pg_temp_%' "
                    "AND schema_name NOT LIKE 'pg_toast_temp_%' "
                    "ORDER BY schema_name"
                )
            )
            assert after == before


@pytest.fixture
def preledger_lane_a_decoder() -> Iterator[Connection[Any]]:
    """Expose migration-time constructors; this is not B2 deferred acceptance."""
    with _pre017_schema() as (connection, schema_name):
        connection.execute(
            sql.SQL("SET search_path TO {}, pg_catalog").format(
                sql.Identifier(schema_name)
            )
        )
        connection.execute(M5_PERSISTED_MATCHING_MIGRATION_PATH.read_text())
        connection.commit()
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
            (M5_PERSISTED_MATCHING_BUNDLE_ID,),
        ).fetchone() == (0,)
        yield connection


def test_identity_is_bound_to_exact_accepted_016() -> None:
    accepted = m5_runtime_recovery_bundle_identity()
    assert (
        accepted.bundle_id,
        accepted.migration_sha256,
        accepted.bundle_sha256,
        accepted.oracle_sha256,
        accepted.prerequisite_sha256,
    ) == (
        M5_ACCEPTED_RECOVERY_BUNDLE_ID,
        M5_ACCEPTED_RECOVERY_MIGRATION_SHA256,
        M5_ACCEPTED_RECOVERY_BUNDLE_SHA256,
        M5_ACCEPTED_RECOVERY_ORACLE_SHA256,
        M5_ACCEPTED_RECOVERY_PREREQUISITE_SHA256,
    )
    identity = m5_persisted_matching_bundle_identity()
    assert identity.bundle_id == M5_PERSISTED_MATCHING_BUNDLE_ID
    assert identity.prerequisite_sha256 == accepted.bundle_sha256
    assert identity.migration_sha256 == (
        M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256
    )
    assert identity.bundle_sha256 == M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256


def test_d26_static_replacement_and_retained_trigger_inventory() -> None:
    source = M5_PERSISTED_MATCHING_MIGRATION_PATH.read_text()
    replacements = tuple(
        re.findall(
            r"(?im)^CREATE OR REPLACE FUNCTION\s+([a-z0-9_]+)\s*\(",
            source,
        )
    )
    assert replacements == (
        "groundloop_m5_validate_working_state_mutation",
        "groundloop_m5_validate_revision_interval_mutation",
        "groundloop_m5_validate_event_result_children",
    )
    created_triggers = set(
        re.findall(
            r"(?im)^CREATE (?:CONSTRAINT )?TRIGGER\s+([a-z0-9_]+)",
            source,
        )
    )
    assert created_triggers.isdisjoint(D26_RETAINED_RESULT_TRIGGERS)
    migration_015 = M5_PERSISTED_MATCHING_MIGRATION_PATH.with_name("015_m5_runtime.sql")
    assert hashlib.sha256(migration_015.read_bytes()).hexdigest() == (
        "85cb7f8e6a33273ce67fc6b4160e74a3aff314cd084df7ac3647cadae930185c"
    )
    assert source.count("m5-changed-state-absence-artifact-v1") >= 2
    assert "CREATE TABLE" in source
    assert "nullable" not in source.lower()


def test_d26_absence_digest_cross_oracle_framing_and_unicode() -> None:
    object_ids = (
        "object",
        "a" * 7,
        "a" * 8,
        "a" * 63,
        "a" * 64,
        "a" * 65,
        "é",
        "e\u0301",
        "界" * 21,
        "界" * 22,
    )
    kinds = ("requirement_state", "group_state", "group_certificate")
    golden = {
        "requirement_state": (
            "b7ed5a826a55c9a38dd2dfa5854b74bb7e012a39fd227a1b3529e6372a3b1de2"
        ),
        "group_state": (
            "582df804bace81e49a09c875ea893dfb594122c4067740ed0b9784cdd29bd611"
        ),
        "group_certificate": (
            "1196cc2f6866bb05375f2920ee040c4debf5cfba354126a81b8663e4b19a324d"
        ),
    }
    assert {
        kind: stable_m5_digest(
            "m5-changed-state-absence-artifact-v1",
            enum_field(kind),
            text_field("object"),
        )
        for kind in kinds
    } == golden
    baseline_preimage = _framed_preimage(
        "m5-changed-state-absence-artifact-v1",
        *_typed("enum", "group_state"),
        *_typed("text", "ab"),
    )
    prefix_adversary = _framed_preimage(
        "m5-changed-state-absence-artifact-v1enum",
        *_typed("text", "group_state"),
        *_typed("text", "ab"),
    )
    one_byte_mutation = baseline_preimage[:-1] + bytes([baseline_preimage[-1] ^ 1])
    assert (
        len(
            {
                hashlib.sha256(value).hexdigest()
                for value in (
                    baseline_preimage,
                    prefix_adversary,
                    one_byte_mutation,
                )
            }
        )
        == 3
    )
    with _pre017_schema() as (connection, _):
        for preimage in (baseline_preimage, prefix_adversary, one_byte_mutation):
            assert connection.execute(
                "SELECT encode(public.digest(%s::bytea,'sha256'),'hex')",
                (preimage,),
            ).fetchone() == (hashlib.sha256(preimage).hexdigest(),)
        for kind in kinds:
            for object_id in object_ids:
                expected = stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    enum_field(kind),
                    text_field(object_id),
                )
                assert connection.execute(
                    """SELECT groundloop_m5_digest_text_fields(ARRAY[
                         'm5-changed-state-absence-artifact-v1',
                         'enum',%s,'text',%s])""",
                    (kind, object_id),
                ).fetchone() == (expected,)
        same_id = "same-object"
        legal_hashes = {
            stable_m5_digest(
                "m5-changed-state-absence-artifact-v1",
                enum_field(kind),
                text_field(same_id),
            )
            for kind in kinds
        }
        assert len(legal_hashes) == 3
        baseline = stable_m5_digest(
            "m5-changed-state-absence-artifact-v1",
            enum_field("group_state"),
            text_field(same_id),
        )
        mutations = (
            (
                "m5-changed-state-absence-artifact-v2",
                "group_state",
                same_id,
            ),
            (
                "m5-changed-state-absence-artifact-v1",
                "requirement_state",
                same_id,
            ),
            (
                "m5-changed-state-absence-artifact-v1",
                "group_state",
                f"{same_id}-changed",
            ),
        )
        for domain, kind, object_id in mutations:
            expected = stable_m5_digest(
                domain,
                enum_field(kind),
                text_field(object_id),
            )
            assert expected != baseline
            assert connection.execute(
                """SELECT groundloop_m5_digest_text_fields(ARRAY[
                     %s,'enum',%s,'text',%s])""",
                (domain, kind, object_id),
            ).fetchone() == (expected,)
        connection.commit()


def test_d26_partial_replace_seals_absent_predecessor_and_present_successor() -> None:
    with _pre017_schema() as (connection, _):
        snapshot = _seed_b3_activated_snapshot(connection, "d26-partial-replace")
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        assert predecessor.certificate_digest is None
        sealed = _seal_d26_event(
            connection,
            prefix="d26-partial-replace",
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            predecessor=predecessor,
            action="REPLACE",
            retire_current_physical=True,
        )
        successor = f"{predecessor.group_id}-successor"
        successor_requirement = f"{successor}-requirement"
        expected_absent = {
            *(
                ("requirement_state", requirement_id)
                for requirement_id in predecessor.requirement_ids
            ),
            ("group_state", predecessor.group_id),
        }
        expected_present = {
            ("requirement_state", successor_requirement),
            ("group_state", successor),
        }
        assert {(row[0], row[1]) for row in sealed.references} == (
            expected_absent | expected_present
        )
        for kind, object_id, state_hash, reference_digest in sealed.references:
            if (kind, object_id) in expected_absent:
                expected_state_hash = stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    enum_field(kind),
                    text_field(object_id),
                )
            elif kind == "requirement_state":
                expected_state_hash = runtime_digests.requirement_state_artifact_digest(
                    requirement_version_id=successor_requirement,
                    witness_hashes=(),
                    supporting_observation_ids=(),
                    witness_count=0,
                    satisfied=False,
                    decision_policy_version=snapshot.policy_version,
                )
            else:
                expected_state_hash = runtime_digests.group_state_artifact_digest(
                    group_version_id=successor,
                    requirement_count=1,
                    satisfied_count=0,
                    matching_size=0,
                    complete=False,
                    decision_policy_version=snapshot.policy_version,
                    certificate_digest=None,
                )
            assert state_hash == expected_state_hash
            assert reference_digest == runtime_digests.changed_state_reference_digest(
                (kind, object_id, sealed.epoch_id, sealed.revision, state_hash)
            )
        assert connection.execute(
            """SELECT lifecycle_state,group_family_id,supersedes_group_version_id
                 FROM groundloop_m5_group_version WHERE group_version_id=%s""",
            (successor,),
        ).fetchone() == ("PUBLISHED", predecessor.family_id, predecessor.group_id)
        assert connection.execute(
            """SELECT valid_from_epoch,valid_to_epoch
                 FROM groundloop_m5_group_validity WHERE group_version_id=%s""",
            (successor,),
        ).fetchone() == (sealed.epoch_id, None)
        assert connection.execute(
            """SELECT valid_from_epoch,sealed_revision,satisfied
                 FROM groundloop_m5_published_requirement_state
                WHERE requirement_version_id=%s""",
            (successor_requirement,),
        ).fetchone() == (sealed.epoch_id, sealed.revision, False)
        assert connection.execute(
            """SELECT valid_from_epoch,sealed_revision,complete
                 FROM groundloop_m5_published_group_state
                WHERE group_version_id=%s""",
            (successor,),
        ).fetchone() == (sealed.epoch_id, sealed.revision, False)
        assert connection.execute(
            """SELECT requirement_count,matching_size,installed_epoch_id,
                      installed_revision
                 FROM groundloop_m5_matching_hall_current
                WHERE group_version_id=%s""",
            (successor,),
        ).fetchone() == (1, 0, sealed.epoch_id, sealed.revision)
        assert connection.execute(
            """SELECT count(*) FROM groundloop_m5_matching_hall_current
                WHERE group_version_id=%s""",
            (predecessor.group_id,),
        ).fetchone() == (0,)
        connection.commit()


@pytest.mark.parametrize("action", ("RETIRE", "REPLACE"))
def test_d26_complete_predecessor_closes_binding_and_retains_artifact(
    action: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-complete-{action.lower()}"
        snapshot = _seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=True,
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.complete_group_id,
            base=snapshot.epoch_id,
        )
        assert predecessor.certificate_digest is not None
        artifact_before = connection.execute(
            """SELECT to_jsonb(artifact),
                      coalesce(jsonb_agg(to_jsonb(row) ORDER BY row.requirement_ordinal)
                               FILTER (WHERE row.certificate_digest IS NOT NULL),'[]')
                 FROM groundloop_m5_group_certificate_artifact artifact
                 LEFT JOIN groundloop_m5_group_certificate_artifact_row row
                   USING(certificate_digest)
                WHERE artifact.certificate_digest=%s
                GROUP BY artifact.certificate_digest""",
            (predecessor.certificate_digest,),
        ).fetchone()
        assert artifact_before is not None
        claim_id = snapshot.claim_ids[0]
        claim_certificate_before = connection.execute(
            """SELECT certificate_digest
                 FROM groundloop_m5_published_claim_certificate_binding
                WHERE claim_id=%s AND valid_to_epoch IS NULL""",
            (claim_id,),
        ).fetchone()
        assert claim_certificate_before is not None
        connection.commit()
        sealed = _seal_d26_event(
            connection,
            prefix=prefix,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            predecessor=predecessor,
            action=action,
            retire_current_physical=True,
        )
        expected_absent = {
            *(
                ("requirement_state", requirement_id)
                for requirement_id in predecessor.requirement_ids
            ),
            ("group_state", predecessor.group_id),
            ("group_certificate", predecessor.group_id),
        }
        expected_present = {
            ("claim_state", claim_id),
            ("claim_certificate", claim_id),
        }
        if action == "REPLACE":
            successor = f"{predecessor.group_id}-successor"
            expected_present |= {
                ("requirement_state", f"{successor}-requirement"),
                ("group_state", successor),
            }
        assert {
            (kind, object_id) for kind, object_id, _, _ in sealed.references
        } == expected_absent | expected_present
        claim_state_row = connection.execute(
            """SELECT support_count,refute_count,best_support_score,
                      best_refute_score,supporting_observation_ids,
                      refuting_observation_ids,complete_group_count,
                      complete_group_ids,status,decision_policy_version,
                      certificate_digest
                 FROM groundloop_m5_published_claim_state
                WHERE claim_id=%s AND valid_from_epoch=%s""",
            (claim_id, sealed.epoch_id),
        ).fetchone()
        assert claim_state_row is not None
        materialized_claim_state_row = connection.execute(
            """SELECT support_count,refute_count,best_support_score,
                      best_refute_score,supporting_observation_ids,
                      refuting_observation_ids,complete_group_count,
                      complete_group_ids,status,decision_policy_version,
                      certificate_digest
                 FROM groundloop_m5_claim_state_materialized
                WHERE claim_id=%s""",
            (claim_id,),
        ).fetchone()
        assert materialized_claim_state_row == claim_state_row
        assert (
            int(claim_state_row[6]),
            len(tuple(claim_state_row[7])),
            str(claim_state_row[8]),
        ) == (1, 1, "supported")
        assert predecessor.group_id not in tuple(claim_state_row[7])
        expected_claim_hash = runtime_digests.claim_state_artifact_digest(
            claim_id=claim_id,
            support_count=int(claim_state_row[0]),
            refute_count=int(claim_state_row[1]),
            best_support_score=(
                None if claim_state_row[2] is None else float(claim_state_row[2])
            ),
            best_refute_score=(
                None if claim_state_row[3] is None else float(claim_state_row[3])
            ),
            supporting_observation_ids=tuple(claim_state_row[4]),
            refuting_observation_ids=tuple(claim_state_row[5]),
            complete_group_count=int(claim_state_row[6]),
            complete_group_ids=tuple(claim_state_row[7]),
            status=str(claim_state_row[8]),
            decision_policy_version=str(claim_state_row[9]),
            certificate_digest=str(claim_state_row[10]),
        )
        assert (
            next(
                state_hash
                for kind, object_id, state_hash, _ in sealed.references
                if (kind, object_id) == ("claim_state", claim_id)
            )
            == expected_claim_hash
        )
        for kind, object_id, state_hash, reference_digest in sealed.references:
            assert reference_digest == runtime_digests.changed_state_reference_digest(
                (kind, object_id, sealed.epoch_id, sealed.revision, state_hash)
            )
        for kind, object_id, state_hash, _ in sealed.references:
            if (kind, object_id) in expected_absent:
                assert state_hash == stable_m5_digest(
                    "m5-changed-state-absence-artifact-v1",
                    enum_field(kind),
                    text_field(object_id),
                )
        assert connection.execute(
            """SELECT valid_to_epoch
                 FROM groundloop_m5_published_group_certificate_binding
                WHERE group_version_id=%s AND certificate_digest=%s""",
            (predecessor.group_id, predecessor.certificate_digest),
        ).fetchone() == (sealed.epoch_id,)
        assert connection.execute(
            """SELECT valid_from_epoch,valid_to_epoch,certificate_digest
                 FROM groundloop_m5_published_claim_certificate_binding
                WHERE claim_id=%s ORDER BY valid_from_epoch""",
            (claim_id,),
        ).fetchall() == [
            (snapshot.epoch_id, sealed.epoch_id, claim_certificate_before[0]),
            (sealed.epoch_id, None, claim_state_row[10]),
        ]
        assert claim_state_row[10] != claim_certificate_before[0]
        artifact_after = connection.execute(
            """SELECT to_jsonb(artifact),
                      coalesce(jsonb_agg(to_jsonb(row) ORDER BY row.requirement_ordinal)
                               FILTER (WHERE row.certificate_digest IS NOT NULL),'[]')
                 FROM groundloop_m5_group_certificate_artifact artifact
                 LEFT JOIN groundloop_m5_group_certificate_artifact_row row
                   USING(certificate_digest)
                WHERE artifact.certificate_digest=%s
                GROUP BY artifact.certificate_digest""",
            (predecessor.certificate_digest,),
        ).fetchone()
        assert artifact_after == artifact_before
        connection.commit()


def test_d26_partial_retire_seals_from_reachable_bootstrapped_image() -> None:
    with _pre017_schema() as (connection, schema_name):
        snapshot = _seed_b3_activated_snapshot(connection, "d26-partial-retire")
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        assert predecessor.requirement_ids == snapshot.partial_requirement_ids
        assert predecessor.certificate_digest is None
        other_current_count = connection.execute(
            """SELECT count(*) FROM groundloop_m5_matching_observation_current
                WHERE group_version_id<>%s""",
            (predecessor.group_id,),
        ).fetchone()
        connection.commit()
        sealed = _seal_d26_event(
            connection,
            prefix="d26-partial-retire",
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            predecessor=predecessor,
            action="RETIRE",
            retire_current_physical=True,
        )
        expected_keys = {
            *(
                ("requirement_state", requirement_id)
                for requirement_id in predecessor.requirement_ids
            ),
            ("group_state", predecessor.group_id),
        }
        assert {(row[0], row[1]) for row in sealed.references} == expected_keys
        for kind, object_id, state_hash, reference_digest in sealed.references:
            assert state_hash == stable_m5_digest(
                "m5-changed-state-absence-artifact-v1",
                enum_field(kind),
                text_field(object_id),
            )
            assert reference_digest == runtime_digests.changed_state_reference_digest(
                (
                    kind,
                    object_id,
                    sealed.epoch_id,
                    sealed.revision,
                    state_hash,
                )
            )
        assert connection.execute(
            """SELECT outcome,revision,semantic_status,evaluation_state,
                      publication_mode,sealed_at IS NOT NULL
                 FROM groundloop_m5_event_result result
                 JOIN groundloop_epoch epoch USING(epoch_id)
                WHERE result.structural_event_id=%s""",
            (sealed.event_id,),
        ).fetchone() == ("sealed", 4, "sealed", "complete", "strict", True)
        assert connection.execute(
            """SELECT valid_to_epoch FROM groundloop_m5_published_group_state
                WHERE group_version_id=%s AND valid_from_epoch=%s""",
            (predecessor.group_id, snapshot.epoch_id),
        ).fetchone() == (sealed.epoch_id,)
        assert connection.execute(
            """SELECT count(*) FROM groundloop_m5_published_requirement_state
                WHERE requirement_version_id=ANY(%s)
                  AND valid_from_epoch=%s AND valid_to_epoch=%s""",
            (
                list(predecessor.requirement_ids),
                snapshot.epoch_id,
                sealed.epoch_id,
            ),
        ).fetchone() == (2,)
        for relation in (
            "groundloop_m5_matching_observation_current",
            "groundloop_m5_matching_edge_current",
            "groundloop_m5_matching_hash_mask_current",
            "groundloop_m5_matching_hall_current",
        ):
            assert connection.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE group_version_id=%s").format(
                    sql.Identifier(relation)
                ),
                (predecessor.group_id,),
            ).fetchone() == (0,)
        assert (
            connection.execute(
                """SELECT count(*) FROM groundloop_m5_matching_observation_current
                WHERE group_version_id<>%s""",
                (predecessor.group_id,),
            ).fetchone()
            == other_current_count
        )
        assert connection.execute(
            "SELECT epoch_id FROM groundloop_m4_publication_head WHERE singleton"
        ).fetchone() == (sealed.epoch_id,)
        assert connection.execute(
            """SELECT epoch_id,sealed_revision
                 FROM groundloop_m5_publication_head WHERE singleton"""
        ).fetchone() == (sealed.epoch_id, sealed.revision)
        connection.commit()

        from groundloop.m5.runtime.contracts import M5ReplayedOutcome, M5RunState
        from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

        with psycopg.connect(_database_url()) as reconnect:
            reconnect.read_only = True
            reconnect.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            reconnect.commit()
            before_replay = _all_table_xmin_count_snapshot(reconnect)
            reconnect.commit()
            replay = PostgresM5RuntimeStore(reconnect).read_typed_event_result(
                sealed.event_id,
                sealed.payload_hash,
            )
            after_replay = _all_table_xmin_count_snapshot(reconnect)
            reconnect.commit()
        assert after_replay == before_replay
        assert replay is not None
        assert replay.state is M5RunState.REPLAYED
        assert replay.replayed_outcome is M5ReplayedOutcome.SEALED
        assert replay.logical_result_hash == sealed.logical_result_hash
        assert (
            tuple(
                (
                    reference.kind.value,
                    reference.object_id,
                    reference.state_artifact_hash,
                    reference.reference_digest,
                )
                for reference in replay.changed_state_references
            )
            == sealed.references
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("fabricated_certificate_absence", "stored absence reference set mismatch"),
        ("excluded_claim_state", "changed-state reference"),
        ("excluded_answer_state", "changed-state reference"),
        ("excluded_claim_certificate", "changed-state reference"),
        ("missing_absence", "stored absence reference set mismatch"),
        ("extra_absence", "stored absence reference set mismatch"),
        ("reordered", "canonically sorted"),
        ("duplicate", "duplicate key value"),
        ("wrong_state_hash", "stored absence reference set mismatch"),
        ("wrong_reference_digest", "reference digest is incorrect"),
        ("wrong_epoch", "invalid seal coordinates"),
        ("wrong_revision", "invalid seal coordinates"),
    ),
)
def test_d26_changed_state_reference_falsifiers(
    mutation: str,
    message: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-reference-{mutation}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(psycopg.Error) as caught:
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                reference_mutation=mutation,
                force_result_constraint_only=True,
            )
        assert message in str(caught.value)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing", "D25 before-to-None change set mismatch"),
        ("wrong_before", "removed state before digest mismatch"),
        ("duplicate", "logical changes not sorted unique"),
        ("reordered", "logical changes not sorted unique"),
        ("malformed", "invalid logical change shape"),
        ("noncanonical", "truncated persisted-matching frame length"),
        ("before_none", "invalid logical change shape"),
        ("present_after", "logical change/output presence mismatch"),
    ),
)
def test_d26_logical_change_falsifiers(
    mutation: str,
    message: str,
) -> None:
    """Reject malformed D25 journals before D26 can justify an absence."""
    with _pre017_schema() as (connection, _):
        prefix = f"d26-logical-{mutation}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(psycopg.Error) as caught:
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                logical_mutation=mutation,
            )
        assert message in str(caught.value)


@pytest.mark.parametrize(
    ("mutation", "complete"),
    (
        ("wrong_requirement_before", False),
        ("wrong_group_before", False),
        ("wrong_certificate_before", True),
    ),
)
def test_d26_predecessor_digest_binding_falsifiers(
    mutation: str,
    complete: bool,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-predecessor-digest-{mutation}"
        snapshot = _seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=complete,
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=(
                snapshot.complete_group_id if complete else snapshot.partial_group_id
            ),
            base=snapshot.epoch_id,
        )
        alternate_certificate_digest = None
        if mutation == "wrong_certificate_before":
            alternate_certificate_digest = _b3_alternate_group_certificate(
                connection, snapshot
            )
            connection.commit()
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5-D26 D25 before-to-None change set mismatch",
        ):
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                child_mutation=f"{mutation}_digest",
                alternate_certificate_digest=alternate_certificate_digest,
                force_result_constraint_only=True,
            )


def test_d26_other_group_requirement_is_not_a_predecessor_change() -> None:
    with _pre017_schema() as (connection, _):
        prefix = "d26-other-group-requirement"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(psycopg.Error) as caught:
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                logical_mutation="other_group_requirement",
            )
        assert "requirement removal lacks exact deactivation" in str(caught.value)


def test_d26_independently_rejects_self_consistent_wrong_structural_payload() -> None:
    with _pre017_schema() as (connection, _):
        prefix = "d26-structural-wrong-payload"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5-D26 structural payload is not independently derived",
        ):
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                structural_mutation="wrong_structural_payload",
                force_result_constraint_only=True,
            )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            "validity_not_covering_previous_head",
            "M5-D26 predecessor group was not present and closed exactly",
        ),
        (
            "predecessor_group_not_published",
            "M5-D26 predecessor group was not present and closed exactly",
        ),
        (
            "predecessor_requirement_not_published",
            "M5-D26 predecessor requirements are not published",
        ),
        (
            "predecessor_requirement_wrong_owner",
            "M5-D26 D25 before-to-None change set mismatch",
        ),
    ),
)
def test_d26_child_independently_rejects_predecessor_provenance(
    mutation: str,
    message: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-provenance-{mutation}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(psycopg.errors.RaiseException, match=message):
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                child_mutation=mutation,
                force_result_constraint_only=True,
            )


@pytest.mark.parametrize(
    ("mutation", "action", "message"),
    (
        (
            "wrong_update_kind",
            "RETIRE",
            "M5-D26 absence reference has invalid seal coordinates",
        ),
        (
            "wrong_deactivation_event",
            "RETIRE",
            "M5-D26 sealed structural identity is inconsistent",
        ),
        (
            "wrong_deactivation_epoch",
            "RETIRE",
            "M5-D26 structural event requires exactly one deactivation",
        ),
        (
            "wrong_predecessor_id",
            "RETIRE",
            "M5-D26 predecessor group was not present and closed exactly",
        ),
        (
            "wrong_deactivation_action",
            "REPLACE",
            "M5-D26 update/deactivation mapping is invalid",
        ),
        (
            "retire_with_successor",
            "RETIRE",
            "M5-D26 update/deactivation mapping is invalid",
        ),
        (
            "wrong_successor_id",
            "REPLACE",
            "M5-D26 replacement successor is invalid",
        ),
        (
            "wrong_successor_record_hash",
            "REPLACE",
            "M5-D26 replacement successor is invalid",
        ),
        (
            "second_deactivation",
            "RETIRE",
            "M5-D26 structural event requires exactly one deactivation",
        ),
    ),
)
def test_d26_child_independently_rejects_structural_identity_mutations(
    mutation: str,
    action: str,
    message: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-structural-child-{mutation}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(psycopg.errors.RaiseException, match=message):
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action=action,
                retire_current_physical=True,
                child_mutation=mutation,
                force_result_constraint_only=True,
            )


def test_d26_nonsealed_structural_result_rejects_absence() -> None:
    with _pre017_schema() as (connection, _):
        prefix = "d26-structural-child-nonsealed"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5-D26 sealed structural identity is inconsistent",
        ):
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                child_mutation="epoch_nonsealed",
                force_result_constraint_only=True,
            )


@pytest.mark.parametrize(
    ("mutation", "complete", "message"),
    (
        ("m4_head", False, "M5-D26 sealed structural identity is inconsistent"),
        ("m5_head", False, "M5-D26 sealed structural identity is inconsistent"),
        (
            "open_group_validity",
            False,
            "M5-D26 predecessor group was not present and closed exactly",
        ),
        (
            "wrong_group_validity_close",
            False,
            "M5-D26 predecessor group was not present and closed exactly",
        ),
        (
            "open_requirement_state",
            False,
            "M5-D26 predecessor state/binding closure is incomplete",
        ),
        (
            "wrong_requirement_state_close",
            False,
            "M5-D26 predecessor state/binding closure is incomplete",
        ),
        (
            "reopen_requirement_state",
            False,
            "M5-D26 predecessor has a same-object successor",
        ),
        (
            "open_group_state",
            False,
            "M5-D26 predecessor group state and binding are inconsistent",
        ),
        (
            "wrong_group_state_close",
            False,
            "M5-D26 predecessor group state and binding are inconsistent",
        ),
        (
            "reopen_group_state",
            False,
            "M5-D26 predecessor has a same-object successor",
        ),
        (
            "open_certificate_binding",
            True,
            "M5-D26 predecessor group state and binding are inconsistent",
        ),
        (
            "wrong_certificate_binding_close",
            True,
            "M5-D26 predecessor group state and binding are inconsistent",
        ),
        (
            "reopen_complete_state_and_binding",
            True,
            "M5-D26 predecessor has a same-object successor",
        ),
    ),
)
def test_d26_seal_coordinate_and_closure_falsifiers(
    mutation: str,
    complete: bool,
    message: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-seal-{mutation}"
        snapshot = _seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=complete,
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = _d26_predecessor_from_published_image(
            connection,
            group_id=(
                snapshot.complete_group_id if complete else snapshot.partial_group_id
            ),
            base=snapshot.epoch_id,
        )
        with pytest.raises(psycopg.Error) as caught:
            _seal_d26_event(
                connection,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action="RETIRE",
                retire_current_physical=True,
                seal_mutation=mutation,
                force_result_constraint_only=True,
            )
        assert message in str(caught.value)


def test_exact_forty_one_relation_nowait_lock_tuple() -> None:
    assert M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS == EXPECTED_LOCKS
    assert len(EXPECTED_LOCKS) == 41


def test_sql_has_grouped_schema_guards_and_no_transaction_control() -> None:
    source = M5_PERSISTED_MATCHING_MIGRATION_PATH.read_text()
    assert source.count("groundloop:m5-persisted-matching-group:") == 8
    assert "BEFORE INSERT OR UPDATE OR DELETE" in source
    assert "groundloop_m5_authorize_persisted_matching_transition" in source
    assert "groundloop_m5_authorize_persisted_matching_seal" in source
    assert "groundloop_m5_authorize_persisted_matching_activation" in source
    dollar_parts = source.split("$$")
    assert len(dollar_parts) % 2 == 1
    top_level_sql = "".join(dollar_parts[::2])
    assert (
        re.search(
            r"(?im)^\s*(?:BEGIN|START\s+TRANSACTION|END|COMMIT|ROLLBACK|ABORT)\s*;",
            top_level_sql,
        )
        is None
    )
    assert "ON COMMIT DROP" in source
    assert "seal delete lacks its exact working tombstone" in source
    assert "TG_OP<>'DELETE'" in source


def test_b2_private_journal_and_single_runtime_anchor_are_installed() -> None:
    source = M5_PERSISTED_MATCHING_MIGRATION_PATH.read_text()
    assert (
        source.count(
            "coalesce(current_setting('groundloop.m5_matching_mode',true),'')"
            "<>'transition'"
        )
        == 3
    )
    assert "pg_temp.groundloop_m5_matching_transition_context" in source
    assert "pg_temp.groundloop_m5_matching_change_journal" in source
    assert "pg_temp.groundloop_m5_matching_expected_changes" in source
    assert "ON COMMIT DROP" in source
    assert "pg_current_xact_id()::text::bigint" in source
    assert (
        "VALUES ($1,pg_current_xact_id()::text::bigint,session_user,"
        "$2,$3,$4,$5,$6)" in source
    )
    assert "validation_started" in source
    assert "validation_done" in source
    assert "saw_insert" in source
    assert "saw_update" in source
    assert "saw_delete" in source
    anchor = "CREATE CONSTRAINT TRIGGER groundloop_m5_matching_runtime_anchor_validate"
    assert source.count(anchor) == 1
    assert "AFTER INSERT OR UPDATE ON groundloop_m5_runtime_epoch" in source
    assert "EXCEPT ALL" in source
    assert (
        "groundloop_m5_matching_capture_transition_anchor()\n"
        "RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER "
        "SET search_path FROM CURRENT" in source
    )
    assert (
        "groundloop_m5_matching_deferred_validate()\n"
        "RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER "
        "SET search_path FROM CURRENT" in source
    )
    assert "REVOKE ALL ON FUNCTION groundloop_m5_matching_deferred_validate()" in source


def test_b2_does_not_add_a_trigger_or_lock_for_legacy_status_delta() -> None:
    source = M5_PERSISTED_MATCHING_MIGRATION_PATH.read_text()
    assert "CREATE TRIGGER groundloop_status_delta" not in source
    assert "ON groundloop_status_delta" not in source
    assert "FROM groundloop_status_delta" in source
    assert "groundloop_status_delta" not in M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS


def test_b2_structural_header_keeps_current_and_new_policy_distinct() -> None:
    source = M5_PERSISTED_MATCHING_MIGRATION_PATH.read_text()
    assert "current_policy.valid_from_epoch<=patch.before_epoch_id" in source
    assert "current_policy.valid_to_epoch>patch.before_epoch_id" in source
    assert (
        "current_image.decision_policy_version=patch.decision_policy_version"
        not in source
    )
    assert (
        "typed_update.decision_policy_version=patch.decision_policy_version" in source
    )
    assert (
        "typed_policy.decision_policy_version=patch.decision_policy_version" in source
    )


def test_first_install_exact_rerun_and_empty_v1_image(
    installed: Connection[Any],
) -> None:
    result = install_m5_persisted_matching_bundle(installed)
    assert result.applied
    installed.commit()
    replay = install_m5_persisted_matching_bundle(installed)
    assert not replay.applied
    installed.commit()
    for relation in D25_RELATIONS:
        assert installed.execute(f"SELECT count(*) FROM {relation}").fetchone() == (0,)
    installed.commit()


def test_d26_validator_catalog_is_atomic_and_replay_stable() -> None:
    with _pre017_schema() as (connection, _):
        before = _d26_validator_catalog_image(connection)
        migration_015_ledger_before = connection.execute(
            """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                      prerequisite_sha256,applied_at
                 FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
            (M5_RUNTIME_BUNDLE_ID,),
        ).fetchone()
        assert migration_015_ledger_before is not None
        connection.commit()
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        installed = _d26_validator_catalog_image(connection)
        assert installed[0][0] == before[0][0]
        assert installed[0][1] != before[0][1]
        assert installed[0][2:] == before[0][2:]
        assert installed[1] == before[1]
        assert (
            connection.execute(
                """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                      prerequisite_sha256,applied_at
                 FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
                (M5_RUNTIME_BUNDLE_ID,),
            ).fetchone()
            == migration_015_ledger_before
        )
        for relation in (
            "groundloop_m5_activation",
            "groundloop_m5_post_terminal_attempt_audit",
        ):
            columns = {
                str(row[0])
                for row in connection.execute(
                    """SELECT attribute.attname
                         FROM pg_attribute AS attribute
                         JOIN pg_class AS relation ON relation.oid=attribute.attrelid
                         JOIN pg_namespace AS namespace
                           ON namespace.oid=relation.relnamespace
                        WHERE namespace.nspname=current_schema()
                          AND relation.relname=%s
                          AND attribute.attnum>0 AND NOT attribute.attisdropped""",
                    (relation,),
                ).fetchall()
            }
            assert columns.isdisjoint(
                {
                    "reference_ordinal",
                    "kind",
                    "object_id",
                    "state_artifact_hash",
                    "reference_digest",
                }
            )
            trigger_functions = {
                str(row[0])
                for row in connection.execute(
                    """SELECT routine.proname
                         FROM pg_trigger AS trigger
                         JOIN pg_class AS relation ON relation.oid=trigger.tgrelid
                         JOIN pg_namespace AS namespace
                           ON namespace.oid=relation.relnamespace
                         JOIN pg_proc AS routine ON routine.oid=trigger.tgfoid
                        WHERE namespace.nspname=current_schema()
                          AND relation.relname=%s AND NOT trigger.tgisinternal""",
                    (relation,),
                ).fetchall()
            }
            assert "groundloop_m5_validate_event_result_children" not in (
                trigger_functions
            )
            assert "groundloop_m5_reject_immutable_row" in trigger_functions
        deactivation_checks = tuple(
            str(row[0])
            for row in connection.execute(
                """SELECT pg_get_constraintdef(constraint_row.oid,true)
                     FROM pg_constraint AS constraint_row
                     JOIN pg_class AS relation
                       ON relation.oid=constraint_row.conrelid
                     JOIN pg_namespace AS namespace
                       ON namespace.oid=relation.relnamespace
                    WHERE namespace.nspname=current_schema()
                      AND relation.relname='groundloop_m5_group_deactivation'
                      AND constraint_row.contype='c'
                    ORDER BY constraint_row.conname"""
            ).fetchall()
        )
        assert any(
            "action = 'REPLACE'::text AND successor_group_version_id IS NOT NULL"
            in definition
            and "action = 'RETIRE'::text AND successor_group_version_id IS NULL"
            in definition
            for definition in deactivation_checks
        )
        connection.commit()
        replay_points: list[str] = []
        assert not install_m5_persisted_matching_bundle(
            connection, failure_injector=replay_points.append
        ).applied
        connection.commit()
        assert replay_points == []
        assert _d26_validator_catalog_image(connection) == installed
        assert (
            connection.execute(
                """SELECT bundle_id,bundle_sha256,migration_sha256,oracle_sha256,
                      prerequisite_sha256,applied_at
                 FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
                (M5_RUNTIME_BUNDLE_ID,),
            ).fetchone()
            == migration_015_ledger_before
        )


def test_d26_activation_and_audit_only_absence_are_schema_inexpressible(
    installed: Connection[Any],
) -> None:
    install_m5_persisted_matching_bundle(installed)
    installed.commit()
    update_kind_checks = tuple(
        str(row[0])
        for row in installed.execute(
            """SELECT pg_get_constraintdef(constraint_row.oid,true)
                 FROM pg_constraint AS constraint_row
                 JOIN pg_class AS relation
                   ON relation.oid=constraint_row.conrelid
                 JOIN pg_namespace AS namespace
                   ON namespace.oid=relation.relnamespace
                WHERE namespace.nspname=current_schema()
                  AND relation.relname='groundloop_m5_update'
                  AND constraint_row.contype='c'
                ORDER BY constraint_row.conname"""
        ).fetchall()
        if "update_kind" in str(row[0])
    )
    assert len(update_kind_checks) == 1
    assert set(re.findall(r"'([^']+)'::text", update_kind_checks[0])) == {
        "register_group",
        "replace_group",
        "retire_group",
        "observe_requirement",
        "policy_change",
        "document_insert",
        "document_delete",
        "document_replace",
    }
    assert all(
        forbidden not in update_kind_checks[0]
        for forbidden in ("activation", "audit_only", "terminal_audit_only")
    )

    foreign_keys = {
        (str(row[0]), str(row[1]), str(row[2]))
        for row in installed.execute(
            """SELECT source.relname,target.relname,
                      pg_get_constraintdef(constraint_row.oid,true)
                 FROM pg_constraint AS constraint_row
                 JOIN pg_class AS source
                   ON source.oid=constraint_row.conrelid
                 JOIN pg_class AS target
                   ON target.oid=constraint_row.confrelid
                 JOIN pg_namespace AS namespace
                   ON namespace.oid=source.relnamespace
                WHERE namespace.nspname=current_schema()
                  AND constraint_row.contype='f'
                  AND source.relname IN (
                    'groundloop_m5_runtime_epoch',
                    'groundloop_m5_event_result',
                    'groundloop_m5_event_result_state_reference',
                    'groundloop_m5_post_terminal_attempt_audit'
                  )"""
        ).fetchall()
    }
    assert any(
        source == "groundloop_m5_runtime_epoch"
        and target == "groundloop_m5_update"
        and "FOREIGN KEY (epoch_id)" in definition
        for source, target, definition in foreign_keys
    )
    assert any(
        source == "groundloop_m5_event_result"
        and target == "groundloop_m5_runtime_epoch"
        and "FOREIGN KEY (epoch_id, structural_event_id)" in definition
        for source, target, definition in foreign_keys
    )
    assert any(
        source == "groundloop_m5_event_result_state_reference"
        and target == "groundloop_m5_event_result"
        and "FOREIGN KEY (structural_event_id)" in definition
        for source, target, definition in foreign_keys
    )
    assert not any(
        source == "groundloop_m5_event_result_state_reference"
        and target
        in {
            "groundloop_m5_activation",
            "groundloop_m5_post_terminal_attempt_audit",
        }
        for source, target, _ in foreign_keys
    )

    columns = {
        str(row[0]): tuple(str(value) for value in row[1])
        for row in installed.execute(
            """SELECT relation.relname,
                      array_agg(attribute.attname ORDER BY attribute.attnum)
                 FROM pg_class AS relation
                 JOIN pg_namespace AS namespace
                   ON namespace.oid=relation.relnamespace
                 JOIN pg_attribute AS attribute
                   ON attribute.attrelid=relation.oid
                  AND attribute.attnum>0 AND NOT attribute.attisdropped
                WHERE namespace.nspname=current_schema()
                  AND relation.relname IN (
                    'groundloop_m5_activation',
                    'groundloop_m5_post_terminal_attempt_audit'
                  )
                GROUP BY relation.relname"""
        ).fetchall()
    }
    assert set(columns) == {
        "groundloop_m5_activation",
        "groundloop_m5_post_terminal_attempt_audit",
    }
    assert "epoch_id" not in columns["groundloop_m5_activation"]
    for relation in columns:
        assert "structural_event_id" not in columns[relation]
        assert "state_reference_count" not in columns[relation]
        assert "changed_state_set_hash" not in columns[relation]
    assert any(
        source == "groundloop_m5_post_terminal_attempt_audit"
        and target == "groundloop_m5_event_result"
        and "FOREIGN KEY (terminal_logical_result_hash)" in definition
        for source, target, definition in foreign_keys
    )

    validator = installed.execute(
        """SELECT pg_get_functiondef(routine.oid)
             FROM pg_proc AS routine
             JOIN pg_namespace AS namespace
               ON namespace.oid=routine.pronamespace
            WHERE namespace.nspname=current_schema()
              AND routine.proname='groundloop_m5_validate_event_result_children'"""
    ).fetchone()
    assert validator is not None
    validator_source = str(validator[0])
    assert "result_row.outcome = 'sealed'" in validator_source
    assert re.search(
        r"d26_update_kind\s+IN\s*\(\s*'replace_group'\s*,\s*"
        r"'retire_group'\s*\)",
        validator_source,
    )
    installed.commit()


def test_promotion_envelope_allows_normal_dml_with_unset_mode(
    installed: Connection[Any],
) -> None:
    assert installed.execute(
        "SELECT nullif(current_setting('groundloop.m5_matching_mode',true),'')"
    ).fetchone() == (None,)
    event = "d26-normal-mode-epoch"
    installed.execute(
        """INSERT INTO groundloop_epoch
           (event_id,payload_hash,revision,structural_status,semantic_status,
            evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,1,'committed','failed','failed','provisional',NULL)""",
        (event, hashlib.sha256(event.encode()).hexdigest()),
    )
    installed.execute("SET CONSTRAINTS ALL IMMEDIATE")
    installed.rollback()


def test_b3a_activated_no_history_backfills_rich_independent_snapshot() -> None:
    with _pre017_schema() as (connection, _):
        snapshot = _seed_b3_activated_snapshot(connection, "b3a-rich")
        epoch_count_before = connection.execute(
            "SELECT count(*) FROM groundloop_epoch"
        ).fetchone()
        d24_anchor_counts_before = _b3_relation_counts(
            connection, _B3_D24_ANCHOR_RELATIONS
        )
        assert epoch_count_before == (1,)
        assert d24_anchor_counts_before == tuple(
            (relation, 0) for relation in _B3_D24_ANCHOR_RELATIONS
        )
        connection.commit()
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        assert (
            connection.execute("SELECT count(*) FROM groundloop_epoch").fetchone()
            == epoch_count_before
        )
        assert (
            _b3_relation_counts(connection, _B3_D24_ANCHOR_RELATIONS)
            == d24_anchor_counts_before
        )
        assert connection.execute(
            "SELECT decision_policy_version,installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current WHERE singleton"
        ).fetchone() == (
            snapshot.policy_version,
            snapshot.epoch_id,
            snapshot.revision,
        )

        expected_observations = tuple(
            sorted(
                (
                    observation_id,
                    requirement_id,
                    group_id,
                    ordinal,
                    text_hash,
                    snapshot.epoch_id,
                    snapshot.revision,
                )
                for (
                    observation_id,
                    requirement_id,
                    group_id,
                    ordinal,
                    text_hash,
                ) in snapshot.requirement_observations
            )
        )
        assert (
            tuple(
                connection.execute(
                    """SELECT observation_id,requirement_version_id,group_version_id,
                          requirement_ordinal,text_hash::text,installed_epoch_id,
                          installed_revision
                     FROM groundloop_m5_matching_observation_current
                    ORDER BY observation_id COLLATE \"C\""""
                ).fetchall()
            )
            == expected_observations
        )

        observation_by_requirement_hash: dict[tuple[str, str, str, int], int] = {}
        for (
            _,
            requirement_id,
            group_id,
            ordinal,
            text_hash,
        ) in snapshot.requirement_observations:
            key = (requirement_id, text_hash, group_id, ordinal)
            observation_by_requirement_hash[key] = (
                observation_by_requirement_hash.get(key, 0) + 1
            )
        expected_edges = tuple(
            sorted(
                (
                    requirement_id,
                    text_hash,
                    group_id,
                    ordinal,
                    refcount,
                    snapshot.epoch_id,
                    snapshot.revision,
                )
                for (
                    requirement_id,
                    text_hash,
                    group_id,
                    ordinal,
                ), refcount in observation_by_requirement_hash.items()
            )
        )
        assert (
            tuple(
                connection.execute(
                    """SELECT requirement_version_id,text_hash::text,group_version_id,
                          requirement_ordinal,refcount,
                          installed_epoch_id,installed_revision
                     FROM groundloop_m5_matching_edge_current
                    ORDER BY requirement_version_id COLLATE \"C\",text_hash"""
                ).fetchall()
            )
            == expected_edges
        )

        masks: dict[tuple[str, str], int] = {}
        for _, _, group_id, ordinal, text_hash in snapshot.requirement_observations:
            masks[(group_id, text_hash)] = masks.get((group_id, text_hash), 0) | (
                1 << ordinal
            )
        expected_masks = tuple(
            sorted(
                (
                    group_id,
                    text_hash,
                    mask,
                    snapshot.epoch_id,
                    snapshot.revision,
                )
                for (group_id, text_hash), mask in masks.items()
            )
        )
        assert (
            tuple(
                connection.execute(
                    """SELECT group_version_id,text_hash::text,mask,
                          installed_epoch_id,installed_revision
                     FROM groundloop_m5_matching_hash_mask_current
                    ORDER BY group_version_id COLLATE \"C\",text_hash"""
                ).fetchall()
            )
            == expected_masks
        )

        expected_hall = tuple(
            sorted(
                (
                    (
                        snapshot.complete_group_id,
                        2,
                        [0, 1, 1, 0],
                        [0, 1, 1, 2],
                        [0, 0, 0, 0],
                        0,
                        2,
                        2,
                        snapshot.epoch_id,
                        snapshot.revision,
                    ),
                    (
                        snapshot.partial_group_id,
                        2,
                        [0, 1, 0, 0],
                        [0, 1, 0, 1],
                        [0, 0, 1, 1],
                        1,
                        1,
                        1,
                        snapshot.epoch_id,
                        snapshot.revision,
                    ),
                    (
                        snapshot.zero_hash_group_id,
                        1,
                        [0, 0],
                        [0, 0],
                        [0, 1],
                        1,
                        0,
                        0,
                        snapshot.epoch_id,
                        snapshot.revision,
                    ),
                )
            )
        )
        assert (
            tuple(
                connection.execute(
                    """SELECT group_version_id,requirement_count,mask_histogram,
                          neighbor_counts,deficiencies,maximum_deficiency,
                          matching_size,distinct_hash_count,installed_epoch_id,
                          installed_revision
                     FROM groundloop_m5_matching_hall_current
                    ORDER BY group_version_id COLLATE \"C\""""
                ).fetchall()
            )
            == expected_hall
        )

        assert tuple(
            connection.execute(
                """SELECT answer_version_id,status::text
                     FROM groundloop_answer_state_materialized
                    ORDER BY answer_version_id COLLATE \"C\""""
            ).fetchall()
        ) == (
            (snapshot.answer_id, "unsupported"),
            (snapshot.second_answer_id, "unsupported"),
        )
        assert tuple(
            connection.execute(
                """SELECT answer_version_id,status::text
                     FROM groundloop_m5_answer_state_materialized
                    ORDER BY answer_version_id COLLATE \"C\""""
            ).fetchall()
        ) == (
            (snapshot.answer_id, "valid"),
            (snapshot.second_answer_id, "unsupported"),
        )
        assert tuple(
            connection.execute(
                """SELECT claim_id,support_kind,direct_support_observation_id,
                          group_version_id,direct_refute_observation_id
                     FROM groundloop_m5_claim_certificate_artifact
                    ORDER BY claim_id COLLATE \"C\""""
            ).fetchall()
        ) == (
            (
                snapshot.claim_ids[0],
                "group",
                None,
                snapshot.complete_group_id,
                None,
            ),
            (
                snapshot.claim_ids[1],
                "direct",
                snapshot.direct_support_observations[0],
                None,
                None,
            ),
            (
                snapshot.claim_ids[2],
                "none",
                None,
                None,
                snapshot.refuted_only_observation,
            ),
            (
                snapshot.claim_ids[3],
                "direct",
                snapshot.conflicted_support_observation,
                None,
                snapshot.conflicted_refute_observation,
            ),
            (
                snapshot.claim_ids[4],
                "direct",
                snapshot.optional_support_observation,
                None,
                None,
            ),
            (snapshot.claim_ids[5], "none", None, None, None),
        )
        assert tuple(
            connection.execute(
                """SELECT claim_id,support_observation_id,refute_observation_id
                     FROM groundloop_claim_certificate
                    ORDER BY claim_id COLLATE \"C\""""
            ).fetchall()
        ) == (
            (snapshot.claim_ids[0], None, None),
            (snapshot.claim_ids[1], snapshot.direct_support_observations[0], None),
            (snapshot.claim_ids[2], None, snapshot.refuted_only_observation),
            (
                snapshot.claim_ids[3],
                snapshot.conflicted_support_observation,
                snapshot.conflicted_refute_observation,
            ),
            (snapshot.claim_ids[4], snapshot.optional_support_observation, None),
            (snapshot.claim_ids[5], None, None),
        )
        assert connection.execute(
            """SELECT selected_observation_id
                 FROM groundloop_m5_group_certificate_artifact_row row
                 JOIN groundloop_m5_published_group_certificate_binding binding
                   USING (certificate_digest)
                WHERE binding.group_version_id=%s AND row.requirement_ordinal=0""",
            (snapshot.complete_group_id,),
        ).fetchone() == (
            min(
                observation_id
                for observation_id, requirement_id, _, _, _ in (
                    snapshot.requirement_observations
                )
                if requirement_id == snapshot.complete_requirement_ids[0]
            ),
        )
        for relation in (
            "groundloop_m5_matching_image_working",
            "groundloop_m5_matching_observation_working",
            "groundloop_m5_matching_edge_working",
            "groundloop_m5_matching_hash_mask_working",
            "groundloop_m5_matching_hall_working",
            "groundloop_m5_matching_patch_artifact",
            "groundloop_m5_matching_work_contribution",
            "groundloop_m5_matching_work_accumulator",
            "groundloop_m5_runtime_epoch",
            "groundloop_m5_update",
        ):
            assert connection.execute(
                f"SELECT count(*) FROM {relation}"
            ).fetchone() == (0,)
        connection.commit()


@pytest.mark.parametrize(
    "mutation",
    (
        "epoch_structural",
        "epoch_semantic",
        "epoch_evaluation",
        "epoch_publication_mode",
        "epoch_revision",
        "activation_head",
        "m4_head",
        "m5_head",
        "overlapping_policy",
        "currency_future_close",
        "m4_claim_materialized",
        "m4_answer_materialized",
        "m4_claim_certificate",
        "m4_claim_published",
        "m4_answer_published",
        "m5_requirement_materialized",
        "m5_group_materialized",
        "m5_claim_materialized",
        "m5_answer_materialized",
        "m5_requirement_published",
        "m5_group_published",
        "m5_claim_published",
        "m5_answer_published",
        "m5_group_binding_missing",
        "m5_group_binding_extra",
        "m5_group_binding_wrong",
        "m5_claim_binding_missing",
        "m5_claim_binding_wrong",
        "m5_claim_binding_extra_catalog_corruption",
        "m5_group_artifact_header",
        "m5_group_artifact_row",
        "m5_claim_artifact",
        "currency_missing_current",
        "currency_current_extra",
        "currency_current_different",
        "typed_update_history",
        "runtime_epoch_history",
    ),
)
def test_b3a_preddl_mismatch_rolls_back_without_017_inventory(
    mutation: str,
) -> None:
    with _pre017_schema() as (connection, _):
        snapshot = _seed_b3_activated_snapshot(
            connection,
            f"b3a-{mutation}",
            inconsistent_activation_head=mutation == "activation_head",
        )
        if mutation != "activation_head":
            _apply_b3_preddl_mutation(connection, snapshot, mutation)
        inventory_before = _b3_install_catalog_inventory(connection)
        connection.commit()

        with pytest.raises(M5PersistedMatchingBundleError):
            install_m5_persisted_matching_bundle(connection)

        assert _b3_install_catalog_inventory(connection) == inventory_before
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        connection.commit()


@pytest.mark.parametrize(
    "failure_point",
    (
        "after_initial_ledger",
        "after_install_locks",
        "after_helpers",
        "after_image_schema",
        "after_observation_schema",
        "after_edge_mask_schema",
        "after_hall_schema",
        "after_artifact_schema",
        "after_authorization",
        "after_deferred_validation",
        "after_backfill_image",
        "after_backfill_observation",
        "after_backfill_edge",
        "after_backfill_mask",
        "after_backfill_hall",
        "after_backfill",
        "before_ledger",
        "after_ledger",
    ),
)
def test_b3a_postwrite_failure_rolls_back_exact_pre017_inventory(
    failure_point: str,
) -> None:
    with _pre017_schema() as (connection, _):
        _seed_b3_activated_snapshot(connection, f"b3a-{failure_point}")
        inventory_before = _b3_install_catalog_inventory(connection)
        connection.commit()

        def fail(point: str) -> None:
            if point == failure_point:
                raise RuntimeError(f"injected B3a failure at {point}")

        with pytest.raises(RuntimeError, match=failure_point):
            install_m5_persisted_matching_bundle(
                connection,
                failure_injector=fail,
            )

        assert _b3_install_catalog_inventory(connection) == inventory_before
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        connection.commit()


def test_b2_structural_empty_transition_commits() -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, "b2-positive")
        epoch, event, payload = _open_b2_runtime_epoch(
            connection, "b2-positive-open", base, policy
        )
        _apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=base,
            policy=policy,
        )
        connection.commit()
        assert connection.execute(
            "SELECT updated_revision FROM groundloop_m5_matching_image_working "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchone() == (1,)
        connection.commit()


@pytest.mark.parametrize("action", ("RETIRE", "REPLACE"))
def test_b2_structural_after_none_group_scope_transition_commits(action: str) -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        prefix = f"b2-after-none-{action.lower()}"
        base, policy = _seed_b2_runtime_base(connection, prefix)
        group, requirement, group_digest, requirement_digest = (
            _seed_b2_absent_transition_state(connection, prefix, base, policy)
        )
        epoch, event, payload = _open_b2_runtime_epoch(
            connection,
            f"{prefix}-open",
            base,
            policy,
            update_kind="retire_group" if action == "RETIRE" else "replace_group",
        )
        _stage_b2_group_deactivation(
            connection, epoch=epoch, event=event, group=group, action=action
        )
        _apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=base,
            policy=policy,
            absent_states=(
                ("group_state", group, group_digest),
                ("requirement_state", requirement, requirement_digest),
            ),
        )
        assert connection.execute(
            "SELECT (SELECT count(*) FROM groundloop_m5_working_requirement_state "
            "WHERE epoch_id=%s),(SELECT count(*) FROM "
            "groundloop_m5_working_group_state WHERE epoch_id=%s),"
            "(SELECT count(*) FROM groundloop_m5_matching_patch_artifact "
            "WHERE resulting_epoch_id=%s),"
            "(SELECT count(*) FROM groundloop_m5_matching_work_contribution "
            "WHERE epoch_id=%s),"
            "(SELECT count(*) FROM groundloop_m5_group_deactivation "
            "WHERE epoch_id=%s AND group_version_id=%s AND action=%s "
            "AND event_id=%s)",
            (epoch, epoch, epoch, epoch, epoch, group, action, event),
        ).fetchone() == (0, 0, 1, 1, 1)
        assert connection.execute(
            "SELECT validation_done FROM "
            "pg_temp.groundloop_m5_matching_transition_context"
        ).fetchone() == (True,)
        connection.commit()


@pytest.mark.parametrize(
    "update_kind",
    (
        "register_group",
        "observe_requirement",
        "policy_change",
        "document_insert",
        "document_delete",
        "document_replace",
    ),
)
def test_d26_replaced_validator_retains_all_six_present_reference_kinds(
    update_kind: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-present-six-{update_kind}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()

        sealed = _seal_d26_present_reference_validator_fixture(
            connection,
            prefix=prefix,
            snapshot=snapshot,
            update_kind=update_kind,
        )

        assert {kind for kind, _, _, _ in sealed.references} == {
            "requirement_state",
            "group_state",
            "group_certificate",
            "claim_state",
            "claim_certificate",
            "answer_state",
        }
        assert connection.execute(
            """SELECT kind,object_id,epoch_id,revision,state_artifact_hash,
                      reference_digest
                 FROM groundloop_m5_event_result_state_reference
                WHERE structural_event_id=%s
                ORDER BY reference_ordinal""",
            (sealed.event_id,),
        ).fetchall() == [
            (
                kind,
                object_id,
                sealed.epoch_id,
                sealed.revision,
                state_hash,
                reference_digest,
            )
            for kind, object_id, state_hash, reference_digest in sealed.references
        ]
        connection.commit()


@pytest.mark.parametrize(
    "kind",
    (
        "requirement_state",
        "group_state",
        "group_certificate",
        "claim_state",
        "claim_certificate",
        "answer_state",
    ),
)
def test_d26_replaced_validator_rejects_wrong_present_state_hash(kind: str) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-present-wrong-{kind}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5 changed-state reference does not match historical state",
        ):
            _seal_d26_present_reference_validator_fixture(
                connection,
                prefix=prefix,
                snapshot=snapshot,
                wrong_state_hash_kind=kind,
            )
        connection.rollback()


@pytest.mark.parametrize(
    "update_kind",
    (
        "register_group",
        "observe_requirement",
        "policy_change",
        "document_insert",
        "document_delete",
        "document_replace",
    ),
)
def test_d26_nonstructural_result_rejects_absence_at_child_validator(
    update_kind: str,
) -> None:
    with _pre017_schema() as (connection, _):
        prefix = f"d26-nonstructural-child-{update_kind}"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5-D26 absence reference has invalid seal coordinates",
        ):
            _seal_d26_present_reference_validator_fixture(
                connection,
                prefix=prefix,
                snapshot=snapshot,
                update_kind=update_kind,
                absence_hash_kind="requirement_state",
            )
        connection.rollback()


def test_d26_failed_result_rejects_absence_at_child_validator() -> None:
    with _pre017_schema() as (connection, _):
        prefix = "d26-failed-child"
        snapshot = _seed_b3_activated_snapshot(connection, prefix)
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        _register_d26_candidate_policy(connection, snapshot)
        connection.commit()

        with pytest.raises(
            psycopg.errors.RaiseException,
            match="M5-D26 absence reference has invalid seal coordinates",
        ):
            _fail_d26_absence_reference_validator_fixture(
                connection,
                prefix=prefix,
                snapshot=snapshot,
            )
        connection.rollback()


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            "claim_after_none",
            "absent logical state contract mismatch|logical state removal is forbidden",
        ),
        (
            "answer_after_none",
            "absent logical state contract mismatch|logical state removal is forbidden",
        ),
        ("wrong_source_kind", "outer patch point law mismatch|check constraint"),
        ("wrong_event", "lacks exact (structural )?deactivation"),
        ("wrong_epoch", "lacks exact (structural )?deactivation"),
        ("wrong_update_kind", "lacks exact (structural )?deactivation"),
        ("missing_deactivation", "lacks exact (structural )?deactivation"),
        ("wrong_deactivation", "lacks exact (structural )?deactivation"),
        (
            "wrong_before_hash",
            "predecessor mismatch|removed state before digest mismatch",
        ),
        (
            "effective_present",
            "lacks exact (structural )?deactivation|remains effective",
        ),
        (
            "forged_requirement_working",
            "wrote forbidden working row|removed state emitted a working mutation",
        ),
        (
            "forged_group_working",
            "wrote forbidden working row|removed state emitted a working mutation",
        ),
    ),
)
def test_b2_structural_after_none_falsifiers(mutation: str, message: str) -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        prefix = f"b2-after-none-{mutation}"
        base, policy = _seed_b2_runtime_base(connection, prefix)
        group, requirement, group_digest, requirement_digest = (
            _seed_b2_absent_transition_state(connection, prefix, base, policy)
        )
        epoch, event, payload = _open_b2_runtime_epoch(
            connection,
            prefix,
            base,
            policy,
            update_kind=(
                "policy_change" if mutation == "wrong_update_kind" else "retire_group"
            ),
        )
        _stage_b2_group_deactivation(
            connection,
            epoch=epoch,
            event=event,
            group=group,
            action="RETIRE",
            mutation=mutation,
        )
        absent_states = (
            ("group_state", group, group_digest),
            ("requirement_state", requirement, requirement_digest),
        )
        if mutation == "claim_after_none":
            claim = connection.execute(
                "SELECT claim_id FROM groundloop_claim"
            ).fetchone()
            assert claim is not None
            absent_states = (("claim_state", str(claim[0]), "f" * 64),)
        elif mutation == "answer_after_none":
            answer = connection.execute(
                "SELECT answer_version_id FROM groundloop_answer_version"
            ).fetchone()
            assert answer is not None
            absent_states = (("answer_state", str(answer[0]), "f" * 64),)
        elif mutation == "wrong_before_hash":
            absent_states = (
                ("group_state", group, "f" * 64),
                ("requirement_state", requirement, requirement_digest),
            )
        elif mutation == "effective_present":
            absent_states = (("requirement_state", requirement, requirement_digest),)
            connection.execute(
                """CREATE OR REPLACE VIEW groundloop_m5_effective_requirement_version AS
                   SELECT update_row.epoch_id,requirement.requirement_version_id,
                          requirement.group_version_id,requirement.ordinal,
                          requirement.requirement_text,
                          requirement.requirement_text_hash,family.claim_id,
                          false AS staged
                     FROM groundloop_m5_update update_row
                     JOIN groundloop_m5_group_validity validity
                       ON validity.valid_from_epoch<=
                          update_row.previous_published_epoch_id
                      AND (validity.valid_to_epoch IS NULL OR
                           update_row.previous_published_epoch_id<validity.valid_to_epoch)
                     JOIN groundloop_m5_group_version group_row
                       ON group_row.group_version_id=validity.group_version_id
                     JOIN groundloop_m5_group_family family
                       ON family.group_family_id=group_row.group_family_id
                     JOIN groundloop_m5_requirement_version requirement
                       ON requirement.group_version_id=group_row.group_version_id
                      AND requirement.lifecycle_state='PUBLISHED'"""
            )
        with pytest.raises(psycopg.Error, match=message):
            _apply_empty_b2_structural(
                connection,
                epoch=epoch,
                event=event,
                payload=payload,
                base=base,
                policy=policy,
                absent_states=absent_states,
                forge_absent_working=(
                    "requirement_state"
                    if mutation == "forged_requirement_working"
                    else "group_state"
                    if mutation == "forged_group_working"
                    else None
                ),
                encoded_source_kind=(
                    "requirement_completion"
                    if mutation == "wrong_source_kind"
                    else None
                ),
            )
        connection.rollback()


def test_b2_structural_nonempty_hall_transition_validates_and_commits() -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, "b2-hall")
        hall_group = _seed_b2_hall_group(connection, "b2-hall", base)
        epoch, event, payload = _open_b2_runtime_epoch(
            connection, "b2-hall-open", base, policy
        )
        _apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=base,
            policy=policy,
            hall_group=hall_group,
        )
        assert connection.execute(
            "SELECT validation_done FROM "
            "pg_temp.groundloop_m5_matching_transition_context"
        ).fetchone() == (True,)
        connection.commit()
        assert connection.execute(
            """SELECT requirement_count,mask_histogram,neighbor_counts,
                      deficiencies,maximum_deficiency,matching_size,
                      distinct_hash_count,updated_revision
                 FROM groundloop_m5_matching_hall_working
                WHERE epoch_id=%s AND group_version_id=%s""",
            (epoch, hall_group[0]),
        ).fetchone() == (1, [0, 0], [0, 0], [0, 1], 1, 0, 0, 1)
        connection.commit()


def test_b2_later_direct_transition_exact_update_commits() -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, "b2-later")
        epoch, event, payload = _open_b2_runtime_epoch(
            connection, "b2-later-open", base, policy, direct_bridge=True
        )
        _apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=base,
            policy=policy,
        )
        connection.commit()

        transition_id, transition_hash = _seed_b2_direct_transition_authority(
            connection, epoch, "b2-later"
        )
        _apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=transition_id,
            payload=transition_hash,
            base=base,
            policy=policy,
            source_kind="direct_transition",
        )
        connection.commit()
        assert connection.execute(
            """SELECT runtime.revision,image.updated_revision,acc.updated_revision
               FROM groundloop_m5_runtime_epoch runtime
               JOIN groundloop_m5_matching_image_working image USING(epoch_id)
               JOIN groundloop_m5_matching_work_accumulator acc USING(epoch_id)
               WHERE runtime.epoch_id=%s""",
            (epoch,),
        ).fetchone() == (2, 2, 2)
        connection.commit()


@pytest.mark.parametrize("mutation", ("base_mutation", "policy_mutation"))
def test_b2_later_image_header_is_immutable(mutation: str) -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, f"b2-later-{mutation}")
        epoch, event, payload = _open_b2_runtime_epoch(
            connection,
            f"b2-later-{mutation}-open",
            base,
            policy,
            direct_bridge=True,
        )
        _apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=base,
            policy=policy,
        )
        connection.commit()
        transition_id, transition_hash = _seed_b2_direct_transition_authority(
            connection, epoch, f"b2-later-{mutation}"
        )
        selected_policy = policy
        if mutation == "policy_mutation":
            selected_policy = f"{policy}-changed"
            connection.execute(
                """INSERT INTO groundloop_decision_policy
                   (policy_version,support_threshold,refute_threshold,
                    tie_rule_version,valid_from_epoch,valid_to_epoch)
                   VALUES (%s,.5,.5,'v1',%s,%s)""",
                (selected_policy, base, epoch),
            )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="later image OLD/update mismatch|working image journal mismatch",
        ):
            _apply_empty_b2_structural(
                connection,
                epoch=epoch,
                event=transition_id,
                payload=transition_hash,
                base=base,
                policy=selected_policy,
                source_kind="direct_transition",
                mutate=mutation,
            )
        connection.rollback()


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("clear_mode", "mode/context escape"),
        ("duplicate_image", "one INSERT"),
        ("accumulator", "work digest|accumulator"),
        ("output_bytes", "derivable work counter"),
        ("unconsumed", "certificate journal has an extra row"),
    ),
)
def test_b2_structural_transition_falsifiers(mutation: str, message: str) -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, f"b2-{mutation}")
        epoch, event, payload = _open_b2_runtime_epoch(
            connection, f"b2-{mutation}-open", base, policy
        )
        with pytest.raises(psycopg.Error, match=message):
            _apply_empty_b2_structural(
                connection,
                epoch=epoch,
                event=event,
                payload=payload,
                base=base,
                policy=policy,
                mutate=mutation,
            )
        connection.rollback()


def test_b2_rejects_forged_structural_source_authority() -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, "b2-forged")
        epoch, event, _ = _open_b2_runtime_epoch(
            connection, "b2-forged-open", base, policy
        )
        with pytest.raises(
            psycopg.errors.RaiseException, match="source authority mismatch"
        ):
            _apply_empty_b2_structural(
                connection,
                epoch=epoch,
                event=event,
                payload="f" * 64,
                base=base,
                policy=policy,
            )
        connection.rollback()


def test_b2_rejects_prior_transaction_structural_runtime_row() -> None:
    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = _seed_b2_runtime_base(connection, "b2-prior")
        epoch, event, payload = _open_b2_runtime_epoch(
            connection, "b2-prior-open", base, policy
        )
        connection.commit()
        with pytest.raises(
            psycopg.errors.RaiseException, match="structural runtime INSERT mismatch"
        ):
            _apply_empty_b2_structural(
                connection,
                epoch=epoch,
                event=event,
                payload=payload,
                base=base,
                policy=policy,
            )
        connection.rollback()


def test_b2_nonowner_forged_private_context_denied_public_path_succeeds() -> None:
    role = f"d25_runtime_{uuid.uuid4().hex}"
    password = uuid.uuid4().hex
    with _pre017_schema() as (owner, schema_name):
        assert install_m5_persisted_matching_bundle(owner).applied
        owner.commit()
        base, policy = _seed_b2_runtime_base(owner, "b2-role")
        owner.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)
            )
        )
        owner.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        owner.execute(
            sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role)
            )
        )
        private_signatures = (
            "groundloop_m5_matching_private_temp_triplet(regclass,regclass,regclass)",
            "groundloop_m5_matching_begin_transition_context(bigint,bigint,bigint,text,text)",
            "groundloop_m5_matching_journal_change(text,text,jsonb,jsonb)",
            "groundloop_m5_matching_capture_transition_anchor()",
            "groundloop_m5_matching_deferred_validate()",
        )
        public_signatures = (
            "groundloop_m5_authorize_checked_transition(bigint,bigint)",
            "groundloop_m5_authorize_persisted_matching_transition(bigint,bigint,bigint,text,text)",
        )
        for signature in private_signatures:
            qualified = f"{schema_name}.{signature}"
            assert owner.execute(
                "SELECT has_function_privilege(%s,%s,'EXECUTE')",
                (role, qualified),
            ).fetchone() == (False,)
        for signature in public_signatures:
            qualified = f"{schema_name}.{signature}"
            assert owner.execute(
                "SELECT has_function_privilege(%s,%s,'EXECUTE')",
                (role, qualified),
            ).fetchone() == (True,)
        owner.commit()
        try:
            with psycopg.connect(
                _database_url(), user=role, password=password
            ) as runtime:
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    runtime.execute(
                        "SELECT groundloop_m5_matching_begin_transition_context"
                        "(1,1,1,'structural_open','forbidden')"
                    )
                runtime.rollback()
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    runtime.execute(
                        "SELECT groundloop_m5_matching_journal_change"
                        "('relation','key','null'::jsonb,'null'::jsonb)"
                    )
                runtime.rollback()
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                forged_epoch, forged_event, _ = _open_b2_runtime_epoch(
                    runtime, "b2-role-forged", base, policy
                )
                runtime.execute(
                    """CREATE TEMP TABLE
                       pg_temp.groundloop_m5_matching_transition_context (
                         backend_pid integer NOT NULL,
                         transaction_id bigint NOT NULL,
                         session_role text NOT NULL,
                         epoch_id bigint NOT NULL,
                         expected_revision bigint NOT NULL,
                         resulting_revision bigint NOT NULL,
                         source_kind text NOT NULL,
                         source_id text NOT NULL,
                         anchor_count integer NOT NULL DEFAULT 0,
                         runtime_first_old jsonb,
                         runtime_final_new jsonb,
                         runtime_saw_insert boolean NOT NULL DEFAULT false,
                         runtime_saw_update boolean NOT NULL DEFAULT false,
                         validation_started boolean NOT NULL DEFAULT false,
                         validation_done boolean NOT NULL DEFAULT false,
                         PRIMARY KEY (backend_pid,transaction_id,session_role)
                       ) ON COMMIT DROP"""
                )
                runtime.execute(
                    """CREATE TEMP TABLE
                       pg_temp.groundloop_m5_matching_change_journal (
                         relation_name text NOT NULL,
                         key_preimage bytea NOT NULL,
                         first_old jsonb,
                         final_new jsonb,
                         first_operation text NOT NULL,
                         last_operation text NOT NULL,
                         mutation_count integer NOT NULL,
                         saw_insert boolean NOT NULL,
                         saw_update boolean NOT NULL,
                         saw_delete boolean NOT NULL,
                         PRIMARY KEY(relation_name,key_preimage)
                       ) ON COMMIT DROP"""
                )
                runtime.execute(
                    """CREATE TEMP TABLE
                       pg_temp.groundloop_m5_matching_expected_changes (
                         relation_name text NOT NULL,
                         key_preimage bytea NOT NULL,
                         PRIMARY KEY(relation_name,key_preimage)
                       ) ON COMMIT DROP"""
                )
                runtime.execute(
                    """INSERT INTO pg_temp.groundloop_m5_matching_transition_context (
                         backend_pid,transaction_id,session_role,epoch_id,
                         expected_revision,resulting_revision,source_kind,source_id,
                         validation_done)
                       VALUES (pg_backend_pid(),pg_current_xact_id()::text::bigint,
                         session_user,%s,1,1,'structural_open',%s,true)""",
                    (forged_epoch, forged_event),
                )
                assert runtime.execute(
                    """SELECT current_setting(
                                 'groundloop.m5_matching_context_oid',true),
                               current_setting(
                                 'groundloop.m5_matching_journal_oid',true),
                               current_setting(
                                 'groundloop.m5_matching_expected_oid',true)"""
                ).fetchone() == (None, None, None)
                for setting, relation in (
                    (
                        "groundloop.m5_matching_context_oid",
                        "pg_temp.groundloop_m5_matching_transition_context",
                    ),
                    (
                        "groundloop.m5_matching_journal_oid",
                        "pg_temp.groundloop_m5_matching_change_journal",
                    ),
                    (
                        "groundloop.m5_matching_expected_oid",
                        "pg_temp.groundloop_m5_matching_expected_changes",
                    ),
                ):
                    runtime.execute(
                        "SELECT set_config(%s,to_regclass(%s)::oid::text,true)",
                        (setting, relation),
                    )
                for setting, value in (
                    ("groundloop.m5_checked_transition", "on"),
                    ("groundloop.m5_matching_mode", "transition"),
                    ("groundloop.m5_matching_epoch_id", str(forged_epoch)),
                    ("groundloop.m5_matching_expected_revision", "1"),
                    ("groundloop.m5_matching_resulting_revision", "1"),
                    ("groundloop.m5_matching_source_kind", "structural_open"),
                    ("groundloop.m5_matching_source_id", forged_event),
                ):
                    runtime.execute("SELECT set_config(%s,%s,true)", (setting, value))
                with pytest.raises(
                    psycopg.errors.RaiseException,
                    match="transition private context was replaced",
                ):
                    runtime.execute(
                        "INSERT INTO groundloop_m5_matching_image_working "
                        "VALUES (%s,%s,0,%s,1)",
                        (forged_epoch, base, policy),
                    )
                    runtime.execute("SET CONSTRAINTS ALL IMMEDIATE")
                runtime.rollback()
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                assert runtime.execute(
                    "SELECT count(*) FROM groundloop_epoch WHERE event_id=%s",
                    (forged_event,),
                ).fetchone() == (0,)
                runtime.commit()
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                promotion_before = runtime.execute(
                    """SELECT xmin::text,decision_policy_version,
                              installed_epoch_id,installed_revision
                         FROM groundloop_m5_matching_image_current
                        WHERE singleton"""
                ).fetchone()
                assert promotion_before == (
                    promotion_before[0],
                    policy,
                    base,
                    0,
                )
                runtime.execute(
                    """CREATE TEMP TABLE
                       pg_temp.groundloop_m5_matching_promotion_context (
                         backend_pid integer NOT NULL,
                         transaction_id bigint NOT NULL,
                         session_role text NOT NULL,
                         mode text NOT NULL,
                         epoch_id bigint NOT NULL,
                         expected_revision bigint NOT NULL,
                         resulting_revision bigint NOT NULL,
                         policy_version text NOT NULL,
                         anchor_m4_epoch_id bigint NOT NULL,
                         anchor_m5_epoch_id bigint,
                         anchor_m5_revision bigint,
                         anchor_activation_count integer NOT NULL,
                         anchor_predecessor_revision bigint NOT NULL,
                         anchor_predecessor_sealed_at timestamptz NOT NULL,
                         anchor_current_policy text,
                         validation_started boolean NOT NULL DEFAULT false,
                         validation_done boolean NOT NULL DEFAULT false,
                         PRIMARY KEY(backend_pid,transaction_id,session_role)
                       ) ON COMMIT DROP"""
                )
                runtime.execute(
                    """CREATE TEMP TABLE
                       pg_temp.groundloop_m5_matching_promotion_journal (
                         relation_name text NOT NULL,
                         key_preimage bytea NOT NULL,
                         first_old jsonb,
                         final_new jsonb,
                         first_operation text NOT NULL,
                         last_operation text NOT NULL,
                         mutation_count integer NOT NULL,
                         saw_insert boolean NOT NULL,
                         saw_update boolean NOT NULL,
                         saw_delete boolean NOT NULL,
                         PRIMARY KEY(relation_name,key_preimage)
                       ) ON COMMIT DROP"""
                )
                runtime.execute(
                    """CREATE TEMP TABLE
                       pg_temp.groundloop_m5_matching_promotion_expected (
                         relation_name text NOT NULL,
                         key_preimage bytea NOT NULL,
                         PRIMARY KEY(relation_name,key_preimage)
                       ) ON COMMIT DROP"""
                )
                runtime.execute(
                    """INSERT INTO pg_temp.groundloop_m5_matching_promotion_context
                       VALUES (pg_backend_pid(),
                         pg_current_xact_id()::text::bigint,session_user,
                         'seal',%s,0,0,%s,%s,%s,0,1,0,now(),%s,false,true)""",
                    (base, policy, base, base, policy),
                )
                for setting, relation in (
                    (
                        "groundloop.m5_matching_context_oid",
                        "pg_temp.groundloop_m5_matching_promotion_context",
                    ),
                    (
                        "groundloop.m5_matching_journal_oid",
                        "pg_temp.groundloop_m5_matching_promotion_journal",
                    ),
                    (
                        "groundloop.m5_matching_expected_oid",
                        "pg_temp.groundloop_m5_matching_promotion_expected",
                    ),
                ):
                    runtime.execute(
                        "SELECT set_config(%s,to_regclass(%s)::oid::text,true)",
                        (setting, relation),
                    )
                for setting, value in (
                    ("groundloop.m5_checked_transition", "on"),
                    ("groundloop.m5_matching_mode", "seal"),
                    ("groundloop.m5_matching_epoch_id", str(base)),
                    ("groundloop.m5_matching_expected_revision", "0"),
                    ("groundloop.m5_matching_resulting_revision", "0"),
                    ("groundloop.m5_matching_policy", policy),
                ):
                    runtime.execute("SELECT set_config(%s,%s,true)", (setting, value))
                with pytest.raises(
                    psycopg.errors.RaiseException,
                    match="promotion context mismatch",
                ):
                    runtime.execute(
                        "UPDATE groundloop_m5_matching_image_current "
                        "SET decision_policy_version=decision_policy_version "
                        "WHERE singleton"
                    )
                    runtime.execute("SET CONSTRAINTS ALL IMMEDIATE")
                runtime.rollback()
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                assert (
                    runtime.execute(
                        """SELECT xmin::text,decision_policy_version,
                              installed_epoch_id,installed_revision
                         FROM groundloop_m5_matching_image_current
                        WHERE singleton"""
                    ).fetchone()
                    == promotion_before
                )
                runtime.commit()
                runtime.execute(
                    sql.SQL("SET search_path TO {}, public, pg_catalog").format(
                        sql.Identifier(schema_name)
                    )
                )
                epoch, event, payload = _open_b2_runtime_epoch(
                    runtime, "b2-role-open", base, policy
                )
                _apply_empty_b2_structural(
                    runtime,
                    epoch=epoch,
                    event=event,
                    payload=payload,
                    base=base,
                    policy=policy,
                )
                runtime.commit()
        finally:
            owner.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
            owner.commit()


@pytest.mark.parametrize(
    ("semantic_status", "evaluation_state"),
    (("pending", "failed"), ("complete", "degraded")),
)
def test_activation_rejects_semantically_live_epoch_regardless_of_evaluation_state(
    semantic_status: str, evaluation_state: str
) -> None:
    from m5.postgres.helpers import seed_base

    with _pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base = seed_base(
            connection,
            prefix=f"activation-live-{semantic_status}-{evaluation_state}",
            epoch_revision=0,
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()
        connection.execute(
            """INSERT INTO groundloop_epoch (
                 event_id,payload_hash,revision,structural_status,semantic_status,
                 evaluation_state,publication_mode,sealed_at)
               VALUES (%s,%s,0,'committed',%s,%s,'provisional',NULL)""",
            (
                f"activation-live-{semantic_status}-{evaluation_state}-event",
                hashlib.sha256(
                    f"activation-live-{semantic_status}-{evaluation_state}".encode()
                ).hexdigest(),
                semantic_status,
                evaluation_state,
            ),
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="invalid persisted matching activation authorization",
        ):
            connection.execute(
                "SELECT groundloop_m5_authorize_persisted_matching_activation(%s,0,%s)",
                (base.epoch_id, base.policy_version),
            )
        connection.rollback()
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_activation"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_matching_image_current"
        ).fetchone() == (0,)
        connection.commit()


def test_arbitrary_first_install_bytes_reject_before_ddl() -> None:
    with _pre017_schema() as (connection, _):
        before = _b3_install_catalog_inventory(connection)
        connection.commit()
        with pytest.raises(
            M5PersistedMatchingBundleError, match="frozen M5-D26 authority"
        ):
            install_m5_persisted_matching_bundle(
                connection,
                migration_bytes=b"-- self-consistent but unauthorized 017 bytes\n",
            )
        assert _b3_install_catalog_inventory(connection) == before
        connection.commit()


def test_two_same_byte_installers_fail_fast_then_replay_exactly() -> None:
    with _pre017_schema() as (connection, schema_name):
        first = _start_paused_installer(
            schema_name,
            pause_at="after_install_locks",
        )
        try:
            assert first.reached_pause.wait(timeout=30)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_persisted_matching_bundle(connection)
        finally:
            _resume_and_join_installer(first)

        assert first.errors == []
        assert first.applied_results == [True]
        assert first.points[:2] == ["after_initial_ledger", "after_install_locks"]
        assert first.points[-1] == "after_ledger"

        replay = install_m5_persisted_matching_bundle(connection)
        assert not replay.applied
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
            (M5_PERSISTED_MATCHING_BUNDLE_ID,),
        ).fetchone() == (1,)
        connection.commit()


def test_two_conflicting_installers_detect_post_lock_ledger_conflict_without_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alternate_bytes = (
        M5_PERSISTED_MATCHING_MIGRATION_PATH.read_bytes()
        + b"\n-- independently accepted concurrent migration-017 bytes\n"
    )
    alternate_identity = m5_persisted_matching_bundle_identity(
        migration_bytes=alternate_bytes
    )
    assert (
        alternate_identity.migration_sha256
        != M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256
    )
    assert (
        alternate_identity.bundle_sha256 != M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256
    )

    with _pre017_schema() as (connection, schema_name):
        current = _start_paused_installer(
            schema_name,
            pause_at="after_initial_ledger",
        )
        try:
            assert current.reached_pause.wait(timeout=30)
            # Model a concurrently running binary whose different bytes are its
            # own frozen authority.  The scoped constants are restored before
            # the current-byte installer resumes and performs its post-lock read.
            with monkeypatch.context() as alternate_authority:
                alternate_authority.setattr(
                    postgres_migrations,
                    "M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256",
                    alternate_identity.migration_sha256,
                )
                alternate_authority.setattr(
                    postgres_migrations,
                    "M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256",
                    alternate_identity.bundle_sha256,
                )
                alternate_result = install_m5_persisted_matching_bundle(
                    connection,
                    migration_bytes=alternate_bytes,
                )
                assert alternate_result.applied
            assert (
                postgres_migrations.M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256
                == M5_ACCEPTED_PERSISTED_MATCHING_MIGRATION_SHA256
            )
            assert (
                postgres_migrations.M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256
                == M5_ACCEPTED_PERSISTED_MATCHING_BUNDLE_SHA256
            )

            ledger = connection.execute(
                """SELECT bundle_sha256,migration_sha256,oracle_sha256,
                          prerequisite_sha256
                     FROM groundloop_m5_schema_bundle WHERE bundle_id=%s""",
                (M5_PERSISTED_MATCHING_BUNDLE_ID,),
            ).fetchone()
            assert ledger == (
                alternate_identity.bundle_sha256,
                alternate_identity.migration_sha256,
                alternate_identity.oracle_sha256,
                alternate_identity.prerequisite_sha256,
            )
            catalog_after_alternate = _b3_install_catalog_inventory(connection)
            rows_after_alternate = _all_table_xmin_count_snapshot(connection)
            connection.commit()
        finally:
            _resume_and_join_installer(current)

        assert current.applied_results == []
        assert len(current.errors) == 1
        assert isinstance(current.errors[0], M5BundleHashConflictError)
        assert current.points == ["after_initial_ledger", "after_install_locks"]
        assert _b3_install_catalog_inventory(connection) == catalog_after_alternate
        assert _all_table_xmin_count_snapshot(connection) == rows_after_alternate
        connection.commit()


def test_commit_between_same_byte_installer_ledger_reads_is_exact_noop_no_ddl() -> None:
    with _pre017_schema() as (connection, schema_name):
        late = _start_paused_installer(
            schema_name,
            pause_at="after_initial_ledger",
        )
        try:
            assert late.reached_pause.wait(timeout=30)
            winner = install_m5_persisted_matching_bundle(connection)
            assert winner.applied
            catalog_after_winner = _b3_install_catalog_inventory(connection)
            rows_after_winner = _all_table_xmin_count_snapshot(connection)
            connection.commit()
        finally:
            _resume_and_join_installer(late)

        assert late.errors == []
        assert late.applied_results == [False]
        assert late.points == ["after_initial_ledger", "after_install_locks"]
        assert _b3_install_catalog_inventory(connection) == catalog_after_winner
        assert _all_table_xmin_count_snapshot(connection) == rows_after_winner
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
            (M5_PERSISTED_MATCHING_BUNDLE_ID,),
        ).fetchone() == (1,)
        connection.commit()


@dataclass(frozen=True, slots=True)
class _PublicInstallerRaceContext:
    base: Any
    legacy_candidate_policy_id: str
    legacy_registry_snapshot_id: str
    activation_request: Any
    typed_plan: Any
    operational_config: Any


def _seed_public_installer_race_context(
    connection: Connection[Any],
    prefix: str,
) -> _PublicInstallerRaceContext:
    from groundloop.m4.persistence import PostgresM4RuntimeStore
    from groundloop.m5.events import RetireGroupEvent, m5_event_payload_digest
    from groundloop.m5.runtime.contracts import (
        ActiveChunkSnapshot,
        ActiveChunkSnapshotEntry,
        M5RuntimeOperationalConfig,
        M5TypedEventPlan,
        RequirementRegistrySnapshot,
    )
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres.helpers import (
        insert_published_group,
        make_group,
        seed_base,
    )
    from tests.m5.postgres_runtime.test_runtime_races import (
        _candidate_manifest,
        _legacy_candidate_manifest,
    )

    chunk_texts = ("alpha", "beta")
    base = seed_base(
        connection,
        prefix=prefix,
        claim_count=1,
        chunk_texts=chunk_texts,
    )
    group = make_group(
        group_id=f"{prefix}-group-v1",
        family_id=f"{prefix}-family",
        claim_id=base.claim_ids[0],
        texts=("required installer-race fact",),
        source_id=f"{prefix}-fixture",
    )
    insert_published_group(connection, group=group, epoch_id=base.epoch_id)
    _seed_b3_direct_m4_snapshot(connection, epoch_id=base.epoch_id, revision=0)
    connection.execute(
        """INSERT INTO groundloop_model_artifact (
             model_artifact_id,task,provider,model_id,immutable_revision,
             tokenizer_revision,license_id,config_hash)
           VALUES ('runtime-race-embedding','embedding','fixture',
                   'runtime-race-embedding','v1','v1','MIT',%s)""",
        (hashlib.sha256(f"{prefix}:embedding".encode()).hexdigest(),),
    )
    connection.commit()

    legacy_manifest = _legacy_candidate_manifest(base)
    legacy_store = PostgresM4RuntimeStore(connection)
    legacy_store.register_claim_registry_snapshot(
        legacy_manifest.claim_registry_snapshot_id,
        base.claim_ids,
    )
    legacy_store.register_candidate_policy(legacy_manifest)
    manifest = _candidate_manifest(base)
    runtime_store = PostgresM5RuntimeStore(connection)
    runtime_store.register_candidate_policy(manifest)
    activation_request = runtime_store.prepare_activation_request(
        f"{prefix}-activation"
    )
    retire_event = RetireGroupEvent(
        event_id=f"{prefix}-typed-retire",
        group_version_id=group.group_version_id,
    )
    typed_plan = M5TypedEventPlan(
        structural_event_id=retire_event.event_id,
        event=retire_event,
        payload_hash=m5_event_payload_digest(retire_event),
        direct_plan=None,
        candidate_policy_id=manifest.candidate_policy_id,
        candidate_policy_manifest_hash=manifest.manifest_hash,
        requirement_registry_snapshot=RequirementRegistrySnapshot.build(()),
        active_chunk_snapshot=ActiveChunkSnapshot.build(
            tuple(
                ActiveChunkSnapshotEntry.build(
                    chunk_version_id=chunk_id,
                    chunk_text=chunk_text,
                )
                for chunk_id, chunk_text in zip(
                    base.chunk_ids,
                    chunk_texts,
                    strict=True,
                )
            )
        ),
        expected_previous_published_epoch_id=base.epoch_id,
    )
    connection.commit()
    return _PublicInstallerRaceContext(
        base=base,
        legacy_candidate_policy_id=legacy_manifest.policy_id,
        legacy_registry_snapshot_id=legacy_manifest.claim_registry_snapshot_id,
        activation_request=activation_request,
        typed_plan=typed_plan,
        operational_config=M5RuntimeOperationalConfig.build(300),
    )


def _run_public_installer_race_operation(
    connection: Connection[Any],
    context: _PublicInstallerRaceContext,
    surface: str,
    *,
    failure_injector: Callable[[str], None] | None = None,
) -> Any:
    if surface == "public_v1_open":
        from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
        from groundloop.m4.persistence import PostgresM4RuntimeStore

        event_id = f"{context.base.policy_version}-v1-open"
        return PostgresM4RuntimeStore(
            connection,
            audit_transitions=False,
        ).open_epoch(
            CorpusUpdateIdentity(
                event_id=event_id,
                payload_hash=hashlib.sha256(f"payload:{event_id}".encode()).hexdigest(),
                update_kind=UpdateKind.INSERT,
                previous_published_epoch_id=context.base.epoch_id,
                candidate_policy_id=context.legacy_candidate_policy_id,
            ),
            (),
            registry_snapshot_id=context.legacy_registry_snapshot_id,
            structural_action=lambda _cursor, _epoch_id: None,
            failure_injector=failure_injector,
        )

    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

    store = PostgresM5RuntimeStore(connection)
    if surface == "activation":
        return store.activate(
            context.activation_request,
            failure_injector=failure_injector,
        )
    return store.open_typed_event_atomically(
        context.typed_plan,
        recovery_operational_config=context.operational_config,
        recovery_root_fallback_required={},
        failure_injector=failure_injector,
    )


@dataclass
class _PausedPublicOperation:
    thread: threading.Thread
    reached_pause: threading.Event
    resume: threading.Event
    points: list[str]
    results: list[Any]
    errors: list[BaseException]


def _start_paused_public_operation(
    schema_name: str,
    context: _PublicInstallerRaceContext,
    surface: str,
    *,
    pause_at: str,
) -> _PausedPublicOperation:
    reached_pause = threading.Event()
    resume = threading.Event()
    points: list[str] = []
    results: list[Any] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            with psycopg.connect(_database_url()) as writer:
                writer.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(schema_name)
                    )
                )
                writer.commit()

                def pause(point: str) -> None:
                    points.append(point)
                    if point == pause_at:
                        reached_pause.set()
                        if not resume.wait(timeout=30):
                            raise TimeoutError(
                                f"public operation was not resumed after {pause_at}"
                            )

                results.append(
                    _run_public_installer_race_operation(
                        writer,
                        context,
                        surface,
                        failure_injector=pause,
                    )
                )
        except BaseException as error:  # retained for assertion in the test thread
            errors.append(error)

    thread = threading.Thread(
        target=run,
        name=f"migration-017-public-{surface}",
        daemon=True,
    )
    worker = _PausedPublicOperation(
        thread=thread,
        reached_pause=reached_pause,
        resume=resume,
        points=points,
        results=results,
        errors=errors,
    )
    thread.start()
    return worker


def _resume_and_join_public_operation(worker: _PausedPublicOperation) -> None:
    worker.resume.set()
    worker.thread.join(timeout=60)
    assert not worker.thread.is_alive(), "public operation did not terminate"


@pytest.mark.parametrize(
    ("surface", "pause_at"),
    (
        ("public_v1_open", "open_structural_written"),
        ("activation", "activation_after_constraints"),
        ("typed_fresh", "typed_open_before_commit"),
    ),
)
def test_public_writer_wins_then_installer_retries_from_committed_state(
    surface: str,
    pause_at: str,
) -> None:
    prefix = f"d25-public-writer-first-{surface}"
    with _pre017_schema() as (connection, schema_name):
        context = _seed_public_installer_race_context(connection, prefix)
        if surface == "typed_fresh":
            activation = _run_public_installer_race_operation(
                connection,
                context,
                "activation",
            )
            assert not activation.replayed

        writer = _start_paused_public_operation(
            schema_name,
            context,
            surface,
            pause_at=pause_at,
        )
        try:
            assert writer.reached_pause.wait(timeout=30)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_persisted_matching_bundle(connection)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_matching_image_current')"
            ).fetchone() == (None,)
            assert connection.execute(
                "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
                (M5_PERSISTED_MATCHING_BUNDLE_ID,),
            ).fetchone() == (0,)
            connection.commit()
        finally:
            _resume_and_join_public_operation(writer)

        assert writer.errors == []
        assert len(writer.results) == 1
        if surface == "typed_fresh":
            assert not writer.results[0].replayed
            with pytest.raises(
                M5PersistedMatchingBundleError,
                match="forbids pre-D25 typed runtime history",
            ):
                install_m5_persisted_matching_bundle(connection)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_matching_image_current')"
            ).fetchone() == (None,)
            connection.commit()
        else:
            assert install_m5_persisted_matching_bundle(connection).applied
            connection.commit()
            assert (
                connection.execute(
                    "SELECT to_regclass('groundloop_m5_matching_image_current')"
                ).fetchone()[0]
                is not None
            )
            if surface == "activation":
                assert connection.execute(
                    "SELECT mode FROM groundloop_runtime_mode WHERE singleton"
                ).fetchone() == ("m5_active",)
                assert connection.execute(
                    "SELECT count(*) FROM groundloop_m5_matching_image_current"
                ).fetchone() == (1,)
            connection.commit()


def test_public_typed_resume_wins_then_installer_rejects_committed_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _pre017_schema() as (connection, schema_name):
        context = _seed_public_installer_race_context(
            connection,
            "d25-public-writer-first-typed-resume",
        )
        activation = _run_public_installer_race_operation(
            connection,
            context,
            "activation",
        )
        assert not activation.replayed
        opened = _run_public_installer_race_operation(
            connection,
            context,
            "typed_resume",
        )
        assert not opened.replayed

        from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore

        original_read = PostgresM5RuntimeStore._read_existing_open
        replay_read = threading.Event()
        release_replay = threading.Event()
        replay_results: list[Any] = []
        replay_errors: list[BaseException] = []

        def pause_existing_read(
            store: PostgresM5RuntimeStore,
            cursor: Any,
            plan: Any,
            *,
            for_update: bool,
        ) -> Any:
            result = original_read(store, cursor, plan, for_update=for_update)
            if result is not None and not for_update and not replay_read.is_set():
                replay_read.set()
                if not release_replay.wait(timeout=30):
                    raise TimeoutError("typed replay read was not released")
            return result

        monkeypatch.setattr(
            PostgresM5RuntimeStore,
            "_read_existing_open",
            pause_existing_read,
        )

        def replay() -> None:
            try:
                with psycopg.connect(_database_url()) as replay_connection:
                    replay_connection.execute(
                        sql.SQL("SET search_path TO {}, public").format(
                            sql.Identifier(schema_name)
                        )
                    )
                    replay_connection.commit()
                    replay_results.append(
                        _run_public_installer_race_operation(
                            replay_connection,
                            context,
                            "typed_resume",
                        )
                    )
            except BaseException as error:
                replay_errors.append(error)

        replay_thread = threading.Thread(
            target=replay,
            name="migration-017-public-typed-replay",
            daemon=True,
        )
        replay_thread.start()
        try:
            assert replay_read.wait(timeout=30)
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_persisted_matching_bundle(connection)
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_matching_image_current')"
            ).fetchone() == (None,)
            connection.commit()
        finally:
            release_replay.set()
            replay_thread.join(timeout=60)
        assert not replay_thread.is_alive()
        assert replay_errors == []
        assert len(replay_results) == 1
        resumed = replay_results[0]
        assert resumed.replayed
        assert resumed.epoch_id == opened.epoch_id
        with pytest.raises(
            M5PersistedMatchingBundleError,
            match="forbids pre-D25 typed runtime history",
        ):
            install_m5_persisted_matching_bundle(connection)
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
            (M5_PERSISTED_MATCHING_BUNDLE_ID,),
        ).fetchone() == (0,)
        connection.commit()


@pytest.mark.parametrize(
    "surface",
    ("public_v1_open", "activation", "typed_fresh"),
)
def test_installer_locks_first_and_public_writer_loses_without_consumption(
    surface: str,
) -> None:
    prefix = f"d25-installer-first-{surface}"
    with _pre017_schema() as (connection, schema_name):
        context = _seed_public_installer_race_context(connection, prefix)
        if surface == "typed_fresh":
            activation = _run_public_installer_race_operation(
                connection,
                context,
                "activation",
            )
            assert not activation.replayed

        installer = _start_paused_installer(
            schema_name,
            pause_at="after_install_locks",
        )
        try:
            assert installer.reached_pause.wait(timeout=30)
            connection.execute("SET lock_timeout = '750ms'")
            connection.commit()
            epoch_sequence_before = connection.execute(
                "SELECT last_value,is_called FROM groundloop_epoch_epoch_id_seq"
            ).fetchone()
            assert epoch_sequence_before is not None
            connection.commit()
            with pytest.raises(psycopg.errors.LockNotAvailable):
                _run_public_installer_race_operation(
                    connection,
                    context,
                    surface,
                )
            connection.rollback()
            assert (
                connection.execute(
                    "SELECT last_value,is_called FROM groundloop_epoch_epoch_id_seq"
                ).fetchone()
                == epoch_sequence_before
            )
            connection.commit()
            assert connection.execute(
                "SELECT to_regclass('groundloop_m5_matching_image_current')"
            ).fetchone() == (None,)
            connection.commit()
        finally:
            _resume_and_join_installer(installer)

        assert installer.errors == []
        assert installer.applied_results == [True]
        connection.execute("SET lock_timeout = 0")
        connection.commit()
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
            (M5_PERSISTED_MATCHING_BUNDLE_ID,),
        ).fetchone() == (1,)
        if surface == "public_v1_open":
            opened = _run_public_installer_race_operation(
                connection,
                context,
                surface,
            )
            assert not opened.replayed
            event_id = f"{context.base.policy_version}-v1-open"
            assert connection.execute(
                "SELECT count(*) FROM groundloop_epoch WHERE event_id=%s",
                (event_id,),
            ).fetchone() == (1,)
        elif surface == "activation":
            # Lane B proves the schema barrier and zero consumption.  The
            # migration-017-aware public activation composition is a later lane.
            assert connection.execute(
                "SELECT count(*) FROM groundloop_m5_activation"
            ).fetchone() == (0,)
        else:
            # Lane B likewise owns only the installer/open barrier here, not
            # the later migration-017-aware typed-store transition adapter.
            assert connection.execute(
                "SELECT count(*) FROM groundloop_epoch WHERE event_id=%s",
                (context.typed_plan.structural_event_id,),
            ).fetchone() == (0,)
        connection.commit()


def test_installer_locks_first_and_public_typed_resume_loses_then_guard_rejects() -> (
    None
):
    with _pre017_schema() as (connection, schema_name):
        context = _seed_public_installer_race_context(
            connection,
            "d25-installer-first-typed-resume",
        )
        activation = _run_public_installer_race_operation(
            connection,
            context,
            "activation",
        )
        assert not activation.replayed
        opened = _run_public_installer_race_operation(
            connection,
            context,
            "typed_resume",
        )
        assert not opened.replayed
        before_sequence = connection.execute(
            "SELECT last_value,is_called FROM groundloop_epoch_epoch_id_seq"
        ).fetchone()
        assert before_sequence is not None
        connection.commit()

        installer = _start_paused_installer(
            schema_name,
            pause_at="after_install_locks",
        )
        try:
            assert installer.reached_pause.wait(timeout=30)
            connection.execute("SET lock_timeout = '750ms'")
            connection.commit()
            with pytest.raises(psycopg.errors.LockNotAvailable):
                _run_public_installer_race_operation(
                    connection,
                    context,
                    "typed_resume",
                )
            connection.rollback()
            assert (
                connection.execute(
                    "SELECT last_value,is_called FROM groundloop_epoch_epoch_id_seq"
                ).fetchone()
                == before_sequence
            )
            connection.commit()
        finally:
            _resume_and_join_installer(installer)

        assert installer.applied_results == []
        assert len(installer.errors) == 1
        assert isinstance(installer.errors[0], M5PersistedMatchingBundleError)
        assert "forbids pre-D25 typed runtime history" in str(installer.errors[0])
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_schema_bundle WHERE bundle_id=%s",
            (M5_PERSISTED_MATCHING_BUNDLE_ID,),
        ).fetchone() == (0,)
        connection.execute("SET lock_timeout = 0")
        connection.commit()
        resumed = _run_public_installer_race_operation(
            connection,
            context,
            "typed_resume",
        )
        assert resumed.replayed
        assert resumed.epoch_id == opened.epoch_id


def test_same_id_conflict_is_ledger_first(installed: Connection[Any]) -> None:
    with pytest.raises(M5BundleHashConflictError):
        install_m5_persisted_matching_bundle(
            installed, migration_bytes=b"-- deliberately conflicting 017 bytes\n"
        )


def test_rejects_ambient_transaction_before_ledger(
    installed: Connection[Any],
) -> None:
    installed.execute("SELECT 1")
    with pytest.raises(M5PersistedMatchingBundleError, match="idle connection"):
        install_m5_persisted_matching_bundle(installed)
    installed.rollback()


@pytest.mark.parametrize(
    ("setting", "value"),
    (
        ("isolation_level", IsolationLevel.REPEATABLE_READ),
        ("isolation_level", IsolationLevel.SERIALIZABLE),
        ("read_only", True),
    ),
)
def test_rejects_non_read_committed_or_read_only_before_ledger(
    installed: Connection[Any], setting: str, value: object
) -> None:
    setattr(installed, setting, value)
    try:
        with pytest.raises(
            M5PersistedMatchingBundleError,
            match="read-write READ COMMITTED",
        ):
            install_m5_persisted_matching_bundle(installed)
    finally:
        setattr(installed, setting, None)


def test_missing_accepted_016_rejects_before_any_017_ddl() -> None:
    with _pre017_schema(install_recovery=False) as (connection, _):
        with pytest.raises(M5PrerequisiteError, match="migration-016"):
            install_m5_persisted_matching_bundle(connection)
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        connection.rollback()


def test_all_d25_relations_have_three_operation_guard(
    installed: Connection[Any],
) -> None:
    rows = installed.execute(
        """SELECT c.relname, t.tgtype FROM pg_trigger t
           JOIN pg_class c ON c.oid=t.tgrelid
           JOIN pg_proc p ON p.oid=t.tgfoid
           WHERE NOT t.tgisinternal AND c.relname=ANY(%s)
             AND p.proname='groundloop_m5_matching_authorized_guard'""",
        (list(D25_RELATIONS),),
    ).fetchall()
    assert {str(row[0]) for row in rows} == set(D25_RELATIONS)
    assert all(int(row[1]) & 4 and int(row[1]) & 8 and int(row[1]) & 16 for row in rows)
    installed.commit()


def test_raw_dml_is_rejected(installed: Connection[Any]) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="unauthorized"):
        installed.execute(
            """INSERT INTO groundloop_m5_matching_image_current
               VALUES (true,'missing',1,0)"""
        )
    installed.rollback()


def test_lane_a_same_byte_empty_vectors_and_canonical_decoders(
    installed: Connection[Any],
) -> None:
    assert installed.execute(
        "SELECT groundloop_m5_matching_digest_text_fields(%s)="
        "groundloop_m5_digest_text_fields(%s)",
        (["domain", "text", "", "int", "0"], ["domain", "text", "", "int", "0"]),
    ).fetchone() == (True,)
    with pytest.raises(psycopg.errors.RaiseException, match="cannot contain SQL NULL"):
        installed.execute(
            "SELECT groundloop_m5_matching_digest_text_fields(ARRAY['ok',NULL])"
        )
    installed.rollback()
    group_shape = _framed_preimage(
        "m5-persisted-matching-group-shape-set-v1", "sequence", "int", "0"
    )
    logical_patch = _framed_preimage(
        "m5-persisted-logical-overlay-patch-v1",
        "sequence",
        "int",
        "0",
        "sequence",
        "int",
        "0",
        "sha256",
        "b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3",
        "int",
        "71",
    )
    logical_output = bytes.fromhex(
        "710000000000000002000000000000002573000000000000001c"
        "6d352d6f7665726c61792d6c6f676963616c2d6f75747075742d7632"
        "0000000000000009710000000000000000"
    )
    assert hashlib.sha256(group_shape).hexdigest() == (
        "641755ac2411ac68744c2b7011c24b9ba2c28aeb91454147d42bd7b3e138aae7"
    )
    assert hashlib.sha256(logical_patch).hexdigest() == (
        "a33bc746597b4dcb54564db2894bf8a9b97ce9902a0623416850846c3ddb6db4"
    )
    assert hashlib.sha256(logical_output).hexdigest() == (
        "b4e641b66a06cb7d204377c37cfe031d958ce6d959832620fc2e9441339581c3"
    )
    assert installed.execute(
        "SELECT groundloop_m5_matching_parse_typed_preimage(%s,%s)->>'domain'",
        (group_shape, "m5-persisted-matching-group-shape-set-v1"),
    ).fetchone() == ("m5-persisted-matching-group-shape-set-v1",)
    assert installed.execute(
        "SELECT groundloop_m5_matching_parse_typed_preimage(%s,%s)->>'domain'",
        (logical_patch, "m5-persisted-logical-overlay-patch-v1"),
    ).fetchone() == ("m5-persisted-logical-overlay-patch-v1",)
    assert installed.execute(
        "SELECT jsonb_array_length(groundloop_m5_matching_validate_logical_output(%s))",
        (logical_output,),
    ).fetchone() == (0,)
    installed.commit()


def test_lane_a_same_byte_all_physical_point_variants(
    installed: Connection[Any],
) -> None:
    h1 = "1" * 64
    oc = _sequence(
        _typed("enum", "current"),
        _typed("text", "o"),
        _typed("text", "r"),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 0),
    )
    ow = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "o"),
        _typed("text", "r"),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("sha256", h1),
        _typed("bool", True),
        _typed("int", 4),
    )
    ec = _sequence(
        _typed("enum", "current"),
        _typed("text", "r"),
        _typed("sha256", h1),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    ew = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "r"),
        _typed("sha256", h1),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("int", 0),
        _typed("int", 4),
    )
    mc = _sequence(
        _typed("enum", "current"),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    mw = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 0),
        _typed("int", 4),
    )
    integer_pair = _sequence(_typed("int", 0), _typed("int", 1))
    zero_pair = _sequence(_typed("int", 0), _typed("int", 0))
    hc = _sequence(
        _typed("enum", "current"),
        _typed("text", "g"),
        _typed("int", 1),
        integer_pair,
        integer_pair,
        zero_pair,
        _typed("int", 0),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    hw = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("bool", False),
        *(("null",) for _ in range(7)),
        _typed("int", 4),
    )
    vectors = (
        (
            "observation",
            _change_bytes(
                "m5-persisted-matching-observation-change-v1",
                (_typed("text", "o"),),
                oc,
                ow,
            ),
            "0c8ebfa96b5a6e714cc6d28ba1e4a6925cb51c92a58ecae0d6dd3f719675dba5",
        ),
        (
            "observation",
            _change_bytes(
                "m5-persisted-matching-observation-change-v1",
                (_typed("text", "o"),),
                ow,
                ow,
            ),
            "bb154018eaf9d1fd80ce44f32e0cece62df4f4b5959b70f2eebbb7bdf2da5510",
        ),
        (
            "edge",
            _change_bytes(
                "m5-persisted-matching-edge-change-v1",
                (_typed("text", "r"), _typed("sha256", h1)),
                ec,
                ew,
            ),
            "ba51a129a160a2c5bf7137575b50b7842b4b2f34ba512869583b629899fbf9ac",
        ),
        (
            "edge",
            _change_bytes(
                "m5-persisted-matching-edge-change-v1",
                (_typed("text", "r"), _typed("sha256", h1)),
                ew,
                ew,
            ),
            "f686b8908e7a86471cc0797cdb70e991899b9abde5e892d486876d25d616b84d",
        ),
        (
            "mask",
            _change_bytes(
                "m5-persisted-matching-mask-change-v1",
                (_typed("text", "g"), _typed("sha256", h1)),
                mc,
                mw,
            ),
            "017135359a61254d2a748266293b9ef82fca5ffeb18ec8d276a7dfe628ae684b",
        ),
        (
            "mask",
            _change_bytes(
                "m5-persisted-matching-mask-change-v1",
                (_typed("text", "g"), _typed("sha256", h1)),
                mw,
                mw,
            ),
            "5f33d51a1ec876df1b6a8e3db1d6db69bc6068b9feb2da40a6c85240298d843f",
        ),
        (
            "hall",
            _change_bytes(
                "m5-persisted-matching-hall-change-v1", (_typed("text", "g"),), hc, hw
            ),
            "e729e5a4700fff2d6c0f5b3c2d8a3ec319d94157f1e3801d8980e6ebe54db3d0",
        ),
        (
            "hall",
            _change_bytes(
                "m5-persisted-matching-hall-change-v1", (_typed("text", "g"),), hw, hw
            ),
            "36fdc7bd588af304c03d858af54637cf6605e9f527fe316798afbcba10c8adce",
        ),
    )
    for family, value, expected in vectors:
        assert hashlib.sha256(value).hexdigest() == expected
        row = installed.execute(
            "SELECT groundloop_m5_matching_validate_change(%s,%s)->>'outer_one'",
            (value, family),
        ).fetchone()
        assert row is not None and row[0]
    installed.commit()


def test_physical_point_source_shape_and_mask_bounds(
    installed: Connection[Any],
) -> None:
    h1 = "1" * 64
    shapes = _framed_preimage(
        "m5-persisted-matching-group-shape-set-v1",
        *_sequence(
            _sequence(
                _typed("text", "g"),
                _typed("int", 1),
                _sequence(_sequence(_typed("int", 0), _typed("text", "r"))),
            )
        ),
    )
    decoded_shapes = installed.execute(
        "SELECT groundloop_m5_matching_validate_group_shapes(%s)", (shapes,)
    ).fetchone()
    assert decoded_shapes is not None
    good_working = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 4),
    )
    valid = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        ("null",),
        good_working,
    )
    decoded = installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'mask')", (valid,)
    ).fetchone()
    assert decoded is not None
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_change_shape(%s,%s,'mask')",
        (Jsonb(decoded[0]), Jsonb(decoded_shapes[0])),
    ).fetchone() == (True,)
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_patch_change_point"
        "(%s,'direct_transition',2,3,2,4)",
        (Jsonb(decoded[0]),),
    ).fetchone() == (True,)
    bad_working = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 2),
        _typed("int", 4),
    )
    bad = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        ("null",),
        bad_working,
    )
    bad_decoded = installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'mask')", (bad,)
    ).fetchone()
    assert bad_decoded is not None
    with pytest.raises(psycopg.errors.RaiseException, match="covering group shape"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_change_shape(%s,%s,'mask')",
            (Jsonb(bad_decoded[0]), Jsonb(decoded_shapes[0])),
        )
    installed.rollback()

    older_working = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 2),
    )
    older = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        older_working,
        good_working,
    )
    older_decoded = installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'mask')", (older,)
    ).fetchone()
    assert older_decoded is not None
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_patch_change_point"
        "(%s,'direct_transition',2,3,2,4)",
        (Jsonb(older_decoded[0]),),
    ).fetchone() == (True,)
    installed.commit()

    negative_current = _sequence(
        _typed("enum", "current"),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", -1),
        _typed("int", 0),
    )
    invalid_coordinates = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        negative_current,
        good_working,
    )
    with pytest.raises(psycopg.errors.RaiseException, match="coordinates"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_change(%s,'mask')",
            (invalid_coordinates,),
        )
    installed.rollback()

    structural_after = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 1),
    )
    later = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        good_working,
        structural_after,
    )
    later_decoded = installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'mask')", (later,)
    ).fetchone()
    assert later_decoded is not None
    with pytest.raises(psycopg.errors.RaiseException, match="current"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_patch_change_point"
            "(%s,'structural_open',1,3,2,1)",
            (Jsonb(later_decoded[0]),),
        )
    installed.rollback()

    wrong_revision = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 5),
    )
    wrong = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        ("null",),
        wrong_revision,
    )
    wrong_decoded = installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'mask')", (wrong,)
    ).fetchone()
    assert wrong_decoded is not None
    with pytest.raises(psycopg.errors.RaiseException, match="patch result"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_patch_change_point"
            "(%s,'direct_transition',2,3,2,4)",
            (Jsonb(wrong_decoded[0]),),
        )
    installed.rollback()


def test_hall_all_array_and_scalar_laws(installed: Connection[Any]) -> None:
    valid = (1, [0, 1], [0, 1], [0, 0], 0, 1, 1)
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_hall(%s,%s,%s,%s,%s,%s,%s)",
        valid,
    ).fetchone() == (True,)
    invalid = (
        (1, [1, 0], [0, 0], [0, 0], 0, 1, 0),
        (1, [0, -1], [0, -1], [0, 0], 0, 1, -1),
        (1, [0, 1], [0, 0], [0, 0], 0, 1, 1),
        (1, [0, 1], [0, 1], [0, 1], 0, 1, 1),
        (1, [0, 1], [0, 1], [0, 0], 1, 1, 1),
        (1, [0, 1], [0, 1], [0, 0], 0, 0, 1),
        (1, [0, 1], [0, 1], [0, 0], 0, 1, 2),
        (2, [0, 1], [0, 1], [0, 0], 0, 1, 1),
    )
    for values in invalid:
        assert installed.execute(
            "SELECT groundloop_m5_matching_validate_hall(%s,%s,%s,%s,%s,%s,%s)",
            values,
        ).fetchone() == (False,)
    pair = _sequence(_typed("int", 0), _typed("int", 1))
    zeros = _sequence(_typed("int", 0), _typed("int", 0))
    hall = _change_bytes(
        "m5-persisted-matching-hall-change-v1",
        (_typed("text", "g"),),
        ("null",),
        _sequence(
            _typed("enum", "working"),
            _typed("int", 2),
            _typed("text", "g"),
            _typed("bool", True),
            _typed("int", 1),
            pair,
            pair,
            zeros,
            _typed("int", 0),
            _typed("int", 1),
            _typed("int", 1),
            _typed("int", 4),
        ),
    )
    decoded_hall = installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'hall')", (hall,)
    ).fetchone()
    shape_r2 = _framed_preimage(
        "m5-persisted-matching-group-shape-set-v1",
        *_sequence(
            _sequence(
                _typed("text", "g"),
                _typed("int", 2),
                _sequence(
                    _sequence(_typed("int", 0), _typed("text", "r0")),
                    _sequence(_typed("int", 1), _typed("text", "r1")),
                ),
            )
        ),
    )
    decoded_r2 = installed.execute(
        "SELECT groundloop_m5_matching_validate_group_shapes(%s)", (shape_r2,)
    ).fetchone()
    assert decoded_hall is not None and decoded_r2 is not None
    with pytest.raises(psycopg.errors.RaiseException, match="covering group shape"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_change_shape(%s,%s,'hall')",
            (Jsonb(decoded_hall[0]), Jsonb(decoded_r2[0])),
        )
    installed.rollback()


@pytest.mark.parametrize(
    ("family", "previous", "current"),
    (
        ("observation", {"outer_one": "b"}, {"outer_one": "a"}),
        ("hall", {"outer_one": "g"}, {"outer_one": "g"}),
        (
            "mask",
            {"outer_one": "g", "outer_two": "2"},
            {"outer_one": "g", "outer_two": "1"},
        ),
        (
            "edge",
            {
                "sort_group": "g",
                "sort_ordinal": "1",
                "outer_two": "1",
                "outer_one": "r",
            },
            {
                "sort_group": "g",
                "sort_ordinal": "0",
                "outer_two": "f",
                "outer_one": "z",
            },
        ),
    ),
)
def test_physical_family_component_order_and_uniqueness(
    installed: Connection[Any],
    family: str,
    previous: dict[str, str],
    current: dict[str, str],
) -> None:
    with pytest.raises(psycopg.errors.RaiseException, match="sorted unique"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_change_order(%s,%s,%s)",
            (
                Jsonb(previous),
                Jsonb(current),
                family,
            ),
        )
    installed.rollback()


def test_edge_repeated_coordinates_are_immutable(installed: Connection[Any]) -> None:
    h1 = "1" * 64
    before = _sequence(
        _typed("enum", "current"),
        _typed("text", "r"),
        _typed("sha256", h1),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    after = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "r"),
        _typed("sha256", h1),
        _typed("text", "other-g"),
        _typed("int", 0),
        _typed("int", 1),
        _typed("int", 4),
    )
    value = _change_bytes(
        "m5-persisted-matching-edge-change-v1",
        (_typed("text", "r"), _typed("sha256", h1)),
        before,
        after,
    )
    with pytest.raises(psycopg.errors.RaiseException, match="immutable coordinates"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_change(%s,'edge')", (value,)
        )
    installed.rollback()


@pytest.mark.parametrize(
    "malformed",
    (
        b"\x00",
        b"nx",
        b"s" + (1).to_bytes(8, "big") + b"\xff",
        b"q" + (1).to_bytes(8, "big"),
        b"i" + (2).to_bytes(8, "big") + b"00",
        b"f" + bytes.fromhex("7ff0000000000000"),
    ),
)
def test_recursive_logical_decoder_rejects_noncanonical_bytes(
    installed: Connection[Any], malformed: bytes
) -> None:
    with pytest.raises(psycopg.Error):
        installed.execute(
            "SELECT groundloop_m5_matching_parse_logical(%s)", (malformed,)
        )
    installed.rollback()


def test_recursive_logical_exact_dataclass_and_empty_string_rules(
    installed: Connection[Any],
) -> None:
    from groundloop.m5.domain import RequirementState
    from groundloop.m5.incremental_overlay import _logical_value_bytes

    assert installed.execute(
        "SELECT groundloop_m5_matching_parse_logical(%s)->>'value'",
        (_logical_value_bytes(""),),
    ).fetchone() == ("",)
    encoded = _logical_value_bytes(RequirementState("r", ("1" * 64,), ("o",), 1, True))
    mutations = (
        encoded + b"x",
        encoded.replace(b"RequirementState", b"XequirementState", 1),
        encoded.replace(b"requirement_version_id", b"xequirement_version_id", 1),
        encoded[: 1 + 8 + len("RequirementState")]
        + (4).to_bytes(8, "big")
        + encoded[1 + 8 + len("RequirementState") + 8 :],
    )
    for malformed in mutations:
        with pytest.raises(psycopg.Error):
            installed.execute(
                "SELECT groundloop_m5_matching_parse_logical(%s)", (malformed,)
            )
        installed.rollback()


def test_lane_a_recursive_logical_output_all_nine_blocks(
    installed: Connection[Any],
) -> None:
    from groundloop.domain import AnswerStatus, ClaimStatus, StatusDelta
    from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
    from groundloop.m5.domain import (
        ClaimCertificateArtifact,
        ClaimSupportKind,
        CombinedAnswerState,
        CombinedClaimState,
        GroupCertificateRow,
        GroupMatchingCertificateArtifact,
        GroupState,
        RequirementState,
    )
    from groundloop.m5.incremental_overlay import _logical_output_image
    from groundloop.m5.matching import WorkingGroupCertificateBinding

    h1 = "1" * 64
    records = (
        ("requirement_state", "r", RequirementState("r", (h1,), ("o",), 1, True)),
        ("group_state", "g", GroupState("g", 1, 1, 1, True)),
        (
            "claim_state",
            "c",
            CombinedClaimState(
                "c", 0, 0, None, None, (), (), 0, (), ClaimStatus.SUPPORTED
            ),
        ),
        (
            "answer_state",
            "a",
            CombinedAnswerState("a", 1, 1, 0, 0, 0, AnswerStatus.VALID),
        ),
        (
            "group_certificate",
            "g",
            GroupMatchingCertificateArtifact(
                "policy", "g", (GroupCertificateRow(0, "r", h1, "o"),)
            ),
        ),
        (
            "claim_certificate",
            "c",
            ClaimCertificateArtifact("c", "policy", ClaimSupportKind.NONE),
        ),
        ("group_binding", "g", WorkingGroupCertificateBinding(2, "g", 4, None, h1)),
        ("claim_binding", "c", WorkingClaimCertificateBinding(2, "c", 4, None, h1)),
        (
            "status_delta",
            "c",
            StatusDelta("event", "claim", "c", "custom-old", "custom-new", ""),
        ),
    )
    encoded = _logical_output_image(records)
    decoded = installed.execute(
        "SELECT groundloop_m5_matching_validate_logical_output(%s)",
        (encoded,),
    ).fetchone()
    assert decoded is not None
    assert [(row["kind"], row["object_id"]) for row in decoded[0]] == [
        (kind, object_id) for kind, object_id, _after in records
    ]
    malformed_hash = encoded.replace(b"1" * 64, b"z" + b"1" * 63, 1)
    with pytest.raises(psycopg.errors.RaiseException, match="type/key mismatch"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_output(%s)",
            (malformed_hash,),
        )
    installed.rollback()
    wrong_type = encoded.replace(b"RequirementState", b"XequirementState", 1)
    with pytest.raises(psycopg.errors.RaiseException, match="unknown or malformed"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_output(%s)",
            (wrong_type,),
        )
    installed.rollback()


def test_claim_certificate_some_identifiers_and_digest_are_strict(
    installed: Connection[Any],
) -> None:
    from groundloop.m5.domain import ClaimCertificateArtifact, ClaimSupportKind
    from groundloop.m5.incremental_overlay import _logical_output_image

    h1 = "1" * 64
    artifact = ClaimCertificateArtifact(
        "c",
        "policy",
        ClaimSupportKind.GROUP,
        group_version_id="g",
        group_certificate_digest=h1,
    )
    encoded = _logical_output_image((("claim_certificate", "c", artifact),))
    assert installed.execute(
        "SELECT jsonb_array_length(groundloop_m5_matching_validate_logical_output(%s))",
        (encoded,),
    ).fetchone() == (1,)
    bad_hash = encoded.replace(h1.encode(), ("z" + "1" * 63).encode(), 1)
    with pytest.raises(psycopg.errors.RaiseException, match="type/key mismatch"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_output(%s)", (bad_hash,)
        )
    installed.rollback()
    framed_group_id = b"s" + (1).to_bytes(8, "big") + b"g"
    for whitespace_bytes in (b" ", b"\t"):
        blank_id = encoded.replace(
            framed_group_id,
            b"s" + (1).to_bytes(8, "big") + whitespace_bytes,
            1,
        )
        with pytest.raises(psycopg.errors.RaiseException, match="type/key mismatch"):
            installed.execute(
                "SELECT groundloop_m5_matching_validate_logical_output(%s)",
                (blank_id,),
            )
        installed.rollback()
    unicode_artifact = ClaimCertificateArtifact(
        "c",
        "policy",
        ClaimSupportKind.GROUP,
        group_version_id="gg",
        group_certificate_digest=h1,
    )
    unicode_encoded = _logical_output_image(
        (("claim_certificate", "c", unicode_artifact),)
    )
    framed_double = b"s" + (2).to_bytes(8, "big") + b"gg"
    unicode_blank = unicode_encoded.replace(
        framed_double, b"s" + (2).to_bytes(8, "big") + "\u00a0".encode(), 1
    )
    with pytest.raises(psycopg.errors.RaiseException, match="type/key mismatch"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_output(%s)",
            (unicode_blank,),
        )
    installed.rollback()


@pytest.mark.parametrize(
    "codepoint",
    (
        9,
        10,
        11,
        12,
        13,
        28,
        29,
        30,
        31,
        32,
        133,
        160,
        5760,
        8192,
        8193,
        8194,
        8195,
        8196,
        8197,
        8198,
        8199,
        8200,
        8201,
        8202,
        8232,
        8233,
        8239,
        8287,
        12288,
    ),
)
def test_identifier_matches_all_python_strip_empty_codepoints(
    installed: Connection[Any], codepoint: int
) -> None:
    whitespace = chr(codepoint)
    assert installed.execute(
        "SELECT groundloop_m5_matching_identifier(%s)", (whitespace,)
    ).fetchone() == (False,)
    assert installed.execute(
        "SELECT groundloop_m5_matching_identifier(%s)",
        (whitespace + "x" + whitespace,),
    ).fetchone() == (True,)
    installed.commit()


def test_lane_a_derivable_logical_hashes_and_preserved_producer_order(
    installed: Connection[Any],
) -> None:
    from groundloop.domain import AnswerStatus
    from groundloop.m5.domain import (
        ClaimCertificateArtifact,
        ClaimSupportKind,
        CombinedAnswerState,
        GroupCertificateRow,
        GroupMatchingCertificateArtifact,
        RequirementState,
    )
    from groundloop.m5.incremental_overlay import _logical_output_image
    from groundloop.m5.runtime import digests as runtime_digests

    policy = "policy"
    h1 = "1" * 64
    requirement_z = RequirementState("rz", (h1,), ("oz",), 1, True)
    requirement_a = RequirementState("ra", (h1,), ("oa",), 1, True)
    answer = CombinedAnswerState("a", 1, 1, 0, 0, 0, AnswerStatus.VALID)
    group_certificate = GroupMatchingCertificateArtifact(
        policy, "g", (GroupCertificateRow(0, "ra", h1, "oa"),)
    )
    claim_certificate = ClaimCertificateArtifact("c", policy, ClaimSupportKind.NONE)
    records = (
        # Deliberately retained first-touch order, not object-id sort.
        ("requirement_state", "rz", requirement_z),
        ("requirement_state", "ra", requirement_a),
        ("answer_state", "a", answer),
        ("group_certificate", "g", group_certificate),
        ("claim_certificate", "c", claim_certificate),
    )
    output_preimage = _logical_output_image(records)
    output_digest = hashlib.sha256(output_preimage).hexdigest()
    output_bytes = len(output_preimage)
    changes = (
        (
            "answer_state",
            "a",
            None,
            runtime_digests.answer_state_artifact_digest(
                answer_version_id="a",
                required_claim_count=1,
                supported_count=1,
                unsupported_count=0,
                refuted_count=0,
                conflicted_count=0,
                status=AnswerStatus.VALID,
            ),
        ),
        (
            "claim_certificate",
            "c",
            None,
            claim_certificate.certificate_digest,
        ),
        (
            "group_certificate",
            "g",
            None,
            group_certificate.certificate_digest,
        ),
        (
            "requirement_state",
            "ra",
            None,
            runtime_digests.requirement_state_artifact_digest(
                requirement_version_id="ra",
                witness_hashes=(h1,),
                supporting_observation_ids=("oa",),
                witness_count=1,
                satisfied=True,
                decision_policy_version=policy,
            ),
        ),
        (
            "requirement_state",
            "rz",
            None,
            runtime_digests.requirement_state_artifact_digest(
                requirement_version_id="rz",
                witness_hashes=(h1,),
                supporting_observation_ids=("oz",),
                witness_count=1,
                satisfied=True,
                decision_policy_version=policy,
            ),
        ),
    )

    def logical_patch(change_rows: tuple[tuple[object, ...], ...]) -> bytes:
        encoded_changes = tuple(
            _sequence(
                _typed("enum", kind),
                _typed("text", object_id),
                ("null",) if before_hash is None else _typed("sha256", before_hash),
                ("null",) if after_hash is None else _typed("sha256", after_hash),
            )
            for kind, object_id, before_hash, after_hash in change_rows
        )
        return _framed_preimage(
            "m5-persisted-logical-overlay-patch-v1",
            *_sequence(*encoded_changes),
            *_sequence(),
            *_typed("sha256", output_digest),
            *_typed("int", output_bytes),
        )

    patch_preimage = logical_patch(changes)
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,%s)",
        (patch_preimage, output_preimage, policy),
    ).fetchone() == (True,)
    bad_changes = (*changes[:-1], (*changes[-1][:-1], "2" * 64))
    bad_preimage = logical_patch(bad_changes)
    with pytest.raises(psycopg.errors.RaiseException, match="derivable after hash"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,%s)",
            (bad_preimage, output_preimage, policy),
        )
    installed.rollback()


def test_lane_a_binding_rows_digest_order_epoch_and_close_open_laws(
    installed: Connection[Any],
) -> None:
    from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
    from groundloop.m5.incremental_overlay import _logical_output_image
    from groundloop.m5.matching import WorkingGroupCertificateBinding

    h1, h2 = "1" * 64, "2" * 64
    rows = (
        ("group_binding", "g", WorkingGroupCertificateBinding(2, "g", 2, 4, h1)),
        ("group_binding", "g", WorkingGroupCertificateBinding(2, "g", 4, None, h2)),
        ("claim_binding", "c", WorkingClaimCertificateBinding(2, "c", 4, None, h2)),
    )
    output = _logical_output_image(rows)

    def binding_digest(
        kind: str,
        epoch: int,
        object_id: str,
        valid_from: int,
        valid_to: int | None,
        digest: str,
    ) -> str:
        option = ("null",) if valid_to is None else _typed("int", valid_to)
        value = _framed_preimage(
            "m5-persisted-certificate-binding-row-v1",
            *_typed("enum", kind),
            *_typed("int", epoch),
            *_typed("text", object_id),
            *_typed("int", valid_from),
            *option,
            *_typed("sha256", digest),
        )
        return hashlib.sha256(value).hexdigest()

    digests = (
        binding_digest("group", 2, "g", 2, 4, h1),
        binding_digest("group", 2, "g", 4, None, h2),
        binding_digest("claim", 2, "c", 4, None, h2),
    )

    def binding_patch(digest_rows: tuple[str, ...], image: bytes) -> bytes:
        return _framed_preimage(
            "m5-persisted-logical-overlay-patch-v1",
            *_sequence(),
            *_sequence(*(_typed("sha256", value) for value in digest_rows)),
            *_typed("sha256", hashlib.sha256(image).hexdigest()),
            *_typed("int", len(image)),
        )

    patch = binding_patch(digests, output)
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,'policy')",
        (patch, output),
    ).fetchone() == (True,)
    reordered = binding_patch((digests[1], digests[0], digests[2]), output)
    with pytest.raises(psycopg.errors.RaiseException, match="digest/output"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,'policy')",
            (reordered, output),
        )
    installed.rollback()

    for lone in (rows[0], rows[1]):
        lone_output = _logical_output_image((lone,))
        lone_digest = digests[0] if lone is rows[0] else digests[1]
        assert installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,'policy')",
            (binding_patch((lone_digest,), lone_output), lone_output),
        ).fetchone() == (True,)
    installed.commit()

    wrong_epoch_row = (
        "group_binding",
        "g",
        WorkingGroupCertificateBinding(3, "g", 4, None, h2),
    )
    wrong_epoch_output = _logical_output_image((wrong_epoch_row,))
    wrong_epoch_digest = binding_digest("group", 3, "g", 4, None, h2)
    with pytest.raises(psycopg.errors.RaiseException, match="binding point"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,'policy')",
            (
                binding_patch((wrong_epoch_digest,), wrong_epoch_output),
                wrong_epoch_output,
            ),
        )
    installed.rollback()

    wrong_revision_row = (
        "group_binding",
        "g",
        WorkingGroupCertificateBinding(2, "g", 2, 5, h1),
    )
    wrong_revision_output = _logical_output_image((wrong_revision_row,))
    wrong_revision_digest = binding_digest("group", 2, "g", 2, 5, h1)
    with pytest.raises(psycopg.errors.RaiseException, match="binding point"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,'policy')",
            (
                binding_patch((wrong_revision_digest,), wrong_revision_output),
                wrong_revision_output,
            ),
        )
    installed.rollback()

    duplicate_output = _logical_output_image((rows[0], rows[0]))
    duplicate_patch = binding_patch((digests[0], digests[0]), duplicate_output)
    with pytest.raises(psycopg.errors.RaiseException, match="close/open order"):
        installed.execute(
            "SELECT groundloop_m5_matching_validate_logical_patch(%s,%s,2,4,'policy')",
            (duplicate_patch, duplicate_output),
        )
    installed.rollback()


def test_preledger_lane_a_full_nonempty_constructor_bytes(
    preledger_lane_a_decoder: Connection[Any],
) -> None:
    installed = preledger_lane_a_decoder
    from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
    from groundloop.m5.domain import (
        ClaimCertificateArtifact,
        ClaimSupportKind,
        GroupCertificateRow,
        GroupMatchingCertificateArtifact,
        GroupState,
    )
    from groundloop.m5.incremental_overlay import _logical_output_image
    from groundloop.m5.matching import WorkingGroupCertificateBinding

    h1 = "1" * 64
    after = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "o"),
        _typed("text", "r"),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("sha256", h1),
        _typed("bool", True),
        _typed("int", 4),
    )
    change_preimage = _change_bytes(
        "m5-persisted-matching-observation-change-v1",
        (_typed("text", "o"),),
        ("null",),
        after,
    )
    change_digest = hashlib.sha256(change_preimage).hexdigest()
    edge_before = _sequence(
        _typed("enum", "current"),
        _typed("text", "r"),
        _typed("sha256", h1),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    edge_after = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "r"),
        _typed("sha256", h1),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("int", 0),
        _typed("int", 4),
    )
    edge_preimage = _change_bytes(
        "m5-persisted-matching-edge-change-v1",
        (_typed("text", "r"), _typed("sha256", h1)),
        edge_before,
        edge_after,
    )
    edge_digest = hashlib.sha256(edge_preimage).hexdigest()
    mask_before = _sequence(
        _typed("enum", "current"),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    mask_after = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("sha256", h1),
        _typed("int", 0),
        _typed("int", 4),
    )
    mask_preimage = _change_bytes(
        "m5-persisted-matching-mask-change-v1",
        (_typed("text", "g"), _typed("sha256", h1)),
        mask_before,
        mask_after,
    )
    mask_digest = hashlib.sha256(mask_preimage).hexdigest()
    pair = _sequence(_typed("int", 0), _typed("int", 1))
    zero_pair = _sequence(_typed("int", 0), _typed("int", 0))
    hall_before = _sequence(
        _typed("enum", "current"),
        _typed("text", "g"),
        _typed("int", 1),
        pair,
        pair,
        zero_pair,
        _typed("int", 0),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 1),
        _typed("int", 0),
    )
    hall_after = _sequence(
        _typed("enum", "working"),
        _typed("int", 2),
        _typed("text", "g"),
        _typed("bool", False),
        *(("null",) for _ in range(7)),
        _typed("int", 4),
    )
    hall_preimage = _change_bytes(
        "m5-persisted-matching-hall-change-v1",
        (_typed("text", "g"),),
        hall_before,
        hall_after,
    )
    hall_digest = hashlib.sha256(hall_preimage).hexdigest()
    shape_preimage = _framed_preimage(
        "m5-persisted-matching-group-shape-set-v1",
        *_sequence(
            _sequence(
                _typed("text", "g"),
                _typed("int", 1),
                _sequence(_sequence(_typed("int", 0), _typed("text", "r"))),
            )
        ),
    )
    shape_digest = hashlib.sha256(shape_preimage).hexdigest()
    group_certificate = GroupMatchingCertificateArtifact(
        "policy", "g", (GroupCertificateRow(0, "r", h1, "o"),)
    )
    claim_certificate = ClaimCertificateArtifact("c", "policy", ClaimSupportKind.NONE)
    output_records = (
        ("group_state", "g", GroupState("g", 1, 1, 1, True)),
        ("group_certificate", "g", group_certificate),
        ("claim_certificate", "c", claim_certificate),
        (
            "group_binding",
            "g",
            WorkingGroupCertificateBinding(
                2, "g", 4, None, group_certificate.certificate_digest
            ),
        ),
        (
            "claim_binding",
            "c",
            WorkingClaimCertificateBinding(
                2, "c", 4, None, claim_certificate.certificate_digest
            ),
        ),
    )
    output = _logical_output_image(output_records)
    output_digest = hashlib.sha256(output).hexdigest()
    logical_changes = _sequence(
        _sequence(
            _typed("enum", "claim_certificate"),
            _typed("text", "c"),
            ("null",),
            _typed("sha256", claim_certificate.certificate_digest),
        ),
        _sequence(
            _typed("enum", "group_certificate"),
            _typed("text", "g"),
            ("null",),
            _typed("sha256", group_certificate.certificate_digest),
        ),
        _sequence(
            _typed("enum", "group_state"),
            _typed("text", "g"),
            ("null",),
            _typed("sha256", h1),
        ),
    )
    group_binding_digest = hashlib.sha256(
        _framed_preimage(
            "m5-persisted-certificate-binding-row-v1",
            *_typed("enum", "group"),
            *_typed("int", 2),
            *_typed("text", "g"),
            *_typed("int", 4),
            "null",
            *_typed("sha256", group_certificate.certificate_digest),
        )
    ).hexdigest()
    claim_binding_digest = hashlib.sha256(
        _framed_preimage(
            "m5-persisted-certificate-binding-row-v1",
            *_typed("enum", "claim"),
            *_typed("int", 2),
            *_typed("text", "c"),
            *_typed("int", 4),
            "null",
            *_typed("sha256", claim_certificate.certificate_digest),
        )
    ).hexdigest()
    logical_preimage = _framed_preimage(
        "m5-persisted-logical-overlay-patch-v1",
        *logical_changes,
        *_sequence(
            _typed("sha256", group_binding_digest),
            _typed("sha256", claim_binding_digest),
        ),
        *_typed("sha256", output_digest),
        *_typed("int", len(output)),
    )
    logical_digest = hashlib.sha256(logical_preimage).hexdigest()
    work_values = [1, *([0] * 29), len(output), *([0] * 6)]
    work_preimage = _framed_preimage(
        "m5-matching-work-v1",
        *(field for value in work_values for field in _typed("int", value)),
    )
    assert len(work_values) == 37
    work_digest = hashlib.sha256(work_preimage).hexdigest()
    patch_preimage = _framed_preimage(
        "m5-persisted-matching-patch-v1",
        *_typed("enum", "direct_transition"),
        *_typed("text", "source"),
        *_typed("sha256", h1),
        *_typed("int", 2),
        *_typed("int", 3),
        *_typed("int", 2),
        *_typed("int", 4),
        *_typed("text", "policy"),
        *_typed("sha256", shape_digest),
        *_sequence(_typed("sha256", change_digest)),
        *_sequence(_typed("sha256", edge_digest)),
        *_sequence(_typed("sha256", mask_digest)),
        *_sequence(_typed("sha256", hall_digest)),
        *_typed("sha256", logical_digest),
        *_typed("sha256", work_digest),
    )
    patch_digest = hashlib.sha256(patch_preimage).hexdigest()
    contribution_preimage = _framed_preimage(
        "m5-matching-work-contribution-v1",
        *_typed("int", 2),
        *_typed("enum", "direct_transition"),
        *_typed("text", "source"),
        *_typed("sha256", h1),
        *_typed("int", 2),
        *_typed("int", 3),
        *_typed("int", 4),
        *_typed("sha256", patch_digest),
        *_typed("sha256", work_digest),
    )
    contribution_digest = hashlib.sha256(contribution_preimage).hexdigest()
    assert patch_digest == (
        "1fd2cebe2a8c52ce759dedeef0be8ea59424e5f188937061980f8b4a68a008e1"
    )
    assert contribution_digest == (
        "175180ec19957d7f474bbdb19360f54467c05189e1774635fd6068f9dd25a538"
    )
    assert (
        change_digest,
        edge_digest,
        mask_digest,
        hall_digest,
        shape_digest,
        logical_digest,
        work_digest,
    ) == (
        "612b9d5c8910d21a7ee4d894a9c960d05f302e8f34bd1e5109f8a27d896723c0",
        "ba51a129a160a2c5bf7137575b50b7842b4b2f34ba512869583b629899fbf9ac",
        "017135359a61254d2a748266293b9ef82fca5ffeb18ec8d276a7dfe628ae684b",
        "e729e5a4700fff2d6c0f5b3c2d8a3ec319d94157f1e3801d8980e6ebe54db3d0",
        "cf81264bcf457551a32f9e24d78d6788b14a34a719b9eacde9fbd1d594bc40f7",
        "befa10487cf40ed99aa08da0510fded69cc1a548ab0c58d8d33ab23473608378",
        "fd9144c72fad33f99ef3c9efb1bb8df7ea4015b47b2c1f0a6d4b1a35f31b7f24",
    )
    assert installed.execute(
        "SELECT groundloop_m5_matching_hash_preimage(%s)", (patch_preimage,)
    ).fetchone() == (patch_digest,)
    assert installed.execute(
        "SELECT groundloop_m5_matching_hash_preimage(%s)", (change_preimage,)
    ).fetchone() == (change_digest,)
    assert installed.execute(
        "SELECT groundloop_m5_matching_validate_change(%s,'observation')"
        "->'before'='null'::jsonb",
        (change_preimage,),
    ).fetchone() == (True,)
    assert installed.execute(
        "SELECT groundloop_m5_matching_work_digest(%s)", (list(work_values),)
    ).fetchone() == (work_digest,)
    assert installed.execute(
        "SELECT groundloop_m5_matching_parse_typed_preimage"
        "(%s,'m5-persisted-matching-patch-v1')->>'domain'",
        (patch_preimage,),
    ).fetchone() == ("m5-persisted-matching-patch-v1",)
    mutated = patch_preimage[:-1] + bytes([patch_preimage[-1] ^ 1])
    assert installed.execute(
        "SELECT groundloop_m5_matching_hash_preimage(%s)<>%s",
        (mutated, patch_digest),
    ).fetchone() == (True,)
    installed.execute(
        """INSERT INTO groundloop_epoch (
             epoch_id,event_id,payload_hash,revision,structural_status,
             semantic_status,evaluation_state,publication_mode,sealed_at
           ) OVERRIDING SYSTEM VALUE VALUES
             (1,'d25-b1-base',%s,3,'committed','sealed','complete','strict',now()),
             (2,'d25-b1-next',%s,4,'committed','complete','complete','strict',NULL)""",
        (h1, "2" * 64),
    )
    installed.execute(
        """INSERT INTO groundloop_decision_policy (
             policy_version,support_threshold,refute_threshold,tie_rule_version,
             valid_from_epoch
           ) VALUES ('policy',0.5,0.5,'v1',1)"""
    )
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    installed.execute(
        """INSERT INTO groundloop_m5_matching_patch_artifact (
             patch_digest,source_kind,source_id,source_identity_hash,
             before_epoch_id,before_revision,resulting_epoch_id,resulting_revision,
             decision_policy_version,group_shape_set_digest,group_shape_set_preimage,
             observation_change_digests,observation_change_preimages,
             edge_change_digests,edge_change_preimages,
             mask_change_digests,mask_change_preimages,
             hall_change_digests,hall_change_preimages,
             logical_overlay_patch_digest,logical_overlay_patch_preimage,
             logical_output_preimage,matching_work_digest,canonical_patch_preimage
           ) VALUES (
             %s,'direct_transition','source',%s,2,3,2,4,'policy',%s,%s,
             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
           )""",
        (
            patch_digest,
            h1,
            shape_digest,
            shape_preimage,
            [change_digest],
            [change_preimage],
            [edge_digest],
            [edge_preimage],
            [mask_digest],
            [mask_preimage],
            [hall_digest],
            [hall_preimage],
            logical_digest,
            logical_preimage,
            output,
            work_digest,
            patch_preimage,
        ),
    )
    assert installed.execute(
        "SELECT count(*) FROM groundloop_m5_matching_patch_artifact"
    ).fetchone() == (1,)
    installed.commit()

    deleted_observation = _sequence(
        _typed("enum", "current"),
        _typed("text", "o"),
        _typed("text", "r"),
        _typed("text", "g"),
        _typed("int", 0),
        _typed("sha256", h1),
        _typed("int", 1),
        _typed("int", 0),
    )
    missing_after_preimage = _change_bytes(
        "m5-persisted-matching-observation-change-v1",
        (_typed("text", "o"),),
        deleted_observation,
        ("null",),
    )
    missing_after_digest = hashlib.sha256(missing_after_preimage).hexdigest()
    missing_after_patch_preimage = _framed_preimage(
        "m5-persisted-matching-patch-v1",
        *_typed("enum", "direct_transition"),
        *_typed("text", "source"),
        *_typed("sha256", h1),
        *_typed("int", 2),
        *_typed("int", 3),
        *_typed("int", 2),
        *_typed("int", 4),
        *_typed("text", "policy"),
        *_typed("sha256", shape_digest),
        *_sequence(_typed("sha256", missing_after_digest)),
        *_sequence(_typed("sha256", edge_digest)),
        *_sequence(_typed("sha256", mask_digest)),
        *_sequence(_typed("sha256", hall_digest)),
        *_typed("sha256", logical_digest),
        *_typed("sha256", work_digest),
    )
    missing_after_patch_digest = hashlib.sha256(
        missing_after_patch_preimage
    ).hexdigest()
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="physical after point disagrees with patch result",
    ):
        installed.execute(
            """UPDATE groundloop_m5_matching_patch_artifact
               SET patch_digest=%s,
                   observation_change_digests=%s,
                   observation_change_preimages=%s,
                   canonical_patch_preimage=%s
               WHERE patch_digest=%s""",
            (
                missing_after_patch_digest,
                [missing_after_digest],
                [missing_after_preimage],
                missing_after_patch_preimage,
                patch_digest,
            ),
        )
    installed.rollback()

    logical_only_preimage = _framed_preimage(
        "m5-persisted-matching-patch-v1",
        *_typed("enum", "direct_transition"),
        *_typed("text", "logical-source"),
        *_typed("sha256", h1),
        *_typed("int", 2),
        *_typed("int", 3),
        *_typed("int", 2),
        *_typed("int", 4),
        *_typed("text", "policy"),
        *_typed("sha256", shape_digest),
        *_sequence(),
        *_sequence(),
        *_sequence(),
        *_sequence(),
        *_typed("sha256", logical_digest),
        *_typed("sha256", work_digest),
    )
    logical_only_digest = hashlib.sha256(logical_only_preimage).hexdigest()
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    installed.execute(
        """INSERT INTO groundloop_m5_matching_patch_artifact
           SELECT %s,source_kind,%s,source_identity_hash,before_epoch_id,
             before_revision,resulting_epoch_id,resulting_revision,
             decision_policy_version,group_shape_set_digest,
             group_shape_set_preimage,ARRAY[]::char(64)[],ARRAY[]::bytea[],
             ARRAY[]::char(64)[],ARRAY[]::bytea[],ARRAY[]::char(64)[],
             ARRAY[]::bytea[],ARRAY[]::char(64)[],ARRAY[]::bytea[],
             logical_overlay_patch_digest,logical_overlay_patch_preimage,
             logical_output_preimage,matching_work_digest,%s
           FROM groundloop_m5_matching_patch_artifact WHERE patch_digest=%s""",
        (logical_only_digest, "logical-source", logical_only_preimage, patch_digest),
    )
    installed.commit()
    empty_shape = _framed_preimage(
        "m5-persisted-matching-group-shape-set-v1", *_sequence()
    )
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    with pytest.raises(psycopg.errors.RaiseException, match="logical change lacks"):
        installed.execute(
            """UPDATE groundloop_m5_matching_patch_artifact
               SET group_shape_set_digest=%s,group_shape_set_preimage=%s
               WHERE patch_digest=%s""",
            (hashlib.sha256(empty_shape).hexdigest(), empty_shape, logical_only_digest),
        )
    installed.rollback()

    mutations: tuple[tuple[str, tuple[Any, ...], str], ...] = (
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET edge_change_digests=%s WHERE patch_digest=%s",
            ([h1], patch_digest),
            "edge child mismatch",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET edge_change_preimages=%s WHERE patch_digest=%s",
            ([], patch_digest),
            "groundloop_m5_matching_patch_artifact_check1",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "observation_change_digests=%s,observation_change_preimages=%s "
            "WHERE patch_digest=%s",
            (
                [change_digest, change_digest],
                [change_preimage, change_preimage],
                patch_digest,
            ),
            "sorted unique",
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact "
            "SET source_id='changed' WHERE patch_digest=%s",
            (patch_digest,),
            "canonical patch preimage mismatch",
        ),
    )
    for statement, parameters, message in mutations:
        installed.execute(
            "SELECT set_config('groundloop.m5_checked_transition','on',true)"
        )
        installed.execute(
            "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
        )
        with pytest.raises(psycopg.Error, match=message):
            installed.execute(statement, parameters)
        installed.rollback()

    array_shape_mutations: tuple[tuple[str, tuple[Any, ...]], ...] = (
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "observation_change_digests="
            "array_fill(%s::char(64),ARRAY[1],ARRAY[0]),"
            "observation_change_preimages="
            "array_fill(%s::bytea,ARRAY[1],ARRAY[0]) "
            "WHERE patch_digest=%s",
            (change_digest, change_preimage, patch_digest),
        ),
        (
            "UPDATE groundloop_m5_matching_patch_artifact SET "
            "observation_change_digests=ARRAY[NULL::char(64)],"
            "observation_change_preimages=ARRAY[%s::bytea] "
            "WHERE patch_digest=%s",
            (change_preimage, patch_digest),
        ),
    )
    for statement, parameters in array_shape_mutations:
        installed.execute(
            "SELECT set_config('groundloop.m5_checked_transition','on',true)"
        )
        installed.execute(
            "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
        )
        with pytest.raises(
            psycopg.errors.RaiseException,
            match=(
                "persisted matching child arrays require canonical elements and bounds"
            ),
        ):
            installed.execute(statement, parameters)
        installed.rollback()
        assert installed.execute(
            """SELECT array_lower(observation_change_digests,1),
                      array_lower(observation_change_preimages,1),
                      array_position(observation_change_digests,NULL),
                      array_position(observation_change_preimages,NULL)
                 FROM groundloop_m5_matching_patch_artifact
                WHERE patch_digest=%s""",
            (patch_digest,),
        ).fetchone() == (1, 1, None, None)

    counter_columns = [
        str(row[0])
        for row in installed.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema=current_schema()
                 AND table_name='groundloop_m5_matching_work_contribution'
                 AND ordinal_position BETWEEN 9 AND 45
               ORDER BY ordinal_position"""
        ).fetchall()
    ]
    assert len(counter_columns) == 37
    contribution_columns = [
        "epoch_id",
        "source_kind",
        "source_id",
        "source_identity_hash",
        "before_epoch_id",
        "before_revision",
        "resulting_revision",
        "patch_digest",
        *counter_columns,
        "matching_work_digest",
        "contribution_digest",
    ]
    contribution_insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier("groundloop_m5_matching_work_contribution"),
        sql.SQL(",").join(map(sql.Identifier, contribution_columns)),
        sql.SQL(",").join(sql.Placeholder() for _ in contribution_columns),
    )
    contribution_values = [
        2,
        "direct_transition",
        "source",
        h1,
        2,
        3,
        4,
        patch_digest,
        *work_values,
        work_digest,
        "0" * 64,
    ]
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    with pytest.raises(psycopg.errors.RaiseException, match="contribution digest"):
        installed.execute(contribution_insert, contribution_values)
    installed.rollback()

    contribution_values[-1] = contribution_digest
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        installed.execute(contribution_insert, contribution_values)
    installed.rollback()

    invalid_structural_preimage = _framed_preimage(
        "m5-matching-work-contribution-v1",
        *_typed("int", 2),
        *_typed("enum", "structural_open"),
        *_typed("text", "structural"),
        *_typed("sha256", h1),
        *_typed("int", 2),
        *_typed("int", 0),
        *_typed("int", 1),
        *_typed("sha256", patch_digest),
        *_typed("sha256", work_digest),
    )
    invalid_structural_values = [
        2,
        "structural_open",
        "structural",
        h1,
        2,
        0,
        1,
        patch_digest,
        *work_values,
        work_digest,
        hashlib.sha256(invalid_structural_preimage).hexdigest(),
    ]
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    with pytest.raises(psycopg.errors.RaiseException, match="point law"):
        installed.execute(contribution_insert, invalid_structural_values)
    installed.rollback()

    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode','migration',true)"
    )
    installed.execute("DELETE FROM groundloop_m5_matching_patch_artifact")
    installed.execute(
        "DELETE FROM groundloop_decision_policy WHERE policy_version='policy'"
    )
    installed.execute("DELETE FROM groundloop_epoch WHERE epoch_id IN (1,2)")
    installed.commit()


@pytest.mark.parametrize(
    ("mode", "statement", "message"),
    (
        (
            "transition",
            "INSERT INTO groundloop_m5_matching_image_current VALUES (true,'x',1,1)",
            "transition lacks private context",
        ),
        (
            "seal",
            "INSERT INTO groundloop_m5_matching_image_working VALUES (1,1,1,'x',1)",
            "promotion context mismatch",
        ),
        (
            "transition",
            "INSERT INTO groundloop_m5_matching_image_working VALUES (2,1,1,'x',1)",
            "transition lacks private context",
        ),
    ),
)
def test_guard_rejects_cross_mode_and_cross_epoch(
    installed: Connection[Any], mode: str, statement: str, message: str
) -> None:
    installed.execute("SELECT set_config('groundloop.m5_checked_transition','on',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_mode',%s,true)", (mode,)
    )
    installed.execute("SELECT set_config('groundloop.m5_matching_epoch_id','1',true)")
    installed.execute(
        "SELECT set_config('groundloop.m5_matching_resulting_revision','1',true)"
    )
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        installed.execute(statement)
    installed.rollback()


def test_mid_group_failure_rolls_back_and_new_transaction_retries() -> None:
    with _pre017_schema() as (connection, _):

        def fail(point: str) -> None:
            if point == "after_image_schema":
                raise RuntimeError("injected migration-017 failure")

        with pytest.raises(RuntimeError, match="injected"):
            install_m5_persisted_matching_bundle(connection, failure_injector=fail)
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        connection.commit()
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()


def test_nowait_partial_prefix_releases_and_fresh_retry_succeeds() -> None:
    with _pre017_schema() as (connection, schema_name):
        with psycopg.connect(_database_url()) as blocker:
            blocker.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            blocker.execute("LOCK TABLE groundloop_epoch IN ACCESS SHARE MODE")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                install_m5_persisted_matching_bundle(connection)
            blocker.rollback()
        assert connection.execute(
            "SELECT to_regclass('groundloop_m5_matching_image_current')"
        ).fetchone() == (None,)
        connection.commit()
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()


def test_failure_injection_is_atomic() -> None:
    source = Path(M5_PERSISTED_MATCHING_MIGRATION_PATH).read_bytes()
    assert b"before_ledger" not in source
