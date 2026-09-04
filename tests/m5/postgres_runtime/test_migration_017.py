"""Contract and live PostgreSQL tests for M5-D25 migration 017."""

from __future__ import annotations

import hashlib
import os
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import Connection, IsolationLevel, sql
from psycopg.types.json import Jsonb

from groundloop.m5.digests import (
    bool_field,
    enum_field,
    hash_field,
    int_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.domain import ClaimCertificateArtifact, ClaimSupportKind
from groundloop.postgres.migrations import (
    M5_ACCEPTED_RECOVERY_BUNDLE_ID,
    M5_ACCEPTED_RECOVERY_BUNDLE_SHA256,
    M5_ACCEPTED_RECOVERY_MIGRATION_SHA256,
    M5_ACCEPTED_RECOVERY_ORACLE_SHA256,
    M5_ACCEPTED_RECOVERY_PREREQUISITE_SHA256,
    M5_PERSISTED_MATCHING_BUNDLE_ID,
    M5_PERSISTED_MATCHING_INSTALL_LOCK_RELATIONS,
    M5_PERSISTED_MATCHING_MIGRATION_PATH,
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
                    "ORDER BY schema_name"
                )
            )
            assert after == before


def _seed_b2_runtime_base(connection: Connection[Any], prefix: str) -> tuple[int, str]:
    def digest(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    base_row = connection.execute(
        """INSERT INTO groundloop_epoch
           (event_id,payload_hash,revision,structural_status,semantic_status,
            evaluation_state,publication_mode,sealed_at)
           VALUES (%s,%s,0,'committed','sealed','complete','strict',now())
           RETURNING epoch_id""",
        (f"{prefix}-base", digest(f"{prefix}-base")),
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
        "SELECT groundloop_m5_authorize_persisted_matching_activation(%s,0,%s)",
        (base, policy),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_matching_image_current VALUES (true,%s,%s,0)",
        (policy, base),
    )
    connection.execute(
        "INSERT INTO groundloop_m5_publication_head VALUES (true,%s,0,now())", (base,)
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
) -> tuple[int, str, str]:
    payload = hashlib.sha256(f"{prefix}-payload".encode()).hexdigest()
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
        (epoch, "document_insert" if direct_bridge else "policy_change", base, policy),
    )
    manifest_row = connection.execute(
        "SELECT candidate_policy_id FROM groundloop_m5_candidate_policy"
    ).fetchone()
    assert manifest_row is not None
    manifest = str(manifest_row[0])
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
               VALUES (%s,'insert',%s,%s,%s,'{}')""",
            (epoch, manifest, base, str(registry_row[0])),
        )
    return epoch, event, payload


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


def _apply_empty_b2_structural(
    connection: Connection[Any],
    *,
    epoch: int,
    event: str,
    payload: str,
    base: int,
    policy: str,
    mutate: str | None = None,
    source_kind: str = "structural_open",
    hall_group: tuple[str, str] | None = None,
) -> None:
    is_structural = source_kind == "structural_open"
    before_epoch = base if is_structural else epoch
    before_revision = 0 if is_structural else 1
    resulting_revision = 1 if is_structural else 2
    output = bytes.fromhex(
        "710000000000000002000000000000002573000000000000001c"
        "6d352d6f7665726c61792d6c6f676963616c2d6f75747075742d7632"
        "0000000000000009710000000000000000"
    )
    hall_preimage: bytes | None = None
    if hall_group is None:
        shapes = _framed_preimage(
            "m5-persisted-matching-group-shape-set-v1", *_sequence()
        )
    else:
        group_id, requirement_id = hall_group
        shapes = _framed_preimage(
            "m5-persisted-matching-group-shape-set-v1",
            *_sequence(
                _sequence(
                    _typed("text", group_id),
                    _typed("int", 1),
                    _sequence(
                        _sequence(_typed("int", 0), _typed("text", requirement_id))
                    ),
                )
            ),
        )
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
    logical = _framed_preimage(
        "m5-persisted-logical-overlay-patch-v1",
        *_sequence(),
        *_sequence(),
        *_typed("sha256", hashlib.sha256(output).hexdigest()),
        *_typed("int", len(output)),
    )
    work = [0] * 37
    if hall_preimage is not None:
        work[11] = 1
        work[12] = 1
        work[14] = 1
    work[30] = len(output)
    if mutate == "output_bytes":
        work[30] += 1
    work_preimage = _framed_preimage(
        "m5-matching-work-v1",
        *(field for value in work for field in _typed("int", value)),
    )
    work_digest = hashlib.sha256(work_preimage).hexdigest()
    patch_preimage = _framed_preimage(
        "m5-persisted-matching-patch-v1",
        *_typed("enum", source_kind),
        *_typed("text", event),
        *_typed("sha256", payload),
        *_typed("int", before_epoch),
        *_typed("int", before_revision),
        *_typed("int", epoch),
        *_typed("int", resulting_revision),
        *_typed("text", policy),
        *_typed("sha256", hashlib.sha256(shapes).hexdigest()),
        *_sequence(),
        *_sequence(),
        *_sequence(),
        *_sequence(
            *(
                ()
                if hall_preimage is None
                else (_typed("sha256", hashlib.sha256(hall_preimage).hexdigest()),)
            )
        ),
        *_typed("sha256", hashlib.sha256(logical).hexdigest()),
        *_typed("sha256", work_digest),
    )
    patch_digest = hashlib.sha256(patch_preimage).hexdigest()
    contribution_preimage = _framed_preimage(
        "m5-matching-work-contribution-v1",
        *_typed("int", epoch),
        *_typed("enum", source_kind),
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
        "SELECT groundloop_m5_authorize_checked_transition(%s,1)", (epoch,)
    )
    connection.execute(
        "SELECT groundloop_m5_authorize_persisted_matching_transition(%s,1,%s,%s,%s)",
        (epoch, resulting_revision, source_kind, event),
    )
    if not is_structural:
        connection.execute(
            "UPDATE groundloop_epoch SET revision=2 WHERE epoch_id=%s", (epoch,)
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch "
            "SET runtime_state='semantic_pending',revision=2 WHERE epoch_id=%s",
            (epoch,),
        )
    if mutate == "clear_mode":
        connection.execute("SELECT set_config('groundloop.m5_matching_mode','',true)")
    if is_structural:
        connection.execute(
            "INSERT INTO groundloop_m5_matching_image_working VALUES (%s,%s,0,%s,1)",
            (epoch, base, policy),
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
                "SET updated_revision=2 WHERE epoch_id=%s",
                (epoch,),
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
    connection.execute(
        """INSERT INTO groundloop_m5_matching_patch_artifact VALUES
           (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            ARRAY[]::char(64)[],ARRAY[]::bytea[],ARRAY[]::char(64)[],ARRAY[]::bytea[],
            ARRAY[]::char(64)[],ARRAY[]::bytea[],%s,%s,
            %s,%s,%s,%s,%s)""",
        (
            patch_digest,
            source_kind,
            event,
            payload,
            before_epoch,
            before_revision,
            epoch,
            resulting_revision,
            policy,
            hashlib.sha256(shapes).hexdigest(),
            shapes,
            []
            if hall_preimage is None
            else [hashlib.sha256(hall_preimage).hexdigest()],
            [] if hall_preimage is None else [hall_preimage],
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
            source_kind,
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
        accumulated_work = [value * 2 for value in accumulator_work]
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
    connection: Connection[Any], epoch: int, prefix: str
) -> tuple[str, str]:
    transition_id = f"{prefix}-transition"
    transition_hash = hashlib.sha256(transition_id.encode()).hexdigest()
    connection.execute(
        """INSERT INTO groundloop_m4_evaluation_epoch_counter
           VALUES (%s,%s,'active','complete',NULL,0,1)""",
        (epoch, hashlib.sha256(f"{prefix}-declaration".encode()).hexdigest()),
    )
    connection.execute(
        """INSERT INTO groundloop_m4_evaluation_counter_transition
           (epoch_id,transition_id,payload_hash,transition_kind,
            from_revision,to_revision,override_rows_written)
           VALUES (%s,%s,%s,'delta',1,2,0)""",
        (epoch, transition_id, transition_hash),
    )
    return transition_id, transition_hash


@pytest.fixture(scope="module")
def installed() -> Iterator[Connection[Any]]:
    schema_name = f"d25_migration_017_{uuid.uuid4().hex}"
    with psycopg.connect(_database_url(), autocommit=True) as admin:
        before = tuple(
            row[0]
            for row in admin.execute(
                "SELECT schema_name FROM information_schema.schemata "
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


def test_b2_nonowner_private_helpers_denied_public_path_succeeds() -> None:
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
            "cannot mutate non-current",
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
