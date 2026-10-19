"""Independent source-fold plus exhaustive working-image differential tests."""

from __future__ import annotations

import re
from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from groundloop.domain import (
    AnswerVersion,
    ChunkVersion,
    Claim,
    DecisionPolicy,
    DocumentVersion,
    ModelStamp,
    Question,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import InvalidEventError
from groundloop.m5.reference import (
    compute_reference_states,
    derive_active_requirement_witnesses,
)
from groundloop.m5.repository import M5Repository
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.repository import InMemoryRepository
from tests.m5.postgres_runtime.test_semantic_readiness import _edge


def _controlled_new_only_registration_seal(
    connection: Connection[Any],
    plan: Any,
    epoch: int,
    revision: int,
    cut: Any = lambda _: None,
) -> Any:
    """Bounded SQL terminal adapter, never a production/public seal route.

    Uses the reported real readiness point; preserves cumulative work and
    coverage. Records actual state-row writes and explicit S/K producer bytes.
    Other Python child validation/SQL hashing is not benchmarked or promoted
    as comprehensive C1 producer instrumentation by this diagnostic fixture.
    """
    import hashlib

    from groundloop.m4.application import OpenEventReceipt, PublicationReceipt
    from groundloop.m4.contracts import stable_m4_digest
    from groundloop.m5.digests import enum_field, hash_field, int_field, text_field
    from groundloop.m5.runtime.contracts import M5EventRunResult, M5RunState
    from groundloop.m5.runtime.persistence import _insert_runtime_work
    from groundloop.m5.runtime.postgres_matching_publication import (
        _prepare_preterminal_matching_publication_children,
        build_matching_publication_children,
        promote_matching_overlay,
    )
    from groundloop.m5.runtime.postgres_readiness import _lock_accounting
    from groundloop.m5.runtime.postgres_recovery import _TIMING_COVERAGE_COLUMNS

    sealed = revision + 1
    with connection.transaction(), connection.cursor() as cursor:
        schema = cursor.execute("SELECT pg_catalog.current_schema()").fetchone()[0]
        for table in (
            "groundloop_runtime_mode",
            "groundloop_m4_publication_head",
            "groundloop_m5_publication_head",
        ):
            cursor.execute(
                sql.SQL("SELECT * FROM {} WHERE singleton FOR UPDATE").format(
                    sql.Identifier(schema, table)
                )
            ).fetchone()
        base = cursor.execute(
            "SELECT revision,semantic_status FROM groundloop_epoch "
            "WHERE epoch_id=%s FOR UPDATE",
            (epoch,),
        ).fetchone()
        cut("after_base_lock")
        runtime = cursor.execute(
            "SELECT revision,runtime_state FROM "
            "groundloop_m5_runtime_epoch WHERE epoch_id=%s FOR UPDATE",
            (epoch,),
        ).fetchone()
        if base != (revision, "complete") or runtime != (revision, "semantic_complete"):
            raise InvalidEventError("SQL adapter requires genuine semantic readiness")
        for table, key in (
            ("groundloop_m5_owner_pending_counter", "owner_claim_id"),
            ("groundloop_m5_answer_pending_counter", "answer_version_id"),
        ):
            cursor.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE epoch_id=%s "
                    'ORDER BY {} COLLATE "C" FOR UPDATE'
                ).format(sql.Identifier(schema, table), sql.Identifier(key)),
                (epoch,),
            ).fetchall()
        prior, timing = _lock_accounting(cursor, schema, epoch, revision)
        if timing.has_pending_anchor:
            raise InvalidEventError(
                "SQL fixture requires separately reported readiness"
            )
        for family, keys, positive in (
            ("observation", ("observation_id",), "present"),
            ("edge", ("requirement_version_id", "text_hash"), "refcount>0"),
            ("hash_mask", ("group_version_id", "text_hash"), "mask>0"),
            ("hall", ("group_version_id",), "present"),
        ):
            join = " AND ".join(f"c.{key}=w.{key}" for key in keys)
            assert cursor.execute(
                f"SELECT count(*) FROM groundloop_m5_matching_{family}_working w "
                f"JOIN groundloop_m5_matching_{family}_current c ON {join} "
                f"WHERE w.epoch_id=%s AND w.{positive}",
                (epoch,),
            ).fetchone() == (0,)
        promotion = promote_matching_overlay(
            cursor, epoch_id=epoch, expected_revision=revision, sealed_revision=sealed
        )
        assert (
            promotion.observation_writes,
            promotion.edge_writes,
            promotion.mask_writes,
            promotion.hall_writes,
        ) == (1, 1, 1, 1)
        journal = cursor.execute(
            "SELECT relation_name,first_old,first_operation,last_operation,mutati"
            "on_count,"
            "saw_insert,saw_update,saw_delete FROM "
            "pg_temp.groundloop_m5_matching_promotion_journal "
            "WHERE relation_name=ANY(%s)",
            (
                [
                    f"groundloop_m5_matching_{family}_current"
                    for family in ("observation", "edge", "hash_mask", "hall")
                ],
            ),
        ).fetchall()
        assert len(journal) == 4
        assert all(
            tuple(row[1:]) == (None, "INSERT", "INSERT", 1, True, False, False)
            for row in journal
        )
        # Final currency includes non-support returns; matching positives do not.
        holders = cursor.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id,"
            "valid_from_revision FROM groundloop_m5_working_currency_history "
            "WHERE epoch_id=%s AND valid_to_revision IS NULL ORDER BY "
            "subject_id,chunk_version_id",
            (epoch,),
        ).fetchall()
        assert len(holders) == 3 and all(row[4] is not None for row in holders)
        for holder in holders:
            assert (
                cursor.execute(
                    "SELECT 1 FROM groundloop_observation_currency WHERE "
                    "subject_kind=%s "
                    "AND subject_id=%s AND chunk_version_id=%s AND task_type=%s",
                    holder[:4],
                ).fetchone()
                is None
            )
            assert (
                cursor.execute(
                    "SELECT 1 FROM groundloop_published_observation_currency "
                    "WHERE subject_kind=%s "
                    "AND subject_id=%s AND chunk_version_id=%s AND task_type=%s",
                    holder[:4],
                ).fetchone()
                is None
            )
            execution = cursor.execute(
                "SELECT eligible_for_currency,produced_epoch_id FROM "
                "groundloop_m5_requirement_verifier_execution "
                "WHERE observation_id=%s",
                (holder[4],),
            ).fetchone()
            assert execution == (True, epoch)
            cursor.execute(
                "INSERT INTO groundloop_observation_currency ("
                "subject_kind,subject_id,chunk_version_id,task_type,observation_"
                "id,installed_revision) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                tuple(holder),
            )
            cursor.execute(
                "INSERT INTO groundloop_published_observation_currency ("
                "subject_kind,subject_id,chunk_version_id,task_type,observation_"
                "id,valid_from_epoch,valid_to_epoch) "
                "VALUES (%s,%s,%s,%s,%s,%s,NULL)",
                (*holder[:5], epoch),
            )
        cut("after_genuine_requirement_currency")
        group = plan.event.group
        cursor.execute(
            "UPDATE groundloop_m5_group_family SET lifecycle_state='PUBLISHED' "
            "WHERE group_family_id=%s",
            (group.group_family_id,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_group_version SET lifecycle_state='PUBLISHED' "
            "WHERE group_version_id=%s",
            (group.group_version_id,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_requirement_version SET "
            "lifecycle_state='PUBLISHED' WHERE group_version_id=%s",
            (group.group_version_id,),
        )
        cursor.execute(
            "INSERT INTO groundloop_m5_group_validity "
            "(group_version_id,group_family_id,claim_id,semantic_structure_hash,"
            "supersedes_group_version_id,valid_from_epoch,valid_to_epoch) "
            "VALUES (%s,%s,%s,%s,NULL,%s,NULL)",
            (
                group.group_version_id,
                group.group_family_id,
                group.owner_claim_id,
                group.semantic_structure_hash,
                epoch,
            ),
        )
        state_write_counts = {}
        for kind, _key in (
            ("requirement", "requirement_version_id"),
            ("group", "group_version_id"),
        ):
            working = "groundloop_m5_working_" + kind + "_state"
            columns = [
                item[0]
                for item in cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s "
                    "AND column_name NOT IN ('epoch_id','updated_revision') "
                    "ORDER BY ordinal_position",
                    (schema, working),
                ).fetchall()
            ]
            col = sql.SQL(",").join(map(sql.Identifier, columns))
            count = cursor.execute(
                sql.SQL(
                    "INSERT INTO {} "
                    "({},valid_from_epoch,valid_to_epoch,sealed_revision) "
                    "SELECT {},%s,NULL,%s FROM {} WHERE epoch_id=%s"
                ).format(
                    sql.Identifier(
                        schema, "groundloop_m5_published_" + kind + "_state"
                    ),
                    col,
                    col,
                    sql.Identifier(schema, working),
                ),
                (epoch, sealed, epoch),
            ).rowcount
            count += cursor.execute(
                sql.SQL(
                    "INSERT INTO {} ({},updated_epoch,updated_revision) "
                    "SELECT {},%s,%s FROM {} WHERE epoch_id=%s"
                ).format(
                    sql.Identifier(
                        schema, "groundloop_m5_" + kind + "_state_materialized"
                    ),
                    col,
                    col,
                    sql.Identifier(schema, working),
                ),
                (epoch, sealed, epoch),
            ).rowcount
            state_write_counts[kind] = count
        cursor.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (epoch,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_publication_head SET epoch_id=%s,"
            "sealed_revision=%s WHERE singleton",
            (epoch, sealed),
        )
        cursor.execute(
            "UPDATE groundloop_epoch SET revision=%s,semantic_status='sealed',"
            "evaluation_state='complete',publication_mode='strict',"
            "sealed_at=clock_timestamp() WHERE epoch_id=%s AND revision=%s",
            (sealed, epoch, revision),
        )
        children = _prepare_preterminal_matching_publication_children(
            cursor,
            epoch_id=epoch,
            expected_revision=revision,
            sealed_revision=sealed,
        )
        assert not children.combined_deltas
        publication = stable_m4_digest("m4-publication-v1", str(epoch))
        combined = digests.combined_status_delta_set_digest(children.combined_deltas)
        changed = digests.changed_state_set_digest(
            ref.reference_digest for ref in children.changed_state_references
        )
        source_bytes = digests.stable_m5_preimage(
            "m5-seal-contribution-source-v1",
            text_field(plan.structural_event_id),
            hash_field(combined),
            hash_field(changed),
            text_field(publication),
        )
        source = hashlib.sha256(source_bytes).hexdigest()
        key_bytes = digests.stable_m5_preimage(
            "m5-runtime-work-contribution-key-v1",
            int_field(epoch),
            enum_field("seal"),
            text_field(plan.structural_event_id),
        )
        key = hashlib.sha256(key_bytes).hexdigest()
        work = M5RuntimeWork(
            group_state_write_count=state_write_counts["group"],
            bytes_hashed=len(source_bytes) + len(key_bytes),
            bytes_serialized=len(source_bytes) + len(key_bytes),
        )
        counts: dict[str, Any] = dict(
            zip(
                prior.counter_names(),
                (
                    a + b
                    for a, b in zip(
                        prior.counter_values(), work.counter_values(), strict=True
                    )
                ),
                strict=True,
            )
        )
        total = M5RuntimeWork(**counts)
        names = (
            "epoch_id",
            "contribution_kind",
            "source_id",
            "source_identity_hash",
            "contribution_key_digest",
            "applied_revision",
            *work.counter_names(),
            "work_digest",
        )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, names)),
                sql.SQL(",").join(sql.Placeholder() for _ in names),
            ),
            (
                epoch,
                "seal",
                plan.structural_event_id,
                source,
                key,
                sealed,
                *work.counter_values(),
                work.work_digest,
            ),
        )
        cut("after_contribution")
        cursor.execute(
            "UPDATE groundloop_m5_runtime_epoch SET revision=%s,"
            "runtime_state='sealed',terminal_at=clock_timestamp() WHERE epoch_id=%s "
            "AND revision=%s",
            (sealed, epoch, revision),
        )
        for table in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            cursor.execute(
                sql.SQL("UPDATE {} SET updated_revision=%s WHERE epoch_id=%s").format(
                    sql.Identifier(schema, table)
                ),
                (sealed, epoch),
            )
        assignments = sql.SQL(",").join(
            sql.SQL("{}=%s").format(sql.Identifier(name))
            for name in total.counter_names()
        )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_work_accumulator SET {},"
                    "work_digest=%s,updated_revision=%s,terminalized=true "
                    "WHERE epoch_id=%s "
                    "AND updated_revision=%s AND work_digest=%s AND NOT terminalized"
                ).format(assignments),
                (
                    *total.counter_values(),
                    total.work_digest,
                    sealed,
                    epoch,
                    revision,
                    prior.work_digest,
                ),
            ).rowcount
            == 1
        )
        coverage_updates = []
        for prefix in (
            "required",
            "postgres_server_execution",
            "postgres_lock_wait",
            "postgres_wal_bytes",
            "postgres_shared_block_reads",
        ):
            coverage_updates.extend(
                [
                    sql.SQL("{}={}+1").format(
                        sql.Identifier(prefix + "_expected_count"),
                        sql.Identifier(prefix + "_expected_count"),
                    ),
                    sql.SQL("{}={}+1").format(
                        sql.Identifier(prefix + "_missing_count"),
                        sql.Identifier(prefix + "_missing_count"),
                    ),
                ]
            )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_timing_accumulator SET {},"
                    "terminalized=true,updated_revision=%s,pending_contribution_k"
                    "ind=NULL,"
                    "pending_source_id=NULL,pending_contribution_key_digest=NULL,"
                    "pending_anchor_revision=NULL "
                    "WHERE epoch_id=%s AND updated_revision=%s AND NOT terminalized"
                ).format(sql.SQL(",").join(coverage_updates)),
                (sealed, epoch, revision),
            ).rowcount
            == 1
        )
        # current_event_timing owns a nested transaction and requests READ ONLY:
        # read the just-frozen point directly instead of relying on that wrapper.
        from groundloop.m5.runtime.postgres_recovery import (
            _TIMING_SUM_COLUMNS,
            _timing_accumulator_from_row,
        )

        returned = cursor.execute(
            "SELECT "
            + ",".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
            + ",updated_revision,terminalized,"
            "pending_contribution_kind,pending_source_id,pending_contribution_key"
            "_digest,"
            "pending_anchor_revision FROM groundloop_m5_runtime_timing_accumulator "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        event_timing, coverage = _timing_accumulator_from_row(returned).project(
            pending_as_missing=False
        )
        for kind, vector in (("event", total), ("call", work)):
            _insert_runtime_work(
                cursor,
                structural_event_id=plan.structural_event_id,
                epoch_id=epoch,
                work_kind=kind,
                work=vector,
            )
        result = M5EventRunResult.build(
            event_id=plan.structural_event_id,
            payload_hash=plan.payload_hash,
            epoch_id=epoch,
            state=M5RunState.SEALED,
            replayed_outcome=None,
            open_receipt=OpenEventReceipt(epoch, False, False),
            publication_receipt=PublicationReceipt(epoch, publication, False),
            event_work=total,
            call_work=work,
            event_timing=event_timing,
            call_timing=M5RuntimeTiming(),
            combined_deltas=children.combined_deltas,
            changed_state_references=children.changed_state_references,
            failure_reason=None,
            event_timing_coverage=coverage,
            call_timing_coverage=M5RuntimeTimingCoverage.single_point(
                timing=None, terminal_client_roundtrip_included=False
            ),
        )
        cursor.execute(
            "INSERT INTO groundloop_m5_event_result "
            "(structural_event_id,payload_hash,epoch_id,outcome,original_open_rec"
            "eipt_binding_hash,"
            "publication_id,original_publication_receipt_binding_hash,event_work_"
            "kind,event_work_digest,"
            "combined_status_delta_set_hash,changed_state_set_hash,failure_reason"
            ",logical_result_hash,"
            "delta_count,state_reference_count,coordinator_non_db_non_neural_ns,n"
            "eural_wall_ns,"
            "postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns,"
            "postgres_server_execution_ns,postgres_lock_wait_ns,postgres_wal_bytes,"
            "postgres_shared_block_reads) "
            "VALUES (%s,%s,%s,'sealed',%s,%s,%s,'event',%s,%s,%s,NULL,%s,%s,%s,"
            + ",".join(["%s"] * 9)
            + ")",
            (
                plan.structural_event_id,
                plan.payload_hash,
                epoch,
                digests.open_event_receipt_binding_digest(
                    epoch_id=epoch,
                    replayed=False,
                    already_sealed=False,
                    publication_id=None,
                    already_failed=False,
                    failure_reason=None,
                ),
                publication,
                digests.publication_receipt_binding_digest(
                    epoch_id=epoch, publication_id=publication, replayed=False
                ),
                total.work_digest,
                combined,
                changed,
                result.logical_result_hash,
                len(children.combined_deltas),
                len(children.changed_state_references),
                *(getattr(event_timing, name) for name in _TIMING_SUM_COLUMNS),
            ),
        )
        assert (
            build_matching_publication_children(
                cursor, epoch_id=epoch, sealed_revision=sealed
            )
            == children
        )
        for ordinal, ref in enumerate(children.changed_state_references):
            cursor.execute(
                "INSERT INTO groundloop_m5_event_result_state_reference VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan.structural_event_id,
                    ordinal,
                    ref.kind.value,
                    ref.object_id,
                    ref.epoch_id,
                    ref.revision,
                    ref.state_artifact_hash,
                    ref.reference_digest,
                ),
            )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_event_timing_coverage "
                "(structural_event_id,epoch_id,{},terminal_client_roundtrip_included) "
                "VALUES "
                "(%s,%s,{},false)"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, _TIMING_COVERAGE_COLUMNS)),
                sql.SQL(",").join(sql.Placeholder() for _ in _TIMING_COVERAGE_COLUMNS),
            ),
            (
                plan.structural_event_id,
                epoch,
                *(getattr(coverage, name) for name in _TIMING_COVERAGE_COLUMNS),
            ),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        cut("after_constraints")
        return result


def expected_currency_from_sources(db: Any) -> dict[tuple[str, ...], str | None]:
    expected: dict[tuple[str, ...], str | None] = {}
    for fixture, _, receipt in db.calls:
        if receipt.exact_replay:
            continue
        observation = fixture.verifier_artifact.to_semantic_observation()
        expected[
            (
                "requirement",
                observation.subject_id,
                observation.chunk_version_id,
                observation.task_type,
            )
        ] = observation.observation_id
    return expected


def assert_currency_sources_and_raw_history(
    db: Any, expected: dict[tuple[str, ...], str | None]
) -> None:
    epoch, revision = db.runtime.epoch_id, db.revision()
    # Independent history fold: neither materialized targets nor native resolver.
    folded = {
        tuple(r[:4]): r[4]
        for r in db.connection.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_published_observation_currency WHERE valid_from_epoch<=%s "
            "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
            (db.base.epoch_id, db.base.epoch_id),
        ).fetchall()
    }
    for row in db.connection.execute(
        "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id,"
        "valid_from_revision,valid_to_revision FROM "
        "groundloop_m5_working_currency_history "
        "WHERE epoch_id=%s AND valid_from_revision<=%s ORDER BY valid_from_revision",
        (epoch, revision),
    ).fetchall():
        if row[6] is None or revision < int(row[6]):
            folded[tuple(row[:4])] = row[4]
    native = {
        tuple(r[:4]): r[4]
        for r in db.connection.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_m5_currency_at(%s,%s)",
            (epoch, revision),
        ).fetchall()
    }
    assert folded == native == expected
    assert db.connection.execute(
        "SELECT requirement_version_id,ordinal,requirement_text,btrim(requirement"
        "_text_hash) "
        "FROM groundloop_m5_requirement_version WHERE group_version_id=%s ORDER "
        "BY ordinal",
        (db.group.group_version_id,),
    ).fetchall() == [
        (
            r.requirement_version_id,
            r.ordinal,
            r.requirement_text,
            r.requirement_text_hash,
        )
        for r in db.group.requirements
    ]


def test_independent_currency_and_full_python_sql_images_after_each_commit(
    c2_db: Any,
) -> None:
    db = c2_db
    for chunk, label in zip(db.chunks, ("SUPPORT", "NEUTRAL", "REFUTE"), strict=True):
        db.complete(db.build(chunk, label))
        expected = expected_currency_from_sources(db)
        assert_currency_sources_and_raw_history(db, expected)
        _assert_complete_point(db, db.runtime.epoch_id, db.revision(), expected)
    assert len(expected) == 3
    assert db.connection.execute(
        "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
        "maximum_deficiency,matching_size,distinct_hash_count "
        "FROM groundloop_m5_matching_hall_working WHERE epoch_id=%s AND "
        "group_version_id=%s",
        (db.runtime.epoch_id, db.group.group_version_id),
    ).fetchone() == (2, [0, 1, 0, 0], [0, 1, 0, 1], [0, 0, 1, 1], 1, 1, 1)


def _seal_genuine_baseline(c2_db: Any) -> None:
    db = c2_db
    for chunk, label in zip(db.chunks, ("SUPPORT", "NEUTRAL", "NEUTRAL"), strict=True):
        db.complete(db.build(chunk, label))
    expected = expected_currency_from_sources(db)
    assert_currency_sources_and_raw_history(db, expected)
    _assert_complete_point(db, db.runtime.epoch_id, db.revision(), expected)
    ready = _edge(
        db.connection,
        db.runtime.epoch_id,
        db.runtime.plan.structural_event_id,
        "semantic_complete",
        db.revision(),
    )
    store = PostgresM5RuntimeStore(db.connection)
    store.append_transition_call_timing(
        db.runtime.epoch_id,
        ready.anchor.contribution_kind,
        ready.anchor.source_id,
        ready.anchor.contribution_key_digest,
        ready.anchor.anchor_revision,
        None,
    )
    result = _controlled_new_only_registration_seal(
        db.connection,
        db.runtime.plan,
        db.runtime.epoch_id,
        ready.anchor.anchor_revision,
    )
    assert result.state.value == "sealed"
    with db.runtime.reconnect() as reader:
        assert reader.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_observation_currency ORDER BY subject_id,chunk_version_id"
        ).fetchall() == sorted((*key, value) for key, value in expected.items())
        assert reader.execute(
            "SELECT installed_epoch_id,installed_revision FROM "
            "groundloop_m5_matching_image_current"
        ).fetchone() == (result.epoch_id, ready.anchor.anchor_revision + 1)
    from dataclasses import replace

    db.base = replace(db.base, epoch_id=result.epoch_id)
    _assert_complete_point(
        db, result.epoch_id, ready.anchor.anchor_revision + 1, expected
    )
    replay = _edge(
        db.connection,
        result.epoch_id,
        db.runtime.plan.structural_event_id,
        "semantic_complete",
        ready.anchor.anchor_revision - 1,
    )
    assert replay.replay and replay.anchor == ready.anchor


def test_genuine_new_only_register_baseline_seals_currency_and_full_images(
    c2_db: Any,
) -> None:
    _seal_genuine_baseline(c2_db)


def _open_genuine_delete_fallback(
    db: Any,
    *,
    requirement_ordinal: int = 0,
    selected_chunks: tuple[str, ...] | None = None,
) -> None:
    """Real sealed predecessor -> DELETE -> native fallback child dispatch."""
    from groundloop.events import DeleteDocumentVersionEvent
    from groundloop.m4.application import ApplicationExecutionPolicy, DynamicEventPlan
    from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
    from groundloop.m4.pipeline import M4ExecutionMode, PostgresM4ApplicationPorts
    from groundloop.m5.events import legacy_event_payload_digest
    from groundloop.m5.runtime import postgres_withdrawal as withdrawal
    from groundloop.m5.runtime.contracts import (
        ActiveChunkSnapshot,
        ActiveChunkSnapshotEntry,
        M5AttemptOutput,
        M5DiscoveryDirection,
        M5ExecutionEvidenceDisposition,
        M5RequirementAdmissionChannel,
        M5RequirementChannelHit,
        M5RuntimeOperationalConfig,
        M5TypedEventPlan,
        SemanticPairKey,
    )
    from groundloop.m5.runtime.frontier import build_rank_interleaved_discovery_result
    from tests.m5.postgres.helpers import sha
    from tests.m5.postgres_runtime.d24_requirement.conftest import (
        D24RequirementDatabase,
    )
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        _build_c2_verifier_fixture,
    )
    from tests.m5.postgres_runtime.test_direct_m4_composition import REGISTRY_ID

    previous = db.runtime
    selected_chunks = db.chunks[:2] if selected_chunks is None else selected_chunks
    survivor_chunks = tuple(
        r[0]
        for r in db.connection.execute(
            "SELECT chunk_version_id FROM groundloop_chunk_version "
            "WHERE valid_to_epoch IS NULL "
            "AND NOT (chunk_version_id=ANY(%s)) ORDER BY chunk_version_id",
            (list(db.base.chunk_ids),),
        ).fetchall()
    )
    # Fresh cache is hydrated from the actual new sealed head outside the update.
    ports = PostgresM4ApplicationPorts(
        db.connection, structural_payloads={}, execution_mode=M4ExecutionMode.MEASURED
    )
    store = PostgresM5RuntimeStore(db.connection)
    event = DeleteDocumentVersionEvent("c2-delete-C", "c2-document-v1")
    payload = legacy_event_payload_digest(event)
    direct_plan = DynamicEventPlan(
        CorpusUpdateIdentity(
            event.event_id,
            payload,
            UpdateKind.DELETE,
            db.base.epoch_id,
            previous.manifest.candidate_policy_id,
        ),
        (),
        db.base.chunk_ids,
        (),
        REGISTRY_ID,
    )
    plan = M5TypedEventPlan(
        event.event_id,
        event,
        payload,
        direct_plan,
        previous.manifest.candidate_policy_id,
        previous.manifest.manifest_hash,
        previous.plan.requirement_registry_snapshot,
        ActiveChunkSnapshot.build(
            tuple(
                ActiveChunkSnapshotEntry.build(
                    chunk_version_id=chunk,
                    chunk_text=db.connection.execute(
                        "SELECT text FROM groundloop_chunk_version "
                        "WHERE chunk_version_id=%s",
                        (chunk,),
                    ).fetchone()[0],
                )
                for chunk in survivor_chunks
            )
        ),
        db.base.epoch_id,
    )
    execution = ApplicationExecutionPolicy(
        sha("c2-impact"),
        sha("c2-frontier"),
        previous.manifest.verifier_execution_spec_hash,
    )
    with db.connection.transaction(), db.connection.cursor() as cursor:
        direct = withdrawal._preview_document_direct_open(
            cursor, plan, execution_policy=execution
        )
        required = withdrawal._plan_document_requirement_withdrawal(cursor, plan)
    roots = withdrawal._document_requirement_roots(plan, previous.manifest, required)
    assert (
        len(roots) == 1
        and roots[0].scope.requirement_version_id
        == db.group.requirements[requirement_ordinal].requirement_version_id
    )
    assert not direct.root_jobs and not direct.discovery_scopes
    assert db.chunks[2] in required.deactivated_chunk_version_ids
    assert len(required.withdrawn_observation_ids) == 1
    receipt = store._open_document_open_prerequisite(
        plan,
        ports=ports,
        execution_policy=execution,
        supplied_direct_open=direct,
        supplied_requirement_withdrawal=required,
        requirement_roots=roots,
        requirement_root_set_hash=digests.requirement_root_set_digest(
            r.job.logical_job_id for r in roots
        ),
        recovery_operational_config=M5RuntimeOperationalConfig.build(60_000),
        recovery_root_fallback_required={r.job.logical_job_id: True for r in roots},
    )
    epoch = receipt.epoch_id
    root = roots[0].job
    lease = store.acquire_m5_job(epoch, 1, root)
    hits = tuple(
        M5RequirementChannelHit.build(
            epoch_id=epoch,
            root_job_id=root.logical_job_id,
            scope_contract_digest=root.scope_contract_digest,
            pair=SemanticPairKey(
                SubjectKind.REQUIREMENT,
                db.group.requirements[requirement_ordinal].requirement_version_id,
                chunk,
            ),
            candidate_policy_id=previous.manifest.candidate_policy_id,
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=rank,
            score=1.0 - rank / 10.0,
            channel_artifact_hash=sha(f"c2-fallback:{rank}"),
        )
        for rank, chunk in enumerate(selected_chunks, 1)
    )
    result = build_rank_interleaved_discovery_result(
        hits=hits,
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        manifest=previous.manifest,
        root_job_id=root.logical_job_id,
        scope_contract_digest=root.scope_contract_digest,
        eligible_snapshot_exhausted=True,
    )
    output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=epoch,
        payload_hash=root.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    staged = store.stage_m5_discovery_result_atomically(
        epoch,
        lease.resulting_revision,
        lease,
        root,
        result,
        output,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        M5RuntimeWork(),
        None,
        eligible_snapshot_exhausted=True,
    )
    revision = store.close_m5_requirement_roots_atomically(
        epoch,
        staged.resulting_revision,
        digests.requirement_root_set_digest(r.job.logical_job_id for r in roots),
    ).resulting_revision
    runtime = D24RequirementDatabase(
        db.dsn,
        db.schema,
        db.connection,
        epoch,
        plan,
        previous.manifest,
        tuple(r.job for r in roots),
        M5RuntimeOperationalConfig.build(60_000),
    )
    fixtures = {}
    for child in store.verifier_jobs(epoch):
        lease = store.acquire_m5_job(epoch, revision, child)
        revision = lease.resulting_revision
        fixtures[child.pair.chunk_version_id] = _build_c2_verifier_fixture(
            runtime, lease=lease, job=child
        )
    assert set(fixtures) == set(selected_chunks)
    db.runtime, db.fixtures = runtime, fixtures


def test_genuine_delete_fallback_same_hash_a_to_neutral_b_to_support(
    c2_db: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C2 working history only; never send carried positives to the old publisher."""
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )
    from tests.m5.postgres_runtime.d34_requirement_completion.test_accounting import (
        assert_native_completion_counters,
        native_producer_observer,
    )

    db = c2_db
    _seal_genuine_baseline(db)
    baseline_expected = expected_currency_from_sources(db)
    edge_before = db.connection.execute(
        "SELECT requirement_version_id,btrim(text_hash),group_version_id,requirem"
        "ent_ordinal,refcount "
        "FROM groundloop_m5_matching_edge_current"
    ).fetchall()
    hall_before = db.connection.execute(
        "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
        "maximum_deficiency,matching_size,distinct_hash_count FROM "
        "groundloop_m5_matching_hall_current"
    ).fetchall()
    _open_genuine_delete_fallback(db)
    epoch = db.runtime.epoch_id
    expected = dict(baseline_expected)
    old_key = next(key for key in expected if key[2] == db.chunks[2])
    expected[old_key] = None
    assert_currency_sources_and_raw_history(db, expected)
    _assert_complete_point(db, epoch, db.revision(), expected)
    for chunk, label in zip(db.chunks[:2], ("NEUTRAL", "SUPPORT"), strict=True):
        source = db.build(chunk, label, "after-delete")
        with native_producer_observer(monkeypatch) as (sizes, calls, details):
            with capture_sql(db.connection) as trace:
                db.complete(source)
        try:
            assert_native_completion_counters(db, source, trace, sizes)
        except AssertionError as error:
            raise AssertionError(
                (sizes, calls, [d for d in details if d[1]])
            ) from error
        observation = source.verifier_artifact.to_semantic_observation()
        expected[
            ("requirement", observation.subject_id, chunk, observation.task_type)
        ] = observation.observation_id
        assert_currency_sources_and_raw_history(db, expected)
        _assert_complete_point(db, epoch, db.revision(), expected)
    assert (
        db.connection.execute(
            "SELECT requirement_version_id,btrim(text_hash),group_version_id,requ"
            "irement_ordinal,refcount "
            "FROM groundloop_m5_matching_edge_working WHERE epoch_id=%s",
            (epoch,),
        ).fetchall()
        == edge_before
    )
    assert (
        db.connection.execute(
            "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
            "maximum_deficiency,matching_size,distinct_hash_count FROM "
            "groundloop_m5_matching_hall_working "
            "WHERE epoch_id=%s",
            (epoch,),
        ).fetchall()
        == hall_before
    )
    assert db.connection.execute(
        "SELECT epoch_id FROM groundloop_m5_publication_head"
    ).fetchone() == (db.base.epoch_id,)


def test_owner_and_real_distinct_login_complete_and_read_retained_bytes(
    c2_db: Any,
) -> None:
    from types import SimpleNamespace

    from psycopg.errors import InsufficientPrivilege

    from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )
    from tests.m5.postgres_runtime.test_semantic_readiness import _runtime_login

    db = c2_db
    first = db.build(db.chunks[0])
    db.complete(first)
    before = database_snapshot(db.connection)
    adapter = SimpleNamespace(
        connection=db.connection, schema_name=db.schema, dsn=db.dsn
    )
    with _runtime_login(adapter) as login:
        identity = login.execute("SELECT session_user,current_user").fetchone()
        assert identity[0] == identity[1] and identity[0] != "groundloop"
        login.commit()
        with capture_sql(login) as trace:
            assert db.complete(first, connection=login).exact_replay
        assert not any(
            s.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "SET CONSTRAINTS"))
            for s, _, _ in trace
        )
        assert database_snapshot(db.connection) == before
        second = db.build(db.chunks[1], "NEUTRAL")
        try:
            assert not db.complete(second, connection=login).exact_replay
        except InsufficientPrivilege:
            # Keep this a normal failing positive qualification, not an xfail
            # or owner-only waiver. Also prove the protected-reader failure
            # rolls back every earlier source/archive/header/accounting write.
            assert database_snapshot(db.connection) == before
            raise
        expected = expected_currency_from_sources(db)
        assert_currency_sources_and_raw_history(db, expected)
        _assert_complete_point(db, db.runtime.epoch_id, db.revision(), expected)


@pytest.mark.parametrize("c2_db", ["complete"], indirect=True)
def test_d35_real_login_fresh_bridge_coordinates_reconnect_replay_and_raw_denial(
    c2_db: Any,
) -> None:
    from types import SimpleNamespace

    from psycopg.errors import InsufficientPrivilege

    from tests.m5.postgres_runtime.d34_requirement_completion.conftest import (
        capture_sql,
    )
    from tests.m5.postgres_runtime.test_semantic_readiness import _runtime_login

    db = c2_db
    source = db.build(db.chunks[0])
    prior = db.revision()
    adapter = SimpleNamespace(
        connection=db.connection, schema_name=db.schema, dsn=db.dsn
    )
    denied: list[str] = []
    with _runtime_login(adapter) as login:
        identity = login.execute("SELECT session_user,current_user").fetchone()
        login.commit()
        assert identity[0] == identity[1] and identity[0] != "groundloop"

        def private_denials(stage: str) -> None:
            if stage != "d34_matching_prepared":
                return
            for table in (
                "groundloop_m5_matching_transition_context",
                "groundloop_m5_matching_change_journal",
                "groundloop_m5_matching_expected_changes",
            ):
                with pytest.raises(InsufficientPrivilege):
                    with login.transaction():
                        login.execute("SELECT * FROM pg_temp." + table)
                denied.append(table)

        with capture_sql(login) as trace:
            receipt = db.complete(
                source, connection=login, failure_injector=private_denials
            )
        assert not receipt.exact_replay and receipt.resulting_revision == prior + 1
        assert len(denied) == 3
        bridges = [
            (s, params)
            for s, params, _ in trace
            if s.startswith('SELECT * FROM "') and "matching_read_transition_" in s
        ]
        assert len(bridges) >= 3
        arguments = {params for _, params in bridges}
        assert len(arguments) == 1
        args = next(iter(arguments))
        assert args[:5] == (
            db.runtime.epoch_id,
            prior,
            prior + 1,
            "requirement_completion",
            source.lease.attempt.attempt_id,
        )
        assert len(args) == 8 and len(set(args[5:])) == 3
        assert all(s.startswith(f'SELECT * FROM "{db.schema}".') for s, _ in bridges)
        # A separate LOGIN connection, not the owner and not the prior session.
        with _runtime_login(adapter) as reader:
            with capture_sql(reader) as replay_trace:
                assert db.complete(source, connection=reader).exact_replay
        assert not any(
            "matching_read_transition_" in s
            or s.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "SET CONSTRAINTS"))
            for s, _, _ in replay_trace
        )
    expected = expected_currency_from_sources(db)
    assert_currency_sources_and_raw_history(db, expected)
    _assert_complete_point(db, db.runtime.epoch_id, db.revision(), expected)


def _wire(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _wire(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_wire(v) for v in value]
    return value


def _raw_python_point(
    db: Any,
    epoch: int,
    revision: int,
    expected_currency: dict[tuple[str, ...], str | None],
) -> M5Repository:
    """Only raw fixture/domain rows and independently resolved raw currency."""
    connection = db.connection
    base = InMemoryRepository(current_epoch=epoch)
    for row in connection.execute("SELECT question_id,text FROM groundloop_question"):
        base.register_question(Question(*row))
    for row in connection.execute(
        "SELECT answer_version_id,question_id,text,generator_model_id,"
        "generator_model_version,prompt_version FROM groundloop_answer_version"
    ):
        claims = tuple(
            Claim(str(c[0]), str(c[1]), str(c[2]), ModelStamp(*c[3:6]), bool(c[6]))
            for c in connection.execute(
                "SELECT claim_id,answer_version_id,text,extractor_model_id,"
                "extractor_model_version,extractor_prompt_version,required "
                "FROM groundloop_claim WHERE answer_version_id=%s",
                (row[0],),
            ).fetchall()
        )
        base.register_answer(AnswerVersion(*row[:3], ModelStamp(*row[3:])), claims)
    for row in connection.execute(
        "SELECT document_version_id,document_id,btrim(content_hash),valid_from_epoch "
        "FROM groundloop_document_version WHERE valid_from_epoch<=%s "
        "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
        (epoch, epoch),
    ).fetchall():
        chunks = tuple(
            ChunkVersion(*c)
            for c in connection.execute(
                "SELECT chunk_version_id,document_version_id,chunk_index,text,"
                "btrim(text_hash) "
                "FROM groundloop_chunk_version WHERE document_version_id=%s "
                "AND valid_from_epoch<=%s AND "
                "(valid_to_epoch IS NULL OR %s<valid_to_epoch) "
                "ORDER BY chunk_index",
                (row[0], epoch, epoch),
            ).fetchall()
        )
        base.register_document_version(DocumentVersion(*row[:3]), chunks, int(row[3]))
    policy = connection.execute(
        "SELECT policy_version,support_threshold,refute_threshold,tie_rule_version,"
        "valid_from_epoch FROM groundloop_decision_policy WHERE valid_from_epoch<=%s "
        "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
        (epoch, epoch),
    ).fetchone()
    assert policy is not None
    base.activate_policy(DecisionPolicy(*policy[:4]), int(policy[4]))
    repo = M5Repository(base, semantic_revision=revision)
    if db.group is not None:
        # Original immutable fixture source, unchanged by DELETE; never a
        # materialized/matching/certificate/patch target as oracle input.
        repo.register_group(
            db.group,
            int(
                connection.execute(
                    "SELECT creator_epoch_id FROM groundloop_m5_group_version "
                    "WHERE group_version_id=%s",
                    (db.group.group_version_id,),
                ).fetchone()[0]
            ),
        )
    currency = expected_currency
    for observation_id in sorted(v for v in currency.values() if v is not None):
        row = connection.execute(
            "SELECT observation_id,subject_kind,subject_id,chunk_version_id,task_type,"
            "support_score,refute_score,neutral_score,model_id,model_version,"
            "prompt_version,input_hash FROM groundloop_semantic_observation "
            "WHERE observation_id=%s",
            (observation_id,),
        ).fetchone()
        assert row is not None
        if not base.has_chunk_version(str(row[3])):
            continue
        observation = SemanticObservation(
            str(row[0]),
            SubjectKind(str(row[1])),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            *row[5:8],
            ModelStamp(*row[8:11]),
            str(row[11]),
        )
        if observation.subject_kind is SubjectKind.REQUIREMENT:
            repo.register_requirement_observation(observation, repo.current_point)
        else:
            base.register_observation(observation)
    return repo


def _independent_sql_point(
    db: Any, epoch: int, expected_currency: dict[tuple[str, ...], str | None]
) -> dict[str, list[dict[str, Any]]]:
    """Inline accepted SQL view bodies, with explicit point/currency CTEs.

    Stored view dependencies cannot be shadowed by a caller CTE. Every actual
    dependency is inlined; no head mutation, DDL or target-state input.
    """
    root = Path(__file__).resolve().parents[4]
    source = (root / "sql/m5/full_recompute_oracle.sql").read_text()
    bodies = dict(
        re.findall(r"CREATE OR REPLACE VIEW (\w+) AS\n(.*?);\n", source, re.S)
    )
    names = [
        "groundloop_m5_oracle_snapshot",
        "groundloop_m5_observation_decision_oracle",
        "groundloop_m5_active_requirement_edge_oracle",
        "groundloop_m5_requirement_state_oracle",
        "groundloop_m5_group_subset_hall_oracle",
        "groundloop_m5_group_state_oracle",
        "groundloop_m5_direct_claim_state_oracle",
        "groundloop_m5_claim_state_oracle",
        "groundloop_m5_answer_state_oracle",
    ]
    bodies[names[0]] = (
        "SELECT %(e)s::bigint AS epoch_id,policy.policy_version,"
        "policy.support_threshold,"
        "policy.refute_threshold FROM groundloop_decision_policy policy "
        "WHERE policy.valid_from_epoch<=%(e)s "
        "AND (policy.valid_to_epoch IS NULL OR %(e)s<policy.valid_to_epoch)"
    )
    bodies[names[1]] = bodies[names[1]].replace(
        "FROM groundloop_observation_currency AS currency",
        "FROM currency_at AS currency",
    )
    # A read-only raw structure projection; it never publishes staged rows.
    for name in names:
        bodies[name] = (
            bodies[name]
            .replace(
                "groundloop_m5_group_validity AS validity",
                "working_validity AS validity",
            )
            .replace(
                "groundloop_m5_requirement_version AS requirement",
                "working_requirement AS requirement",
            )
            .replace(
                "requirement.lifecycle_state = 'PUBLISHED'",
                "requirement.lifecycle_state IN ('PUBLISHED','STAGED')",
            )
        )
    currency = "SELECT * FROM jsonb_to_recordset(%(currency)s::jsonb) AS f("
    currency += "subject_kind groundloop_subject_kind,subject_id text,"
    currency += "chunk_version_id text,task_type text,observation_id text)"
    schema = sql.Identifier(db.schema).as_string(db.connection)
    validity = (
        f"SELECT * FROM {schema}.groundloop_m5_group_validity "
        "WHERE valid_from_epoch<=%(e)s AND (valid_to_epoch IS NULL OR "
        "%(e)s<valid_to_epoch) "
        "UNION ALL SELECT g.group_version_id,g.group_family_id,f.claim_id,g.seman"
        "tic_structure_hash,"
        "g.supersedes_group_version_id,g.creator_epoch_id,NULL::bigint "
        f"FROM {schema}.groundloop_m5_group_version g "
        f"JOIN {schema}.groundloop_m5_group_family f USING(group_family_id) "
        "WHERE g.creator_epoch_id=%(e)s AND g.lifecycle_state='STAGED' "
        "AND f.lifecycle_state='STAGED' AND g.group_version_id=%(gid)s "
        f"AND NOT EXISTS (SELECT 1 FROM {schema}.groundloop_m5_group_validity v "
        "WHERE v.group_version_id=g.group_version_id)"
    )
    requirements = (
        f"SELECT r.* FROM {schema}.groundloop_m5_requirement_version r "
        "JOIN working_validity v USING(group_version_id) "
        "WHERE r.lifecycle_state='PUBLISHED' OR "
        "(r.lifecycle_state='STAGED' AND r.creator_epoch_id=%(e)s AND "
        "r.group_version_id=%(gid)s)"
    )
    ctes = [
        f"currency_at AS ({currency})",
        f"working_validity AS ({validity})",
        f"working_requirement AS ({requirements})",
        *(f"{name} AS ({bodies[name]})" for name in names),
    ]
    selections = {
        "requirements": names[3],
        "groups": names[5],
        "claims": names[7],
        "answers": names[8],
        "edges": names[2],
        "hall": names[4],
    }
    query = (
        "WITH "
        + ",".join(ctes)
        + " "
        + " UNION ALL ".join(
            f"SELECT '{family}',to_jsonb(value) FROM {view} value"
            for family, view in selections.items()
        )
    )
    result: dict[str, list[dict[str, Any]]] = {family: [] for family in selections}
    for family, row in db.connection.execute(
        query,
        {
            "e": epoch,
            "gid": db.group.group_version_id,
            "currency": Jsonb(
                [
                    dict(
                        zip(
                            (
                                "subject_kind",
                                "subject_id",
                                "chunk_version_id",
                                "task_type",
                                "observation_id",
                            ),
                            (*key, value),
                            strict=True,
                        )
                    )
                    for key, value in expected_currency.items()
                ]
            ),
        },
    ).fetchall():
        result[family].append(row)
    return result


def _assert_complete_point(
    db: Any,
    epoch: int,
    revision: int,
    expected_currency: dict[tuple[str, ...], str | None],
) -> M5Repository:
    repo = _raw_python_point(db, epoch, revision, expected_currency)
    states = compute_reference_states(repo)
    independent_sql = _independent_sql_point(db, epoch, expected_currency)
    for family in ("requirements", "groups", "claims", "answers"):
        expected = getattr(states, family)
        key = {
            "requirements": "requirement_version_id",
            "groups": "group_version_id",
            "claims": "claim_id",
            "answers": "answer_version_id",
        }[family]
        sql_rows = {row[key]: row for row in independent_sql[family]}
        assert sql_rows == {
            identity: _wire(asdict(value)) for identity, value in expected.items()
        }
        kind = family[:-1] if family != "requirements" else "requirement"
        published = db.connection.execute(
            sql.SQL(
                "SELECT to_jsonb(value) FROM {} value WHERE valid_from_epoch<=%s "
                "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)"
            ).format(sql.Identifier(f"groundloop_m5_published_{kind}_state")),
            (db.base.epoch_id, db.base.epoch_id),
        ).fetchall()
        effective = {row[0][key]: row[0] for row in published}
        if epoch != db.base.epoch_id:
            for (row,) in db.connection.execute(
                sql.SQL(
                    "SELECT to_jsonb(value) FROM {} value WHERE epoch_id=%s"
                ).format(sql.Identifier(f"groundloop_m5_working_{kind}_state")),
                (epoch,),
            ).fetchall():
                effective[row[key]] = row
        assert {
            identity: {name: row[name] for name in asdict(expected[identity])}
            for identity, row in effective.items()
        } == sql_rows
    witnesses = derive_active_requirement_witnesses(repo)
    edges = {
        (w.requirement_version_id, w.text_hash): len(w.active_observation_ids)
        for w in witnesses
    }
    edge_payloads = {
        (w.requirement_version_id, w.text_hash): (
            repo.requirement(w.requirement_version_id).group_version_id,
            w.requirement_ordinal,
            len(w.active_observation_ids),
        )
        for w in witnesses
    }
    observations = {
        oid: (
            w.requirement_version_id,
            repo.requirement(w.requirement_version_id).group_version_id,
            w.requirement_ordinal,
            w.text_hash,
        )
        for w in witnesses
        for oid in w.active_observation_ids
    }
    masks: dict[tuple[str, str], int] = {}
    for w in witnesses:
        group_id = repo.requirement(w.requirement_version_id).group_version_id
        masks[group_id, w.text_hash] = (
            masks.get((group_id, w.text_hash), 0) | 1 << w.requirement_ordinal
        )
    assert {
        (r["requirement_version_id"], r["text_hash"]): len(r["active_observation_ids"])
        for r in independent_sql["edges"]
    } == edges
    for family, columns, keys, positive, expected in (
        (
            "observation",
            "observation_id,requirement_version_id,group_version_id,requirement_o"
            "rdinal,text_hash",
            1,
            "present",
            observations,
        ),
        (
            "edge",
            "requirement_version_id,text_hash,group_version_id,requirement_ordina"
            "l,refcount",
            2,
            "refcount",
            edge_payloads,
        ),
        ("hash_mask", "group_version_id,text_hash,mask", 2, "mask", masks),
    ):
        current = db.connection.execute(
            f"SELECT {columns} FROM groundloop_m5_matching_{family}_current"
        ).fetchall()
        effective = {
            (r[0] if keys == 1 else tuple(r[:keys])): (
                tuple(r[keys:]) if family in {"observation", "edge"} else r[-1]
            )
            for r in current
        }
        if epoch != db.base.epoch_id:
            rows = db.connection.execute(
                f"SELECT {columns},{positive} "
                f"FROM groundloop_m5_matching_{family}_working WHERE epoch_id=%s",
                (epoch,),
            ).fetchall()
            for r in rows:
                key = r[0] if keys == 1 else tuple(r[:keys])
                if not r[-1]:
                    effective.pop(key, None)
                else:
                    effective[key] = (
                        tuple(r[keys:-1])
                        if family in {"observation", "edge"}
                        else r[-2]
                    )
        assert effective == expected
    hall_ids = {
        row[0]
        for row in db.connection.execute(
            "SELECT group_version_id FROM groundloop_m5_matching_hall_current"
        ).fetchall()
    }
    if epoch != db.base.epoch_id:
        for group_id, present in db.connection.execute(
            "SELECT group_version_id,present "
            "FROM groundloop_m5_matching_hall_working WHERE epoch_id=%s",
            (epoch,),
        ).fetchall():
            assert present is True  # DELETE does not retire a group.
            hall_ids.add(group_id)
    assert hall_ids == set(states.groups)
    for group_id, state in states.groups.items():
        n = state.requirement_count
        histogram = [0] * (1 << n)
        for (gid, _), mask in masks.items():
            if gid == group_id:
                histogram[mask] += 1
        neighbors = [
            sum(count for mask, count in enumerate(histogram) if mask & subset)
            for subset in range(1 << n)
        ]
        deficiencies = [
            subset.bit_count() - neighbors[subset] for subset in range(1 << n)
        ]
        expected = (
            n,
            histogram,
            neighbors,
            deficiencies,
            max(deficiencies),
            n - max(deficiencies),
            sum(histogram),
        )
        relation = "working" if epoch != db.base.epoch_id else "current"
        parameters = (group_id, epoch) if relation == "working" else (group_id,)
        row = db.connection.execute(
            "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
            "maximum_deficiency,matching_size,distinct_hash_count "
            f"FROM groundloop_m5_matching_hall_{relation} "
            "WHERE group_version_id=%s"
            + (" AND epoch_id=%s" if relation == "working" else ""),
            parameters,
        ).fetchone()
        if row is None and relation == "working":
            row = db.connection.execute(
                "SELECT requirement_count,mask_histogram,neighbor_counts,deficiencies,"
                "maximum_deficiency,matching_size,distinct_hash_count "
                "FROM groundloop_m5_matching_hall_current WHERE group_version_id=%s",
                (group_id,),
            ).fetchone()
        assert row == expected
        assert {
            r["subset_mask"]: (r["neighbor_count"], r["deficiency"])
            for r in independent_sql["hall"]
            if r["group_version_id"] == group_id
        } == {s: (neighbors[s], deficiencies[s]) for s in range(1, 1 << n)}
    return repo
