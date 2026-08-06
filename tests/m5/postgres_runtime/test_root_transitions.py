"""Live PostgreSQL acceptance checks for M5 root staging and closure."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from psycopg import Connection, sql

from groundloop.domain import SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m5.events import RegisterGroupEvent, ReplaceGroupEvent
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptOutput,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobAttempt,
    M5JobKind,
    M5JobLease,
    M5LogicalJobSpec,
    M5RequirementAdmissionChannel,
    M5RequirementChannelHit,
    M5RequirementDiscoveryResult,
    M5RequirementScopeSelection,
    M5RetrievalTermination,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_roots import (
    close_m5_requirement_roots,
    stage_m5_discovery_result,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _roots(
    plan: Any, manifest: Any
) -> tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec], ...]:
    if isinstance(plan.event, RegisterGroupEvent):
        requirements = plan.event.group.requirements
    elif isinstance(plan.event, ReplaceGroupEvent):
        requirements = plan.event.successor.requirements
    else:
        requirements = ()
    roots = []
    for requirement in requirements:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=requirement.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=manifest.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                plan.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        roots.append(
            (
                scope,
                M5LogicalJobSpec.build(
                    structural_event_id=plan.structural_event_id,
                    job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                    manifest=manifest,
                    scope=scope,
                ),
            )
        )
    return tuple(sorted(roots, key=lambda item: item[1].logical_job_id))


def _acquire(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    job: M5LogicalJobSpec,
) -> M5JobLease:
    attempt = M5JobAttempt.build(
        logical_job_id=job.logical_job_id,
        attempt_ordinal=1,
        execution_spec_hash=job.execution_spec_hash,
        lease_token_hash=_sha(f"lease:{job.logical_job_id}"),
    )
    resulting_revision = expected_revision + 1
    with connection.transaction():
        cursor = connection.cursor()
        assert cursor.execute(
            "SELECT revision FROM groundloop_epoch WHERE epoch_id = %s FOR UPDATE",
            (epoch_id,),
        ).fetchone() == (expected_revision,)
        assert cursor.execute(
            """
            SELECT revision FROM groundloop_m5_runtime_epoch
            WHERE epoch_id = %s FOR UPDATE
            """,
            (epoch_id,),
        ).fetchone() == (expected_revision,)
        cursor.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s, %s)",
            (epoch_id, expected_revision),
        )
        assert cursor.execute(
            """
            UPDATE groundloop_m5_semantic_job
            SET job_state = 'running'
            WHERE epoch_id = %s AND logical_job_id = %s
              AND job_state = 'declared'
            """,
            (epoch_id, job.logical_job_id),
        ).rowcount == 1
        cursor.execute(
            """
            INSERT INTO groundloop_m5_job_attempt (
                attempt_id, logical_job_id, attempt_ordinal,
                execution_spec_hash, lease_token_hash, attempt_state
            ) VALUES (%s, %s, %s, %s, %s, 'dispatched')
            """,
            (
                attempt.attempt_id,
                attempt.logical_job_id,
                attempt.attempt_ordinal,
                attempt.execution_spec_hash,
                attempt.lease_token_hash,
            ),
        )
        cursor.execute(
            """
            UPDATE groundloop_m5_owner_pending_counter
            SET updated_revision = %s WHERE epoch_id = %s
            """,
            (resulting_revision, epoch_id),
        )
        cursor.execute(
            """
            UPDATE groundloop_m5_answer_pending_counter
            SET updated_revision = %s WHERE epoch_id = %s
            """,
            (resulting_revision, epoch_id),
        )
        assert cursor.execute(
            """
            UPDATE groundloop_epoch SET revision = %s
            WHERE epoch_id = %s AND revision = %s
            """,
            (resulting_revision, epoch_id, expected_revision),
        ).rowcount == 1
        assert cursor.execute(
            """
            UPDATE groundloop_m5_runtime_epoch
            SET runtime_state = 'semantic_pending', revision = %s
            WHERE epoch_id = %s AND revision = %s
            """,
            (resulting_revision, epoch_id, expected_revision),
        ).rowcount == 1
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return M5JobLease(job.logical_job_id, attempt, resulting_revision, True, False)


def _result(
    *,
    epoch_id: int,
    root: M5LogicalJobSpec,
    pairs: tuple[SemanticPairKey, ...],
) -> M5RequirementDiscoveryResult:
    hits = tuple(
        M5RequirementChannelHit.build(
            epoch_id=epoch_id,
            root_job_id=root.logical_job_id,
            scope_contract_digest=root.scope_contract_digest or "",
            pair=pair,
            candidate_policy_id=root.candidate_policy_id,
            channel=M5RequirementAdmissionChannel.VECTOR,
            rank=rank,
            score=1.0 - rank / 10.0,
            channel_artifact_hash=_sha(
                f"hit:{root.logical_job_id}:{pair.semantic_pair_digest}"
            ),
        )
        for rank, pair in enumerate(pairs, start=1)
    )
    selections = tuple(
        M5RequirementScopeSelection.build(
            root_job_id=root.logical_job_id,
            scope_contract_digest=root.scope_contract_digest or "",
            pair=pair,
            fused_rank=rank,
            reasons=(M5RequirementAdmissionChannel.VECTOR,),
        )
        for rank, pair in enumerate(pairs, start=1)
    )
    return M5RequirementDiscoveryResult.build(
        root_job_id=root.logical_job_id,
        scope_contract_digest=root.scope_contract_digest or "",
        termination=M5RetrievalTermination.SNAPSHOT_EXHAUSTED,
        channel_hits=hits,
        selections=selections,
    )


def _stage(
    connection: Connection[Any],
    *,
    epoch_id: int,
    expected_revision: int,
    lease: M5JobLease,
    job: M5LogicalJobSpec,
    result: M5RequirementDiscoveryResult,
    injector: Callable[[str], None] | None = None,
) -> tuple[Any, M5AttemptOutput]:
    assert lease.attempt is not None
    output = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=result.result_artifact_id,
        result_artifact_hash=result.result_artifact_hash,
    )
    with connection.transaction():
        receipt = stage_m5_discovery_result(
            connection.cursor(),
            epoch_id,
            expected_revision,
            lease,
            job,
            result,
            output,
            failure_injector=injector,
        )
    return receipt, output


def _root_set_hash(
    roots: tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec], ...]
) -> str:
    return digests.requirement_root_set_digest(job.logical_job_id for _, job in roots)


def _open_overlap_event(
    m5_runtime_db: Any,
) -> tuple[int, tuple[tuple[M5DiscoveryScopeContract, M5LogicalJobSpec], ...]]:
    event_id = "root-overlap-event"
    requirement = m5_runtime_db.group.requirements[0]
    requirement_snapshot = RequirementRegistrySnapshot.build(
        (
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id=requirement.requirement_version_id,
                group_version_id=m5_runtime_db.group.group_version_id,
                group_family_id=m5_runtime_db.group.group_family_id,
                owner_claim_id=m5_runtime_db.group.owner_claim_id,
                requirement_text=requirement.requirement_text,
            ),
        )
    )
    forward_scope = M5DiscoveryScopeContract.build(
        direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
        requirement_version_id=requirement.requirement_version_id,
        inserted_chunk_version_id=None,
        candidate_policy_id=m5_runtime_db.manifest.candidate_policy_id,
        requirement_registry_snapshot_digest=(
            requirement_snapshot.requirement_registry_snapshot_digest
        ),
        active_chunk_snapshot_digest=(
            m5_runtime_db.chunk_snapshot.active_chunk_snapshot_digest
        ),
    )
    reverse_scope = M5DiscoveryScopeContract.build(
        direction=M5DiscoveryDirection.REVERSE_CHUNK,
        requirement_version_id=None,
        inserted_chunk_version_id=m5_runtime_db.base.chunk_ids[0],
        candidate_policy_id=m5_runtime_db.manifest.candidate_policy_id,
        requirement_registry_snapshot_digest=(
            requirement_snapshot.requirement_registry_snapshot_digest
        ),
        active_chunk_snapshot_digest=(
            m5_runtime_db.chunk_snapshot.active_chunk_snapshot_digest
        ),
    )
    roots = tuple(
        sorted(
            (
                (
                    forward_scope,
                    M5LogicalJobSpec.build(
                        structural_event_id=event_id,
                        job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                        manifest=m5_runtime_db.manifest,
                        scope=forward_scope,
                    ),
                ),
                (
                    reverse_scope,
                    M5LogicalJobSpec.build(
                        structural_event_id=event_id,
                        job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
                        manifest=m5_runtime_db.manifest,
                        scope=reverse_scope,
                    ),
                ),
            ),
            key=lambda item: item[1].logical_job_id,
        )
    )
    connection = m5_runtime_db.connection
    with connection.transaction():
        cursor = connection.cursor()
        row = cursor.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s, %s, 1, 'committed', 'pending', 'pending',
                      'provisional', NULL)
            RETURNING epoch_id
            """,
            (event_id, _sha("root-overlap-payload")),
        ).fetchone()
        assert row is not None
        epoch_id = int(row[0])
        cursor.execute(
            """
            INSERT INTO groundloop_m5_update (
                epoch_id, update_kind, previous_published_epoch_id,
                decision_policy_version, manifest
            ) VALUES (%s, 'observe_requirement', %s, %s, '{}'::jsonb)
            """,
            (
                epoch_id,
                m5_runtime_db.base.epoch_id,
                m5_runtime_db.manifest.decision_policy_version,
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_runtime_epoch (
                epoch_id, structural_event_id, candidate_policy_id,
                candidate_policy_manifest_hash,
                requirement_registry_snapshot_digest,
                active_chunk_snapshot_digest,
                expected_previous_published_epoch_id,
                requirement_root_set_hash, runtime_state, revision,
                open_work_count, open_scope_count, blocking_failure_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                      'structural_committed', 1, 2, 2, 0)
            """,
            (
                epoch_id,
                event_id,
                m5_runtime_db.manifest.candidate_policy_id,
                m5_runtime_db.manifest.manifest_hash,
                requirement_snapshot.requirement_registry_snapshot_digest,
                m5_runtime_db.chunk_snapshot.active_chunk_snapshot_digest,
                m5_runtime_db.base.epoch_id,
                _root_set_hash(roots),
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_registry_snapshot (
                requirement_registry_snapshot_digest, requirement_count,
                created_epoch_id
            ) VALUES (%s, 1, %s)
            """,
            (
                requirement_snapshot.requirement_registry_snapshot_digest,
                epoch_id,
            ),
        )
        entry = requirement_snapshot.entries[0]
        cursor.execute(
            """
            INSERT INTO groundloop_m5_requirement_registry_snapshot_member (
                requirement_registry_snapshot_digest, member_ordinal,
                requirement_version_id, group_version_id, group_family_id,
                owner_claim_id, normalized_requirement_text,
                requirement_text_hash
            ) VALUES (%s, 0, %s, %s, %s, %s, %s, %s)
            """,
            (
                requirement_snapshot.requirement_registry_snapshot_digest,
                entry.requirement_version_id,
                entry.group_version_id,
                entry.group_family_id,
                entry.owner_claim_id,
                entry.normalized_requirement_text,
                entry.requirement_text_hash,
            ),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_active_chunk_snapshot (
                active_chunk_snapshot_digest, chunk_count, created_epoch_id,
                normalizer_id, normalizer_provenance_hash
            ) VALUES (%s, %s, %s, 'm5-normalize-text-v1', %s)
            ON CONFLICT (active_chunk_snapshot_digest) DO NOTHING
            """,
            (
                m5_runtime_db.chunk_snapshot.active_chunk_snapshot_digest,
                m5_runtime_db.chunk_snapshot.chunk_count,
                epoch_id,
                "d91b94f256f79c6bc7b29fafa41c7a608b90c17bd479b29a64be00fa538c49fb",
            ),
        )
        for ordinal, chunk in enumerate(m5_runtime_db.chunk_snapshot.entries):
            cursor.execute(
                """
                INSERT INTO groundloop_m5_active_chunk_snapshot_member (
                    active_chunk_snapshot_digest, member_ordinal,
                    chunk_version_id, text_hash
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (active_chunk_snapshot_digest, chunk_version_id)
                DO NOTHING
                """,
                (
                    m5_runtime_db.chunk_snapshot.active_chunk_snapshot_digest,
                    ordinal,
                    chunk.chunk_version_id,
                    chunk.text_hash,
                ),
            )
        for scope, job in roots:
            cursor.execute(
                """
                INSERT INTO groundloop_m5_discovery_scope (
                    root_job_id, epoch_id, direction,
                    requirement_version_id, inserted_chunk_version_id,
                    candidate_policy_id,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest, scope_contract_digest,
                    scope_state, created_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'open', 1)
                """,
                (
                    job.logical_job_id,
                    epoch_id,
                    scope.direction.value,
                    scope.requirement_version_id,
                    scope.inserted_chunk_version_id,
                    scope.candidate_policy_id,
                    scope.requirement_registry_snapshot_digest,
                    scope.active_chunk_snapshot_digest,
                    scope.scope_contract_digest,
                ),
            )
            cursor.execute(
                """
                INSERT INTO groundloop_m5_semantic_job (
                    logical_job_id, epoch_id, structural_event_id, job_kind,
                    candidate_policy_id, candidate_policy_manifest_hash,
                    scope_contract_digest,
                    requirement_registry_snapshot_digest,
                    active_chunk_snapshot_digest, role_template_hash,
                    execution_spec_hash, expandable, payload_hash,
                    job_state, created_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, TRUE, %s, 'declared', 1)
                """,
                (
                    job.logical_job_id,
                    epoch_id,
                    job.structural_event_id,
                    job.job_kind.value,
                    job.candidate_policy_id,
                    job.candidate_policy_manifest_hash,
                    job.scope_contract_digest,
                    job.requirement_registry_snapshot_digest,
                    job.active_chunk_snapshot_digest,
                    job.role_template_hash,
                    job.execution_spec_hash,
                    job.payload_hash,
                ),
            )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_owner_pending_counter (
                epoch_id, owner_claim_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, 1, 1, 0, 0, 1)
            """,
            (epoch_id, m5_runtime_db.group.owner_claim_id),
        )
        cursor.execute(
            """
            INSERT INTO groundloop_m5_answer_pending_counter (
                epoch_id, answer_version_id, broad_reverse_scope_count,
                forward_scope_count, verifier_job_count,
                blocking_failure_count, updated_revision
            ) VALUES (%s, %s, 1, 1, 0, 0, 1)
            """,
            (epoch_id, m5_runtime_db.base.answer_id),
        )
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return epoch_id, roots


def _transition_snapshot(
    connection: Connection[Any], epoch_id: int
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    tables = (
        "groundloop_epoch",
        "groundloop_m5_runtime_epoch",
        "groundloop_m5_discovery_scope",
        "groundloop_m5_semantic_job",
        "groundloop_m5_job_attempt",
        "groundloop_m5_attempt_result_artifact",
        "groundloop_m5_requirement_channel_hit",
        "groundloop_m5_requirement_scope_selection",
        "groundloop_m5_requirement_discovery_result",
        "groundloop_m5_requirement_admitted_pair",
        "groundloop_m5_requirement_admitted_pair_source",
        "groundloop_m5_job_dependency",
        "groundloop_m5_requirement_frontier_head",
        "groundloop_m5_owner_pending_counter",
        "groundloop_m5_answer_pending_counter",
    )
    snapshot = []
    for table in tables:
        if table in {"groundloop_epoch", "groundloop_m5_runtime_epoch"}:
            predicate = " WHERE epoch_id = %s"
            parameters: tuple[Any, ...] = (epoch_id,)
        elif table == "groundloop_m5_requirement_frontier_head":
            predicate = ""
            parameters = ()
        elif table in {
            "groundloop_m5_job_attempt",
            "groundloop_m5_attempt_result_artifact",
            "groundloop_m5_requirement_channel_hit",
            "groundloop_m5_requirement_scope_selection",
            "groundloop_m5_requirement_discovery_result",
            "groundloop_m5_requirement_admitted_pair_source",
            "groundloop_m5_job_dependency",
        }:
            predicate = ""
            parameters = ()
        else:
            predicate = " WHERE epoch_id = %s"
            parameters = (epoch_id,)
        statement = sql.SQL(
            "SELECT xmin::text, to_jsonb(row_value)::text FROM {} AS row_value"
            + predicate
            + ' ORDER BY to_jsonb(row_value)::text COLLATE "C"'
        ).format(sql.Identifier(table))
        rows = tuple(
            (str(row[0]), str(row[1]))
            for row in connection.execute(statement, parameters).fetchall()
        )
        snapshot.append((table, rows))
    connection.commit()
    return tuple(snapshot)


def test_empty_roots_stage_exactly_and_close_with_forward_heads(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="root-empty-close")
    receipt = PostgresM5RuntimeStore(
        m5_runtime_db.connection
    ).open_typed_event_atomically(plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    revision = 1
    staged: list[
        tuple[
            M5JobLease,
            M5LogicalJobSpec,
            M5RequirementDiscoveryResult,
            M5AttemptOutput,
        ]
    ] = []
    for _scope, job in roots:
        lease = _acquire(
            m5_runtime_db.connection,
            epoch_id=receipt.epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision += 1
        result = _result(epoch_id=receipt.epoch_id, root=job, pairs=())
        staged_receipt, output = _stage(
            m5_runtime_db.connection,
            epoch_id=receipt.epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=result,
        )
        revision += 1
        assert staged_receipt.resulting_revision == revision
        staged.append((lease, job, result, output))

    before_replay = _transition_snapshot(m5_runtime_db.connection, receipt.epoch_id)
    lease, job, result, output = staged[0]
    with m5_runtime_db.connection.transaction():
        replay = stage_m5_discovery_result(
            m5_runtime_db.connection.cursor(),
            receipt.epoch_id,
            lease.resulting_revision,
            lease,
            job,
            result,
            output,
        )
    assert replay.exact_replay
    assert replay.resulting_revision == revision
    assert (
        _transition_snapshot(m5_runtime_db.connection, receipt.epoch_id)
        == before_replay
    )

    with m5_runtime_db.connection.transaction():
        barrier = close_m5_requirement_roots(
            m5_runtime_db.connection.cursor(),
            receipt.epoch_id,
            revision,
            _root_set_hash(roots),
        )
    assert not barrier.exact_replay
    assert barrier.resulting_revision == revision + 1
    assert m5_runtime_db.connection.execute(
        """
        SELECT open_work_count, open_scope_count, blocking_failure_count
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (receipt.epoch_id,),
    ).fetchone() == (0, 0, 0)
    assert m5_runtime_db.connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_requirement_frontier_head
        WHERE completed_epoch_id = %s
        """,
        (receipt.epoch_id,),
    ).fetchone() == (2,)
    m5_runtime_db.connection.commit()

    before_barrier_replay = _transition_snapshot(
        m5_runtime_db.connection, receipt.epoch_id
    )
    with m5_runtime_db.connection.transaction():
        replay_barrier = close_m5_requirement_roots(
            m5_runtime_db.connection.cursor(),
            receipt.epoch_id,
            revision,
            _root_set_hash(roots),
        )
    assert replay_barrier.exact_replay
    assert replay_barrier.barrier_completion_hash == barrier.barrier_completion_hash
    assert _transition_snapshot(
        m5_runtime_db.connection, receipt.epoch_id
    ) == before_barrier_replay


def test_incomplete_barrier_and_conflicting_attempt_output_write_nothing(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="root-incomplete-conflict")
    opened = PostgresM5RuntimeStore(
        m5_runtime_db.connection
    ).open_typed_event_atomically(plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    _scope, job = roots[0]
    lease = _acquire(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=1,
        job=job,
    )
    result = _result(epoch_id=opened.epoch_id, root=job, pairs=())
    _, output = _stage(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=2,
        lease=lease,
        job=job,
        result=result,
    )
    before = _transition_snapshot(m5_runtime_db.connection, opened.epoch_id)
    with pytest.raises(InvalidEventError):
        with m5_runtime_db.connection.transaction():
            close_m5_requirement_roots(
                m5_runtime_db.connection.cursor(),
                opened.epoch_id,
                3,
                _root_set_hash(roots),
            )
    assert _transition_snapshot(m5_runtime_db.connection, opened.epoch_id) == before

    conflict_pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        roots[0][0].requirement_version_id or "",
        plan.active_chunk_snapshot.entries[0].chunk_version_id,
    )
    conflicting_result = _result(
        epoch_id=opened.epoch_id, root=job, pairs=(conflict_pair,)
    )
    conflicting = M5AttemptOutput.build(
        attempt=lease.attempt,
        job_epoch_id=opened.epoch_id,
        payload_hash=job.payload_hash,
        result_artifact_id=conflicting_result.result_artifact_id,
        result_artifact_hash=conflicting_result.result_artifact_hash,
    )
    with pytest.raises(EventConflictError):
        with m5_runtime_db.connection.transaction():
            stage_m5_discovery_result(
                m5_runtime_db.connection.cursor(),
                opened.epoch_id,
                2,
                lease,
                job,
                conflicting_result,
                conflicting,
            )
    assert output != conflicting
    assert _transition_snapshot(m5_runtime_db.connection, opened.epoch_id) == before


def test_forward_reverse_overlap_uses_least_root_and_retains_both_sources(
    m5_runtime_db: Any,
) -> None:
    epoch_id, roots = _open_overlap_event(m5_runtime_db)
    pair = SemanticPairKey(
        SubjectKind.REQUIREMENT,
        m5_runtime_db.group.requirements[0].requirement_version_id,
        m5_runtime_db.base.chunk_ids[0],
    )
    revision = 1
    for _scope, job in roots:
        lease = _acquire(
            m5_runtime_db.connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision += 1
        result = _result(epoch_id=epoch_id, root=job, pairs=(pair,))
        staged, _output = _stage(
            m5_runtime_db.connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=result,
        )
        revision = staged.resulting_revision

    with m5_runtime_db.connection.transaction():
        barrier = close_m5_requirement_roots(
            m5_runtime_db.connection.cursor(),
            epoch_id,
            revision,
            _root_set_hash(roots),
        )
    assert not barrier.exact_replay
    revision += 1
    expected_owner = min(job.logical_job_id for _scope, job in roots)
    admitted = m5_runtime_db.connection.execute(
        """
        SELECT owner_root_job_id, semantic_pair_digest
        FROM groundloop_m5_requirement_admitted_pair
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert admitted is not None
    assert str(admitted[0]).strip() == expected_owner
    assert str(admitted[1]).strip() == pair.semantic_pair_digest
    assert m5_runtime_db.connection.execute(
        """
        SELECT count(*)
        FROM groundloop_m5_requirement_admitted_pair_source AS source
        JOIN groundloop_m5_requirement_admitted_pair AS admitted
          ON admitted.admitted_pair_digest = source.admitted_pair_digest
        WHERE admitted.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (2,)
    assert m5_runtime_db.connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s AND job_kind = 'verify_requirement_pair'
        """,
        (epoch_id,),
    ).fetchone() == (1,)
    assert m5_runtime_db.connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone() == (0, 0, 1, 0, revision)
    m5_runtime_db.connection.commit()

    before = _transition_snapshot(m5_runtime_db.connection, epoch_id)
    with m5_runtime_db.connection.transaction():
        replay = close_m5_requirement_roots(
            m5_runtime_db.connection.cursor(),
            epoch_id,
            revision - 1,
            _root_set_hash(roots),
        )
    assert replay.exact_replay
    assert replay.resulting_revision == revision
    assert _transition_snapshot(m5_runtime_db.connection, epoch_id) == before


class _InjectedRootFailure(RuntimeError):
    pass


def _raise_at(point: str) -> Callable[[str], None]:
    def inject(actual: str) -> None:
        if actual == point:
            raise _InjectedRootFailure(point)

    return inject


@pytest.mark.parametrize(
    "failure_point",
    (
        "root_stage_output_reserved",
        "root_stage_attempt_artifact_inserted",
        "root_stage_result_inserted",
        "root_stage_revision_advanced",
        "root_stage_constraints_validated",
    ),
)
def test_stage_failure_injection_rolls_back_and_clean_retry_succeeds(
    m5_runtime_db: Any, failure_point: str
) -> None:
    plan = m5_runtime_db.register_plan(event_id=f"stage-rollback-{failure_point}")
    opened = PostgresM5RuntimeStore(
        m5_runtime_db.connection
    ).open_typed_event_atomically(plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    _scope, job = roots[0]
    lease = _acquire(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=1,
        job=job,
    )
    result = _result(epoch_id=opened.epoch_id, root=job, pairs=())
    before = _transition_snapshot(m5_runtime_db.connection, opened.epoch_id)
    with pytest.raises(_InjectedRootFailure):
        _stage(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=2,
            lease=lease,
            job=job,
            result=result,
            injector=_raise_at(failure_point),
        )
    assert _transition_snapshot(m5_runtime_db.connection, opened.epoch_id) == before
    retry, _output = _stage(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=2,
        lease=lease,
        job=job,
        result=result,
    )
    assert not retry.exact_replay
    assert retry.resulting_revision == 3


@pytest.mark.parametrize(
    "failure_point",
    (
        "root_barrier_admitted_pairs_inserted",
        "root_barrier_children_inserted",
        "root_barrier_scopes_closed",
        "root_barrier_pending_recomputed",
        "root_barrier_constraints_validated",
    ),
)
def test_barrier_failure_injection_rolls_back_and_clean_retry_succeeds(
    m5_runtime_db: Any, failure_point: str
) -> None:
    plan = m5_runtime_db.register_plan(event_id=f"barrier-rollback-{failure_point}")
    opened = PostgresM5RuntimeStore(
        m5_runtime_db.connection
    ).open_typed_event_atomically(plan)
    epoch_id = opened.epoch_id
    roots = _roots(plan, m5_runtime_db.manifest)
    revision = 1
    for _scope, job in roots:
        lease = _acquire(
            m5_runtime_db.connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision += 1
        staged, _output = _stage(
            m5_runtime_db.connection,
            epoch_id=epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=_result(epoch_id=epoch_id, root=job, pairs=()),
        )
        revision = staged.resulting_revision
    before = _transition_snapshot(m5_runtime_db.connection, epoch_id)
    with pytest.raises(_InjectedRootFailure):
        with m5_runtime_db.connection.transaction():
            close_m5_requirement_roots(
                m5_runtime_db.connection.cursor(),
                epoch_id,
                revision,
                _root_set_hash(roots),
                failure_injector=_raise_at(failure_point),
            )
    assert _transition_snapshot(m5_runtime_db.connection, epoch_id) == before
    with m5_runtime_db.connection.transaction():
        retry = close_m5_requirement_roots(
            m5_runtime_db.connection.cursor(),
            epoch_id,
            revision,
            _root_set_hash(roots),
        )
    assert not retry.exact_replay
    assert retry.resulting_revision == revision + 1


def test_concurrent_identical_stage_and_barrier_have_one_writer_each(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="root-concurrent-identical")
    opened = PostgresM5RuntimeStore(
        m5_runtime_db.connection
    ).open_typed_event_atomically(plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    _scope, first_job = roots[0]
    first_lease = _acquire(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=1,
        job=first_job,
    )
    first_result = _result(epoch_id=opened.epoch_id, root=first_job, pairs=())

    def stage_once() -> Any:
        with m5_runtime_db.reconnect() as connection:
            return _stage(
                connection,
                epoch_id=opened.epoch_id,
                expected_revision=2,
                lease=first_lease,
                job=first_job,
                result=first_result,
            )[0]

    with ThreadPoolExecutor(max_workers=2) as executor:
        stage_receipts = tuple(executor.map(lambda _: stage_once(), range(2)))
    assert sorted(receipt.exact_replay for receipt in stage_receipts) == [False, True]
    revision = 3
    for _scope, job in roots[1:]:
        lease = _acquire(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            job=job,
        )
        revision += 1
        staged, _output = _stage(
            m5_runtime_db.connection,
            epoch_id=opened.epoch_id,
            expected_revision=revision,
            lease=lease,
            job=job,
            result=_result(epoch_id=opened.epoch_id, root=job, pairs=()),
        )
        revision = staged.resulting_revision

    def close_once() -> Any:
        with m5_runtime_db.reconnect() as connection:
            with connection.transaction():
                return close_m5_requirement_roots(
                    connection.cursor(),
                    opened.epoch_id,
                    revision,
                    _root_set_hash(roots),
                )

    with ThreadPoolExecutor(max_workers=2) as executor:
        barrier_receipts = tuple(executor.map(lambda _: close_once(), range(2)))
    assert sorted(receipt.exact_replay for receipt in barrier_receipts) == [
        False,
        True,
    ]
    assert {receipt.resulting_revision for receipt in barrier_receipts} == {
        revision + 1
    }


def test_final_stage_and_barrier_race_never_closes_a_partial_root_set(
    m5_runtime_db: Any,
) -> None:
    plan = m5_runtime_db.register_plan(event_id="root-final-stage-barrier-race")
    opened = PostgresM5RuntimeStore(
        m5_runtime_db.connection
    ).open_typed_event_atomically(plan)
    roots = _roots(plan, m5_runtime_db.manifest)
    revision = 1
    _first_scope, first_job = roots[0]
    first_lease = _acquire(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=revision,
        job=first_job,
    )
    revision += 1
    first_stage, _first_output = _stage(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=revision,
        lease=first_lease,
        job=first_job,
        result=_result(epoch_id=opened.epoch_id, root=first_job, pairs=()),
    )
    revision = first_stage.resulting_revision
    _second_scope, second_job = roots[1]
    second_lease = _acquire(
        m5_runtime_db.connection,
        epoch_id=opened.epoch_id,
        expected_revision=revision,
        job=second_job,
    )
    revision += 1
    second_result = _result(epoch_id=opened.epoch_id, root=second_job, pairs=())

    def final_stage() -> Any:
        with m5_runtime_db.reconnect() as connection:
            return _stage(
                connection,
                epoch_id=opened.epoch_id,
                expected_revision=revision,
                lease=second_lease,
                job=second_job,
                result=second_result,
            )[0]

    def premature_barrier() -> Exception | None:
        try:
            with m5_runtime_db.reconnect() as connection:
                with connection.transaction():
                    close_m5_requirement_roots(
                        connection.cursor(),
                        opened.epoch_id,
                        revision,
                        _root_set_hash(roots),
                    )
        except (EventConflictError, InvalidEventError) as error:
            return error
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(final_stage)
        barrier_future = executor.submit(premature_barrier)
        stage_receipt = stage_future.result()
        barrier_error = barrier_future.result()
    assert not stage_receipt.exact_replay
    assert stage_receipt.resulting_revision == revision + 1
    assert isinstance(barrier_error, (EventConflictError, InvalidEventError))
    assert m5_runtime_db.connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_discovery_scope
        WHERE epoch_id = %s AND scope_state = 'result_staged'
        """,
        (opened.epoch_id,),
    ).fetchone() == (2,)
    m5_runtime_db.connection.commit()
    with m5_runtime_db.connection.transaction():
        completed = close_m5_requirement_roots(
            m5_runtime_db.connection.cursor(),
            opened.epoch_id,
            revision + 1,
            _root_set_hash(roots),
        )
    assert completed.resulting_revision == revision + 2
    assert not completed.exact_replay
