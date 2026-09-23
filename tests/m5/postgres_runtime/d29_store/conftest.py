"""Focused fixtures for the M5-D29 bounded-withdrawal store surface."""

from __future__ import annotations

import hashlib
import inspect
import textwrap
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from typing import Any

import pytest
from psycopg import Connection

from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import DynamicEventPlan
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    ChildClosure,
    CorpusUpdateIdentity,
    JobCompletion,
    JobKind,
    JobState,
    LogicalJobSpec,
    UpdateKind,
    stable_m4_digest,
)
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import postgres_withdrawal
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5TypedEventPlan,
)
from groundloop.postgres.migrations import (
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m5.postgres_runtime import test_migration_018 as migration_018_tests


@dataclass(frozen=True, slots=True)
class D29Schema:
    connection: Connection[Any]
    schema_name: str
    first_m4: dict[str, object]
    first_m5: dict[str, object]
    database: Any

    def document_plan(self, event_kind: str, *, tag: str) -> M5TypedEventPlan:
        """Build one exact absent-event document plan over the B3 base head."""

        base = self.database.base
        manifest = self.database.manifest
        source = self.connection.execute(
            """
            SELECT document.document_id, version.document_version_id
            FROM groundloop_document AS document
            JOIN groundloop_document_version AS version USING (document_id)
            WHERE version.valid_to_epoch IS NULL
            ORDER BY version.document_version_id COLLATE "C"
            LIMIT 1
            """
        ).fetchone()
        assert source is not None
        document_id = str(source[0])
        predecessor_version_id = str(source[1])
        event_id = f"d29-store-{event_kind}-{tag}"
        inserted: tuple[str, ...]
        deactivated: tuple[str, ...]
        snapshot = self.database.chunk_snapshot
        if event_kind == "insert":
            chunk = ChunkInput(
                f"{event_id}-chunk-v1",
                0,
                f"D29 inserted text for {tag}",
            )
            event = InsertDocumentEvent(
                event_id,
                f"{event_id}-document",
                f"{event_id}-document-v1",
                hashlib.sha256(f"{event_id}:content".encode()).hexdigest(),
                (chunk,),
            )
            inserted = (chunk.chunk_version_id,)
            deactivated = ()
            snapshot = ActiveChunkSnapshot.build(
                (
                    *self.database.chunk_snapshot.entries,
                    ActiveChunkSnapshotEntry.build(
                        chunk_version_id=chunk.chunk_version_id,
                        chunk_text=chunk.text,
                    ),
                )
            )
            update_kind = UpdateKind.INSERT
        elif event_kind == "delete":
            event = DeleteDocumentVersionEvent(event_id, predecessor_version_id)
            inserted = ()
            deactivated = tuple(sorted(base.chunk_ids))
            snapshot = ActiveChunkSnapshot.build(())
            update_kind = UpdateKind.DELETE
        elif event_kind == "replace":
            chunk = ChunkInput(
                f"{event_id}-chunk-v2",
                0,
                f"D29 replacement text for {tag}",
            )
            event = ReplaceDocumentVersionEvent(
                event_id,
                document_id,
                predecessor_version_id,
                f"{event_id}-document-v2",
                hashlib.sha256(f"{event_id}:content".encode()).hexdigest(),
                (chunk,),
            )
            inserted = (chunk.chunk_version_id,)
            deactivated = tuple(sorted(base.chunk_ids))
            snapshot = ActiveChunkSnapshot.build(
                (
                    ActiveChunkSnapshotEntry.build(
                        chunk_version_id=chunk.chunk_version_id,
                        chunk_text=chunk.text,
                    ),
                )
            )
            update_kind = UpdateKind.REPLACE
        else:
            raise AssertionError(f"unknown D29 document event kind: {event_kind}")
        payload_hash = legacy_event_payload_digest(event)
        direct = DynamicEventPlan(
            update=CorpusUpdateIdentity(
                event_id=event_id,
                payload_hash=payload_hash,
                update_kind=update_kind,
                previous_published_epoch_id=base.epoch_id,
                candidate_policy_id=manifest.candidate_policy_id,
            ),
            inserted_chunk_version_ids=inserted,
            deactivated_chunk_version_ids=deactivated,
            registered_claim_ids=tuple(sorted(base.claim_ids)),
            claim_registry_snapshot_id="d29-store-claim-registry",
        )
        return M5TypedEventPlan(
            structural_event_id=event_id,
            event=event,
            payload_hash=payload_hash,
            direct_plan=direct,
            candidate_policy_id=manifest.candidate_policy_id,
            candidate_policy_manifest_hash=manifest.manifest_hash,
            requirement_registry_snapshot=self.database.requirement_snapshot,
            active_chunk_snapshot=snapshot,
            expected_previous_published_epoch_id=base.epoch_id,
        )


@dataclass(frozen=True, slots=True)
class D29DirectAllStates:
    connection: Connection[Any]
    event: M5TypedEventPlan
    closure: Any
    state_job_ids: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class D29VerifierSchema:
    connection: Connection[Any]
    epoch_id: int
    revision: int
    observation_id: str
    job_id: str
    root_job_id: str
    requirement_id: str
    chunk_id: str
    task_type: str
    candidate_policy_id: str
    requirement_snapshot_digest: str
    chunk_snapshot_digest: str


@dataclass(frozen=True, slots=True)
class D29ReservationSchema:
    connection: Connection[Any]
    schema_name: str
    first_m5: dict[str, object]
    excluded_m5: dict[str, object]
    database: Any

    def document_plan(self, event_kind: str, *, tag: str) -> M5TypedEventPlan:
        return D29Schema(
            self.connection,
            self.schema_name,
            {},
            self.first_m5,
            self.database,
        ).document_plan(event_kind, tag=tag)


class _SnapshotReuseConnection:
    """Reuse a validated snapshot while preserving real transaction commits."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(query, str) and (
            "INSERT INTO groundloop_m5_requirement_registry_snapshot (" in query
            or "INSERT INTO groundloop_m5_requirement_registry_snapshot_member ("
            in query
        ):
            query += "\nON CONFLICT DO NOTHING"
        return self._connection.execute(query, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def _function_source(name: str) -> str:
    """Return dedented source for one package-private withdrawal helper."""

    return textwrap.dedent(inspect.getsource(getattr(postgres_withdrawal, name)))


@pytest.fixture
def function_source() -> Callable[[str], str]:
    return _function_source


@pytest.fixture(scope="module")
def d29_schema() -> Iterator[D29Schema]:
    """Install 018 over the accepted populated B3/017 history."""

    with migration_018_tests._pre018_populated_long_schema() as (
        connection,
        schema_name,
        database,
        _b3_snapshot,
        migration_016_tests,
    ):
        first_m4 = migration_018_tests._insert_m4_job_graph(
            connection,
            database=database,
            label="d29-store-plan",
            long_job_id=migration_018_tests._low_compressibility_identifier(
                "d29-store-plan-root",
                migration_018_tests.LONG_M4_ID_LENGTH,
            ),
            chunk_version_id=database.base.chunk_ids[0],
        )
        first_m5 = migration_018_tests._insert_valid_admitted_pair_fixture(
            connection,
            database=database,
            migration_016_tests=migration_016_tests,
            event_id="d29-store-plan-requirement-event",
            chunk_index=0,
            include_all_job_states=True,
        )
        result = install_m5_bounded_document_withdrawal_bundle(connection)
        assert result.applied
        connection.commit()
        yield D29Schema(connection, schema_name, first_m4, first_m5, database)
        connection.rollback()


@pytest.fixture(scope="module")
def d29_database(d29_schema: D29Schema) -> D29Schema:
    return d29_schema


@pytest.fixture
def d29_direct_all_states(d29_schema: D29Schema) -> Iterator[D29DirectAllStates]:
    """Create one exact retained-direct root in each M4 job state."""

    connection = d29_schema.connection
    savepoint = "d29_retained_direct_all_states"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        event = d29_schema.document_plan("delete", tag="retained-direct-all-states")
        direct = event.direct_plan
        assert direct is not None
        claim_ids = list(d29_schema.database.base.claim_ids)
        while len(claim_ids) < len(JobState):
            claim_id = f"d29-retained-direct-state-claim-{len(claim_ids)}"
            assert (
                connection.execute(
                    """
                    INSERT INTO groundloop_claim (
                        claim_id, answer_version_id, text, extractor_model_id,
                        extractor_model_version, extractor_prompt_version, required
                    )
                    SELECT %s, answer_version_id, text || ' retained-state',
                           extractor_model_id, extractor_model_version,
                           extractor_prompt_version, required
                    FROM groundloop_claim WHERE claim_id = %s
                    """,
                    (claim_id, claim_ids[0]),
                ).rowcount
                == 1
            )
            claim_ids.append(claim_id)
        claim_ids = list(sorted(claim_ids[: len(JobState)]))
        direct = replace(direct, registered_claim_ids=tuple(claim_ids))
        event = replace(event, direct_plan=direct)
        candidate_policy_id = event.candidate_policy_id
        m5_policy = d29_schema.database.manifest
        direct_policy = CandidatePolicyManifest.build(
            policy_id=candidate_policy_id,
            embedding_model_artifact_id=m5_policy.embedding_model_artifact_id,
            claim_role_template_hash=m5_policy.requirement_role_template_hash,
            chunk_role_template_hash=m5_policy.chunk_role_template_hash,
            vector_method_version=m5_policy.vector_method_version,
            vector_index_kind=m5_policy.vector_index_kind,
            vector_index_build_config_hash=(m5_policy.vector_index_build_config_hash),
            vector_search_config_hash=m5_policy.vector_search_config_hash,
            lexical_method_version=m5_policy.lexical_method_version,
            lexical_config_hash=m5_policy.lexical_config_hash,
            lexical_postgres_version=m5_policy.lexical_postgres_version,
            lexical_regconfig_identity=m5_policy.lexical_regconfig_identity,
            claim_registry_snapshot_id=direct.claim_registry_snapshot_id,
            claim_count=len(claim_ids),
            fusion_version=m5_policy.fusion_version,
            approximate_cap_per_inserted_chunk=(
                m5_policy.reverse_budget_per_inserted_chunk
            ),
            frontier_depth=1,
            verifier_execution_spec_hash=m5_policy.verifier_execution_spec_hash,
            decision_policy_version=m5_policy.decision_policy_version,
            lineage_safety_override=m5_policy.lineage_safety_override,
        )
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
                claim_count, fusion_version,
                approximate_cap_per_inserted_chunk, frontier_depth, manifest
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, '{}'::jsonb
            )
            ON CONFLICT (candidate_policy_id) DO NOTHING
            """,
            (
                candidate_policy_id,
                direct_policy.policy_hash,
                direct_policy.embedding_model_artifact_id,
                direct_policy.decision_policy_version,
                direct_policy.claim_role_template_hash,
                direct_policy.chunk_role_template_hash,
                direct_policy.vector_method_version,
                direct_policy.vector_index_kind.value,
                direct_policy.vector_index_build_config_hash,
                direct_policy.vector_search_config_hash,
                direct_policy.lexical_method_version,
                direct_policy.lexical_config_hash,
                direct_policy.lexical_postgres_version,
                direct_policy.lexical_regconfig_identity,
                direct_policy.claim_registry_snapshot_id,
                direct_policy.claim_count,
                direct_policy.fusion_version,
                direct_policy.approximate_cap_per_inserted_chunk,
                direct_policy.frontier_depth,
            ),
        )
        epoch_row = connection.execute(
            """
            INSERT INTO groundloop_epoch (
                event_id, payload_hash, revision, structural_status,
                semantic_status, evaluation_state, publication_mode, sealed_at
            ) VALUES (%s, %s, 0, 'committed', 'sealed', 'complete', 'strict', now())
            RETURNING epoch_id
            """,
            (event.structural_event_id, event.payload_hash),
        ).fetchone()
        assert epoch_row is not None
        epoch_id = int(epoch_row[0])
        connection.execute(
            """
            ALTER TABLE groundloop_m4_update
            DISABLE TRIGGER groundloop_m4_update_runtime_mode_guard
            """
        )
        connection.execute(
            """
            INSERT INTO groundloop_m4_update (
                epoch_id, update_kind, candidate_policy_id,
                previous_published_epoch_id, registry_snapshot_id, manifest
            ) VALUES (%s, 'delete', %s, %s, %s, '{}'::jsonb)
            """,
            (
                epoch_id,
                candidate_policy_id,
                event.expected_previous_published_epoch_id,
                direct.claim_registry_snapshot_id,
            ),
        )
        connection.execute(
            """
            ALTER TABLE groundloop_m4_update
            ENABLE TRIGGER groundloop_m4_update_runtime_mode_guard
            """
        )

        state_job_ids: list[tuple[str, str]] = []
        root_ids: list[str] = []
        fallback_claim_ids: list[str] = []
        for state, claim_id in zip(JobState, claim_ids, strict=True):
            execution_spec_hash = stable_m4_digest(
                "d29-retained-direct-all-states-execution-v1", state.value
            )
            job_id = LogicalJobSpec.derive_job_id(
                event_id=event.structural_event_id,
                kind=JobKind.FRONTIER_RETRIEVE,
                candidate_policy_id=candidate_policy_id,
                execution_spec_hash=execution_spec_hash,
                claim_id=claim_id,
            )
            payload_hash = stable_m4_digest(
                "m4-application-job-payload-v1",
                event.payload_hash,
                JobKind.FRONTIER_RETRIEVE.value,
                "",
                claim_id,
                "",
            )
            successful = state in {
                JobState.COMPLETED_ACTIVE,
                JobState.COMPLETED_INACTIVE,
            }
            terminal = state in {
                JobState.COMPLETED_ACTIVE,
                JobState.COMPLETED_INACTIVE,
                JobState.TERMINAL_FAILED,
                JobState.CANCELLED,
            }
            result_artifact_id = (
                f"d29-retained-direct-{state.value}-result" if successful else None
            )
            result_artifact_hash = (
                stable_m4_digest(
                    "d29-retained-direct-all-states-result-v1", state.value
                )
                if successful
                else None
            )
            child_closure = (
                ChildClosure.build(
                    parent_job_id=job_id,
                    result_artifact_hash=result_artifact_hash,
                    child_job_ids=(),
                )
                if result_artifact_hash is not None
                else None
            )
            completion = (
                JobCompletion.build(
                    job_id=job_id,
                    payload_hash=payload_hash,
                    execution_spec_hash=execution_spec_hash,
                    result_artifact_id=result_artifact_id,
                    result_artifact_hash=result_artifact_hash,
                    terminal_state=state,
                    child_closure=child_closure,
                )
                if result_artifact_id is not None
                and result_artifact_hash is not None
                and child_closure is not None
                else None
            )
            connection.execute(
                """
                INSERT INTO groundloop_semantic_job (
                    job_id, epoch_id, parent_job_id, job_kind,
                    candidate_policy_id, payload_hash, execution_spec_hash,
                    claim_id, chunk_version_id, expandable, job_state,
                    child_closed, child_set_hash, completion_digest,
                    result_artifact_id, result_artifact_hash,
                    created_revision, completed_revision, completed_at
                ) VALUES (
                    %s, %s, NULL, 'frontier_retrieve', %s, %s, %s,
                    %s, NULL, true, %s, %s, %s, %s, %s, %s,
                    0, %s, CASE WHEN %s THEN clock_timestamp() ELSE NULL END
                )
                """,
                (
                    job_id,
                    epoch_id,
                    candidate_policy_id,
                    payload_hash,
                    execution_spec_hash,
                    claim_id,
                    state.value,
                    successful,
                    None if child_closure is None else child_closure.child_set_hash,
                    None if completion is None else completion.completion_digest,
                    result_artifact_id,
                    result_artifact_hash,
                    1 if terminal else None,
                    terminal,
                ),
            )
            if successful:
                connection.execute(
                    """
                    INSERT INTO groundloop_m4_discovery_result (
                        root_job_id, epoch_id, result_artifact_id,
                        result_artifact_hash, fallback_satisfied,
                        channel_hit_count, admitted_pair_count,
                        channel_set_hash, admitted_pair_set_hash
                    ) VALUES (%s, %s, %s, %s, true, 0, 0, %s, %s)
                    """,
                    (
                        job_id,
                        epoch_id,
                        result_artifact_id,
                        result_artifact_hash,
                        stable_m4_digest("m4-discovery-channel-set-v1"),
                        stable_m4_digest("m4-discovery-admitted-set-v1"),
                    ),
                )
            state_job_ids.append((state.value, job_id))
            root_ids.append(job_id)
            fallback_claim_ids.append(claim_id)

        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.execute("SET CONSTRAINTS ALL DEFERRED")
        closure = postgres_withdrawal._DocumentClosure(
            epoch_id=epoch_id,
            epoch_revision=0,
            update_kind="document_delete",
            previous_epoch_id=event.expected_previous_published_epoch_id,
            runtime_revision=1,
            runtime_state="structural_committed",
            open_work_count=3,
            open_scope_count=0,
            blocking_failure_count=1,
            candidate_policy=d29_schema.database.manifest,
            requirement_snapshot_digest=(
                event.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            chunk_snapshot_digest=(
                event.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
            requirement_root_set_hash=stable_m4_digest(
                "d29-retained-direct-empty-requirement-roots-v1"
            ),
            direct_root_job_ids=tuple(sorted(root_ids)),
            direct_scope_root_job_ids=(),
            direct_fallback_claim_ids=tuple(sorted(fallback_claim_ids)),
        )
        yield D29DirectAllStates(
            connection=connection,
            event=event,
            closure=closure,
            state_job_ids=tuple(state_job_ids),
        )
    finally:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")


@pytest.fixture
def d29_reservation_database() -> Iterator[D29ReservationSchema]:
    """Build two committed owners in an isolated reservation history."""

    from m5.postgres import helpers as b3_helpers

    with migration_018_tests._pre018_populated_long_schema() as (
        connection,
        schema_name,
        database,
        _b3_snapshot,
        migration_016_tests,
    ):
        first_m5 = migration_018_tests._insert_valid_admitted_pair_fixture(
            connection,
            database=database,
            migration_016_tests=migration_016_tests,
            event_id="d29-reservation-qualifying-owner",
            chunk_index=0,
        )
        migration_016_tests._terminalize_recovered_epoch_for_audit(
            connection,
            epoch_id=int(str(first_m5["epoch_id"])),
        )
        connection.execute("SET CONSTRAINTS ALL DEFERRED")

        excluded_group = b3_helpers.make_group(
            group_id="d29-width-partial-group",
            family_id="d29-width-partial-family",
            claim_id=database.base.claim_ids[5],
            texts=("partial missing",),
            requirement_ids=("d29-width-partial-group-requirement-1",),
            source_id="d29-width-partial-source",
        )
        excluded_answer = connection.execute(
            "SELECT answer_version_id FROM groundloop_claim WHERE claim_id = %s",
            (excluded_group.owner_claim_id,),
        ).fetchone()
        assert excluded_answer is not None
        excluded_requirement = excluded_group.requirements[0]
        excluded_snapshot = migration_016_tests.RequirementRegistrySnapshot.build(
            (
                migration_016_tests.RequirementRegistrySnapshotEntry.build(
                    requirement_version_id=(
                        excluded_requirement.requirement_version_id
                    ),
                    group_version_id=excluded_group.group_version_id,
                    group_family_id=excluded_group.group_family_id,
                    owner_claim_id=excluded_group.owner_claim_id,
                    requirement_text=excluded_requirement.requirement_text,
                ),
            )
        )
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_registry_snapshot (
                requirement_registry_snapshot_digest, requirement_count,
                created_epoch_id
            ) VALUES (%s, 1, %s)
            """,
            (
                excluded_snapshot.requirement_registry_snapshot_digest,
                database.base.epoch_id,
            ),
        )
        excluded_entry = excluded_snapshot.entries[0]
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
                excluded_snapshot.requirement_registry_snapshot_digest,
                excluded_entry.requirement_version_id,
                excluded_entry.group_version_id,
                excluded_entry.group_family_id,
                excluded_entry.owner_claim_id,
                excluded_entry.normalized_requirement_text,
                excluded_entry.requirement_text_hash,
            ),
        )
        connection.commit()
        fixture_connection = _SnapshotReuseConnection(connection)
        excluded_database = replace(
            database,
            base=replace(database.base, answer_id=str(excluded_answer[0])),
            connection=fixture_connection,
            group=excluded_group,
            requirement_snapshot=excluded_snapshot,
        )
        excluded_m5 = migration_018_tests._insert_valid_admitted_pair_fixture(
            fixture_connection,
            database=excluded_database,
            migration_016_tests=migration_016_tests,
            event_id="d29-reservation-excluded-owner",
            chunk_index=0,
        )
        excluded_subject = connection.execute(
            """
            SELECT subject_id
            FROM groundloop_m5_requirement_admitted_pair
            WHERE admitted_pair_digest = %s
            """,
            (str(excluded_m5["admitted_pair_digest"]),),
        ).fetchone()
        assert excluded_subject is not None
        assert str(excluded_subject[0]) == excluded_requirement.requirement_version_id
        active_requirement_rows = connection.execute(
            """
            SELECT requirement.requirement_version_id,
                   requirement.group_version_id,
                   group_version.group_family_id,
                   family.claim_id,
                   requirement.requirement_text
            FROM groundloop_m5_requirement_version AS requirement
            JOIN groundloop_m5_group_version AS group_version
              ON group_version.group_version_id = requirement.group_version_id
            JOIN groundloop_m5_group_family AS family
              ON family.group_family_id = group_version.group_family_id
            JOIN groundloop_m5_group_validity AS validity
              ON validity.group_version_id = group_version.group_version_id
            WHERE requirement.lifecycle_state = 'PUBLISHED'
              AND group_version.lifecycle_state = 'PUBLISHED'
              AND family.lifecycle_state = 'PUBLISHED'
              AND validity.valid_from_epoch <= %s
              AND (validity.valid_to_epoch IS NULL
                   OR %s < validity.valid_to_epoch)
            ORDER BY requirement.requirement_version_id COLLATE "C"
            """,
            (database.base.epoch_id, database.base.epoch_id),
        ).fetchall()
        active_snapshot = migration_016_tests.RequirementRegistrySnapshot.build(
            tuple(
                migration_016_tests.RequirementRegistrySnapshotEntry.build(
                    requirement_version_id=str(row[0]),
                    group_version_id=str(row[1]),
                    group_family_id=str(row[2]),
                    owner_claim_id=str(row[3]),
                    requirement_text=str(row[4]),
                )
                for row in active_requirement_rows
            )
        )
        assert active_snapshot.contains(str(excluded_subject[0]))
        assert active_snapshot.contains(
            database.group.requirements[0].requirement_version_id
        )
        connection.commit()
        result = install_m5_bounded_document_withdrawal_bundle(connection)
        assert result.applied
        connection.commit()
        yield D29ReservationSchema(
            connection=connection,
            schema_name=schema_name,
            first_m5=first_m5,
            excluded_m5=excluded_m5,
            database=replace(database, requirement_snapshot=active_snapshot),
        )
        connection.rollback()


@pytest.fixture(scope="module")
def d29_verifier_database() -> Iterator[D29VerifierSchema]:
    """Build one complete verifier-produced requirement observation on 016."""

    from m5.postgres import helpers as b3_helpers

    from tests.m5.postgres_runtime import test_migration_016 as migration_016_tests

    with migration_016_tests._isolated_015_schema() as connection:
        install_m5_runtime_recovery_bundle(connection)
        connection.commit()
        completed = (
            migration_016_tests._complete_requirement_verifier_for_recovery_test(
                connection
            )
        )
        epoch_id = int(completed["epoch_id"])
        row = connection.execute(
            """
            SELECT execution.observation_id, execution.logical_job_id,
                   job.parent_job_id, observation.subject_id,
                   observation.chunk_version_id, observation.task_type,
                   job.candidate_policy_id,
                   runtime.requirement_registry_snapshot_digest,
                   runtime.active_chunk_snapshot_digest
            FROM groundloop_m5_requirement_verifier_execution AS execution
            JOIN groundloop_m5_semantic_job AS job
              ON job.logical_job_id = execution.logical_job_id
            JOIN groundloop_semantic_observation AS observation
              ON observation.observation_id = execution.observation_id
            JOIN groundloop_m5_runtime_epoch AS runtime
              ON runtime.epoch_id = job.epoch_id
            WHERE job.epoch_id = %s
            """,
            (epoch_id,),
        ).fetchone()
        assert row is not None and row[2] is not None
        observation_id = str(row[0])
        b3_helpers.install_current_currency(
            connection,
            observation_id=observation_id,
            subject_kind="requirement",
            subject_id=str(row[3]),
            chunk_id=str(row[4]),
            task_type=str(row[5]),
            epoch_id=epoch_id,
            revision=int(completed["revision"]),
        )
        connection.commit()
        yield D29VerifierSchema(
            connection=connection,
            epoch_id=epoch_id,
            revision=int(completed["revision"]),
            observation_id=observation_id,
            job_id=str(row[1]).strip(),
            root_job_id=str(row[2]).strip(),
            requirement_id=str(row[3]),
            chunk_id=str(row[4]),
            task_type=str(row[5]),
            candidate_policy_id=str(row[6]).strip(),
            requirement_snapshot_digest=str(row[7]).strip(),
            chunk_snapshot_digest=str(row[8]).strip(),
        )
