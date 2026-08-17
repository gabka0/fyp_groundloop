"""Live composition evidence for the D24 group/requirement pre-seal bridge."""

from __future__ import annotations

from copy import copy
from dataclasses import replace
from typing import cast

import pytest

from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ObserveEvent,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import DynamicEventPlan, StructuralWithdrawal
from groundloop.m4.contracts import (
    CorpusUpdateIdentity,
    UpdateKind,
)
from groundloop.m4.contracts import (
    DiscoveryScope as M4DiscoveryScope,
)
from groundloop.m4.contracts import (
    JobKind as M4JobKind,
)
from groundloop.m4.contracts import (
    LogicalJobSpec as M4LogicalJobSpec,
)
from groundloop.m4.pipeline import StructuralPayload
from groundloop.m4.runtime.withdrawal import WithdrawalPlan
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    legacy_event_payload_digest,
    m5_event_payload_digest,
)
from groundloop.m5.runtime import digests as runtime_digests
from groundloop.m5.runtime.application import M5RequirementRootDeclaration
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5RequirementFallbackKey,
    M5RequirementWithdrawalPlan,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeOperationalConfig,
    M5RuntimeTiming,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TypedEventPlan,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_application import (
    PostgresM5GroupRequirementPreSealPorts,
)
from tests.m5.postgres_runtime.d24_application.conftest import (
    ApplicationD24Database,
    ControlledDiscovery,
    ControlledMeasurements,
    ControlledVerifier,
    assemble_application,
    database_snapshot,
    relation_rows,
    requirement_root_set_hash,
    requirement_roots,
    requirement_snapshot,
    sha,
)

_PRE_SEAL_ERROR = (
    "production typed seal is unavailable in the group/requirement pre-seal facade"
)
_ATTEMPT_TIMING = M5RuntimeTiming(
    coordinator_non_db_non_neural_ns=3,
    neural_wall_ns=31,
    postgres_roundtrip_wall_ns=5,
    external_io_wall_ns=7,
    end_to_end_wall_ns=46,
)


def _plan(
    database: ApplicationD24Database,
    event_kind: str,
    *,
    tag: str,
    requirement_count: int = 1,
) -> M5TypedEventPlan:
    if event_kind == "register":
        return database.register_plan(
            tag=tag,
            requirement_count=requirement_count,
        )
    if event_kind == "replace":
        return database.replace_plan(
            tag=tag,
            requirement_count=requirement_count,
        )
    if event_kind == "retire":
        return database.retire_plan(tag=tag)
    raise AssertionError(f"unknown controlled event kind: {event_kind}")


def _excluded_plan(
    database: ApplicationD24Database,
    event_kind: str,
) -> M5TypedEventPlan:
    """Build each valid typed event intentionally excluded by R2c."""

    event_id = f"d24-app-excluded-{event_kind}"
    event: (
        InsertDocumentEvent
        | DeleteDocumentVersionEvent
        | ReplaceDocumentVersionEvent
        | PolicyChangeEvent
        | ObserveEvent
        | ObserveRequirementEvent
    )
    update_kind: UpdateKind | None
    inserted: tuple[str, ...]
    deactivated: tuple[str, ...]
    direct_plan: DynamicEventPlan | None = None
    if event_kind == "document_insert":
        event = InsertDocumentEvent(
            event_id,
            "d24-app-excluded-document",
            "d24-app-excluded-document-v1",
            sha("d24-app-excluded-document-content"),
            (ChunkInput("d24-app-excluded-inserted-chunk", 0, "excluded chunk"),),
        )
        update_kind = UpdateKind.INSERT
        inserted = ("d24-app-excluded-inserted-chunk",)
        deactivated = ()
    elif event_kind == "document_delete":
        event = DeleteDocumentVersionEvent(
            event_id,
            "d24-application-base-document-v1",
        )
        update_kind = UpdateKind.DELETE
        inserted = ()
        deactivated = database.base.chunk_ids
    elif event_kind == "document_replace":
        event = ReplaceDocumentVersionEvent(
            event_id,
            "d24-application-base-document",
            "d24-application-base-document-v1",
            "d24-app-excluded-document-v2",
            sha("d24-app-excluded-replacement-content"),
            (
                ChunkInput(
                    "d24-app-excluded-replacement-chunk",
                    0,
                    "excluded replacement chunk",
                ),
            ),
        )
        update_kind = UpdateKind.REPLACE
        inserted = ("d24-app-excluded-replacement-chunk",)
        deactivated = database.base.chunk_ids
    elif event_kind == "policy_change":
        event = PolicyChangeEvent(
            event_id,
            DecisionPolicy("d24-app-excluded-policy", 0.7, 0.7, "v1"),
        )
        update_kind = None
        inserted = ()
        deactivated = ()
    elif event_kind == "claim_observation":
        event = ObserveEvent(
            event_id,
            SemanticObservation(
                "d24-app-excluded-claim-observation",
                SubjectKind.CLAIM,
                database.base.claim_ids[0],
                database.base.chunk_ids[0],
                "verify_claim_v1",
                0.9,
                0.05,
                0.05,
                ModelStamp("excluded-model", "v1", "p1"),
                sha("d24-app-excluded-claim-input"),
            ),
        )
        update_kind = None
        inserted = ()
        deactivated = ()
    elif event_kind == "requirement_observation":
        event = ObserveRequirementEvent(
            event_id,
            SemanticObservation(
                "d24-app-excluded-requirement-observation",
                SubjectKind.REQUIREMENT,
                database.published_group.requirements[0].requirement_version_id,
                database.base.chunk_ids[0],
                "verify_requirement_v1",
                0.9,
                0.05,
                0.05,
                ModelStamp("excluded-model", "v1", "p1"),
                sha("d24-app-excluded-requirement-input"),
            ),
        )
        update_kind = None
        inserted = ()
        deactivated = ()
    else:
        raise AssertionError(f"unknown excluded event kind: {event_kind}")

    payload_hash = (
        m5_event_payload_digest(event)
        if isinstance(event, ObserveRequirementEvent)
        else legacy_event_payload_digest(event)
    )
    if update_kind is not None:
        direct_plan = DynamicEventPlan(
            update=CorpusUpdateIdentity(
                event_id=event_id,
                payload_hash=payload_hash,
                update_kind=update_kind,
                previous_published_epoch_id=database.base.epoch_id,
                candidate_policy_id=database.manifest.candidate_policy_id,
            ),
            inserted_chunk_version_ids=inserted,
            deactivated_chunk_version_ids=deactivated,
            registered_claim_ids=database.base.claim_ids,
            claim_registry_snapshot_id="d24-app-excluded-claim-registry",
        )
    return M5TypedEventPlan(
        structural_event_id=event_id,
        event=event,
        payload_hash=payload_hash,
        direct_plan=direct_plan,
        candidate_policy_id=database.manifest.candidate_policy_id,
        candidate_policy_manifest_hash=database.manifest.manifest_hash,
        requirement_registry_snapshot=requirement_snapshot((database.published_group,)),
        active_chunk_snapshot=database.active_chunk_snapshot,
        expected_previous_published_epoch_id=database.base.epoch_id,
    )


def _open_exact(
    database: ApplicationD24Database,
    plan: M5TypedEventPlan,
    *,
    facade: PostgresM5GroupRequirementPreSealPorts | None = None,
) -> tuple[int, tuple[M5RequirementRootDeclaration, ...]]:
    selected = database.facade if facade is None else facade
    withdrawal = selected.plan_exact_requirement_withdrawal(plan)
    roots = requirement_roots(plan, database.manifest)
    opened = selected.open_typed_event_atomically(
        plan,
        None,
        None,
        withdrawal,
        (),
        (),
        roots,
        requirement_root_set_hash(roots),
    )
    assert not opened.replayed
    assert not opened.already_sealed
    assert not opened.already_failed
    return opened.epoch_id, roots


def _withdrawal(
    plan: M5TypedEventPlan,
    *,
    deactivated_chunks: tuple[str, ...] = (),
    fallback_keys: tuple[M5RequirementFallbackKey, ...] = (),
) -> M5RequirementWithdrawalPlan:
    return M5RequirementWithdrawalPlan(
        event_id=plan.structural_event_id,
        deactivated_chunk_version_ids=deactivated_chunks,
        withdrawn_candidate_pair_digests=(),
        withdrawn_observation_ids=(),
        cancelled_job_ids=(),
        fallback_keys=fallback_keys,
        plan_digest=runtime_digests.requirement_withdrawal_plan_digest(
            event_id=plan.structural_event_id,
            deactivated_chunk_version_ids=deactivated_chunks,
            withdrawn_candidate_pair_digests=(),
            withdrawn_observation_ids=(),
            cancelled_job_ids=(),
            fallback_keys=(
                (key.requirement_version_id, key.candidate_policy_id)
                for key in fallback_keys
            ),
        ),
    )


def _direct_root(
    database: ApplicationD24Database,
    plan: M5TypedEventPlan,
) -> tuple[M4LogicalJobSpec, M4DiscoveryScope]:
    execution_hash = sha("d24-app-excluded-direct-execution")
    claim_id = database.base.claim_ids[0]
    job_id = M4LogicalJobSpec.derive_job_id(
        event_id=plan.structural_event_id,
        kind=M4JobKind.FRONTIER_RETRIEVE,
        candidate_policy_id=plan.candidate_policy_id,
        execution_spec_hash=execution_hash,
        claim_id=claim_id,
    )
    job = M4LogicalJobSpec(
        job_id=job_id,
        event_id=plan.structural_event_id,
        kind=M4JobKind.FRONTIER_RETRIEVE,
        candidate_policy_id=plan.candidate_policy_id,
        payload_hash=sha("d24-app-excluded-direct-payload"),
        execution_spec_hash=execution_hash,
        target_claim_id=claim_id,
        expandable=True,
    )
    return job, M4DiscoveryScope(
        root_job_id=job.job_id,
        registry_snapshot_id="d24-app-excluded-registry",
        registered_claim_ids=(claim_id,),
    )


def _empty_direct_withdrawal() -> StructuralWithdrawal:
    return StructuralWithdrawal(
        WithdrawalPlan(
            deactivated_chunk_ids=(),
            observation_ids=(),
            candidate_edge_ids=(),
            affected_pairs=(),
            affected_claim_ids=(),
            chunk_lookups=0,
            observation_edge_visits=0,
            candidate_edge_visits=0,
        )
    )


def _sum_work(*values: M5RuntimeWork) -> M5RuntimeWork:
    counters = {
        name: sum(getattr(value, name) for value in values)
        for name in M5RuntimeWork.counter_names()
    }
    return M5RuntimeWork(**counters)


def _durable_transition_rows(
    database: ApplicationD24Database,
    *,
    epoch_id: int,
) -> tuple[tuple[object, ...], ...]:
    """Read each transition point together with its exact work anchor."""

    with database.connection.transaction():
        return tuple(
            tuple(row)
            for row in database.connection.execute(
                """
                SELECT timing.epoch_id, timing.contribution_kind,
                       timing.source_id, timing.contribution_key_digest,
                       timing.anchor_revision,
                       contribution.contribution_key_digest,
                       contribution.applied_revision,
                       timing.required_interval_observed,
                       timing.coordinator_non_db_non_neural_ns,
                       timing.neural_wall_ns,
                       timing.postgres_roundtrip_wall_ns,
                       timing.external_io_wall_ns,
                       timing.end_to_end_wall_ns,
                       timing.postgres_server_execution_ns,
                       timing.postgres_lock_wait_ns,
                       timing.postgres_wal_bytes,
                       timing.postgres_shared_block_reads,
                       timing.observation_digest,
                       timing.transition_timing_digest
                FROM groundloop_m5_transition_call_timing AS timing
                JOIN groundloop_m5_runtime_work_contribution AS contribution
                  ON contribution.epoch_id = timing.epoch_id
                 AND contribution.contribution_kind = timing.contribution_kind
                 AND contribution.source_id = timing.source_id
                 AND contribution.contribution_key_digest =
                     timing.contribution_key_digest
                 AND contribution.applied_revision = timing.anchor_revision
                WHERE timing.epoch_id = %s
                ORDER BY timing.anchor_revision,
                         timing.contribution_kind COLLATE "C",
                         timing.source_id COLLATE "C"
                """,
                (epoch_id,),
            ).fetchall()
        )


def _assert_transition_calls_persisted(
    database: ApplicationD24Database,
    *,
    epoch_id: int,
    measurements: ControlledMeasurements,
    expected_kinds: tuple[M5RuntimeWorkContributionKind, ...],
) -> tuple[tuple[object, ...], ...]:
    """Prove one observed durable timing row for every first-written anchor."""

    anchors = tuple(measurements.transition_calls)
    assert tuple(anchor.contribution_kind for anchor in anchors) == expected_kinds
    timing = measurements.transition_timing
    assert timing is not None
    observation = M5RuntimeTimingObservation.build(timing)
    expected_rows = tuple(
        (
            anchor.epoch_id,
            anchor.contribution_kind.value,
            anchor.source_id,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            anchor.contribution_key_digest,
            anchor.anchor_revision,
            True,
            timing.coordinator_non_db_non_neural_ns,
            timing.neural_wall_ns,
            timing.postgres_roundtrip_wall_ns,
            timing.external_io_wall_ns,
            timing.end_to_end_wall_ns,
            timing.postgres_server_execution_ns,
            timing.postgres_lock_wait_ns,
            timing.postgres_wal_bytes,
            timing.postgres_shared_block_reads,
            observation.observation_digest,
            runtime_digests.transition_call_timing_digest(
                epoch_id=anchor.epoch_id,
                contribution_kind=anchor.contribution_kind,
                source_id=anchor.source_id,
                contribution_key_digest=anchor.contribution_key_digest,
                anchor_revision=anchor.anchor_revision,
                observation_digest=observation.observation_digest,
            ),
        )
        for anchor in anchors
    )
    rows = _durable_transition_rows(database, epoch_id=epoch_id)
    assert rows == expected_rows
    return rows


@pytest.mark.parametrize("event_kind", ("register", "replace", "retire"))
def test_group_planning_and_structural_open_are_exact(
    d24_application_db: ApplicationD24Database,
    event_kind: str,
) -> None:
    database = d24_application_db
    plan = _plan(database, event_kind, tag=f"positive-{event_kind}")
    withdrawal = database.facade.plan_exact_requirement_withdrawal(plan)
    assert withdrawal == _withdrawal(plan)

    epoch_id, roots = _open_exact(database, plan)
    expected_root_count = 0 if event_kind == "retire" else 1
    assert len(roots) == expected_root_count
    assert relation_rows(
        database.connection,
        "groundloop_m5_runtime_epoch",
        (
            "epoch_id",
            "structural_event_id",
            "candidate_policy_id",
            "requirement_root_set_hash",
            "runtime_state",
            "revision",
        ),
    ) == (
        (
            epoch_id,
            plan.structural_event_id,
            database.manifest.candidate_policy_id,
            requirement_root_set_hash(roots),
            "structural_committed",
            1,
        ),
    )
    jobs = relation_rows(
        database.connection,
        "groundloop_m5_semantic_job",
        ("logical_job_id", "job_kind", "job_state"),
    )
    assert len(jobs) == expected_root_count
    assert all(row[1:] == ("forward_requirement_retrieval", "declared") for row in jobs)


@pytest.mark.parametrize("corruption", ("manifest", "operational_config"))
def test_constructor_revalidates_immutable_inputs_without_writes(
    d24_application_db: ApplicationD24Database,
    corruption: str,
) -> None:
    database = d24_application_db
    manifest = copy(database.manifest)
    config = copy(database.operational_config)
    if corruption == "manifest":
        object.__setattr__(manifest, "candidate_policy_id", sha("corrupt-manifest"))
    else:
        object.__setattr__(config, "config_digest", sha("corrupt-config"))
    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError):
        PostgresM5GroupRequirementPreSealPorts(database.store, manifest, config)
    assert database_snapshot(database.connection) == before


def test_candidate_policy_is_exact_and_read_only(
    d24_application_db: ApplicationD24Database,
) -> None:
    database = d24_application_db
    before = database_snapshot(database.connection)
    returned = database.facade.candidate_policy(database.manifest.candidate_policy_id)
    assert returned == database.manifest
    assert returned is not database.manifest
    with pytest.raises(ValidationError, match="not configured"):
        database.facade.candidate_policy(
            database.alternate_manifest().candidate_policy_id
        )
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "event_kind",
    (
        "document_insert",
        "document_delete",
        "document_replace",
        "policy_change",
        "claim_observation",
        "requirement_observation",
    ),
)
def test_every_excluded_typed_event_is_rejected_before_write(
    d24_application_db: ApplicationD24Database,
    event_kind: str,
) -> None:
    database = d24_application_db
    plan = _excluded_plan(database, event_kind)
    empty_hash = runtime_digests.requirement_root_set_digest(())
    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError, match="only group lifecycle events"):
        database.facade.plan_exact_requirement_withdrawal(plan)
    with pytest.raises(ValidationError, match="only group lifecycle events"):
        database.facade.plan_direct_open(plan)
    with pytest.raises(ValidationError, match="only group lifecycle events"):
        database.facade.open_typed_event_atomically(
            plan,
            None,
            None,
            _withdrawal(plan),
            (),
            (),
            (),
            empty_hash,
        )
    assert database_snapshot(database.connection) == before


def test_group_plan_with_injected_direct_plan_is_rejected_before_write(
    d24_application_db: ApplicationD24Database,
) -> None:
    database = d24_application_db
    group_plan = copy(database.register_plan(tag="injected-direct-plan"))
    excluded = _excluded_plan(database, "document_insert")
    assert excluded.direct_plan is not None
    object.__setattr__(group_plan, "direct_plan", excluded.direct_plan)
    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError, match="cannot carry a direct plan"):
        database.facade.plan_exact_requirement_withdrawal(group_plan)
    assert database_snapshot(database.connection) == before


@pytest.mark.parametrize(
    "mode",
    (
        "direct_payload",
        "direct_withdrawal",
        "direct_root",
        "direct_scope",
        "wrong_event_id",
        "wrong_withdrawal_event",
        "nonempty_withdrawal",
        "fallback_key",
        "changed_withdrawal_digest",
        "missing_root",
        "duplicate_root",
        "reordered_roots",
        "foreign_root",
        "wrong_registry_snapshot",
        "wrong_chunk_snapshot",
        "wrong_execution",
        "wrong_direction",
        "wrong_target",
        "wrong_root_policy",
        "mutable_roots",
        "mutable_direct_roots",
        "mutable_direct_scopes",
        "wrong_root_hash",
        "wrong_manifest",
        "wrong_predecessor",
    ),
)
def test_structural_preflight_rejection_matrix_is_zero_write(
    d24_application_db: ApplicationD24Database,
    mode: str,
) -> None:
    database = d24_application_db
    plan = database.register_plan(tag=f"reject-{mode}", requirement_count=2)
    withdrawal = database.facade.plan_exact_requirement_withdrawal(plan)
    roots = requirement_roots(plan, database.manifest)
    direct_payload: StructuralPayload | None = None
    direct_withdrawal: StructuralWithdrawal | None = None
    direct_roots: tuple[M4LogicalJobSpec, ...] = ()
    direct_scopes: tuple[M4DiscoveryScope, ...] = ()
    root_hash = requirement_root_set_hash(roots)

    if mode == "direct_payload":
        direct_payload = StructuralPayload()
    elif mode == "direct_withdrawal":
        direct_withdrawal = _empty_direct_withdrawal()
    elif mode == "direct_root":
        direct_root, _ = _direct_root(database, plan)
        direct_roots = (direct_root,)
    elif mode == "direct_scope":
        _, direct_scope = _direct_root(database, plan)
        direct_scopes = (direct_scope,)
    elif mode == "wrong_event_id":
        plan = copy(plan)
        object.__setattr__(plan, "structural_event_id", "d24-app-wrong-event-id")
    elif mode == "wrong_withdrawal_event":
        withdrawal = _withdrawal(database.register_plan(tag="another-withdrawal"))
    elif mode == "nonempty_withdrawal":
        withdrawal = _withdrawal(
            plan,
            deactivated_chunks=(database.base.chunk_ids[0],),
        )
    elif mode == "fallback_key":
        assert isinstance(plan.event, RegisterGroupEvent)
        requirement_id = plan.event.group.requirements[0].requirement_version_id
        withdrawal = _withdrawal(
            plan,
            fallback_keys=(
                M5RequirementFallbackKey(
                    requirement_id,
                    database.manifest.candidate_policy_id,
                ),
            ),
        )
    elif mode == "changed_withdrawal_digest":
        withdrawal = copy(withdrawal)
        object.__setattr__(withdrawal, "plan_digest", sha("wrong-withdrawal"))
    elif mode == "missing_root":
        roots = roots[1:]
    elif mode == "duplicate_root":
        roots = (roots[0], roots[0], roots[1])
    elif mode == "reordered_roots":
        roots = tuple(reversed(roots))
    elif mode == "foreign_root":
        foreign = database.register_plan(tag="foreign-root", requirement_count=1)
        roots = requirement_roots(foreign, database.manifest)
    elif mode == "wrong_registry_snapshot":
        assert isinstance(plan.event, RegisterGroupEvent)
        plan = replace(
            plan,
            requirement_registry_snapshot=requirement_snapshot((plan.event.group,)),
        )
    elif mode == "wrong_chunk_snapshot":
        plan = replace(plan, active_chunk_snapshot=ActiveChunkSnapshot.build(()))
    elif mode == "wrong_execution":
        changed_job = copy(roots[0].job)
        object.__setattr__(
            changed_job,
            "execution_spec_hash",
            sha("wrong-root-execution"),
        )
        changed = copy(roots[0])
        object.__setattr__(changed, "job", changed_job)
        roots = (changed, *roots[1:])
    elif mode == "wrong_direction":
        changed_scope = copy(roots[0].scope)
        object.__setattr__(
            changed_scope,
            "direction",
            M5DiscoveryDirection.REVERSE_CHUNK,
        )
        changed = copy(roots[0])
        object.__setattr__(changed, "scope", changed_scope)
        roots = (changed, *roots[1:])
    elif mode == "wrong_target":
        changed = copy(roots[0])
        object.__setattr__(changed, "scope", roots[1].scope)
        roots = (changed, *roots[1:])
    elif mode == "wrong_root_policy":
        roots = requirement_roots(plan, database.alternate_manifest())
    elif mode == "mutable_roots":
        roots = cast(tuple[M5RequirementRootDeclaration, ...], list(roots))
    elif mode == "mutable_direct_roots":
        direct_roots = cast(tuple[M4LogicalJobSpec, ...], [])
    elif mode == "mutable_direct_scopes":
        direct_scopes = cast(tuple[M4DiscoveryScope, ...], [])
    elif mode == "wrong_root_hash":
        root_hash = sha("wrong-root-set")
    elif mode == "wrong_manifest":
        alternate = database.alternate_manifest()
        plan = database.register_plan(
            tag="wrong-manifest",
            requirement_count=2,
            manifest=alternate,
        )
        roots = requirement_roots(plan, alternate)
        root_hash = requirement_root_set_hash(roots)
        withdrawal = _withdrawal(plan)
    elif mode == "wrong_predecessor":
        plan = replace(
            plan,
            expected_previous_published_epoch_id=database.base.epoch_id + 100,
        )
    else:
        raise AssertionError(f"unhandled rejection mode: {mode}")

    before = database_snapshot(database.connection)
    with pytest.raises((ValidationError, InvalidEventError, EventConflictError)):
        database.facade.open_typed_event_atomically(
            plan,
            direct_payload,
            direct_withdrawal,
            withdrawal,
            direct_roots,
            direct_scopes,
            roots,
            root_hash,
        )
    assert database_snapshot(database.connection) == before


def test_group_direct_port_is_a_checked_zero_write_noop(
    d24_application_db: ApplicationD24Database,
) -> None:
    database = d24_application_db
    plan = database.register_plan(tag="direct-noop")
    before = database_snapshot(database.connection)
    with pytest.raises(ValidationError, match="does not plan direct events"):
        database.facade.plan_direct_open(plan)
    receipt = database.facade.run_pending_direct(
        database.base.epoch_id,
        7,
        plan,
    )
    assert receipt.resulting_revision == 7
    assert receipt.call_work.is_zero
    assert receipt.blocked_reason is None
    assert receipt.terminal_failure_reason is None
    assert database_snapshot(database.connection) == before


def _provider_work(stage: str, *, nonzero: bool) -> M5RuntimeWork:
    if not nonzero:
        return M5RuntimeWork()
    if stage == "discovery":
        return M5RuntimeWork(
            requirement_forward_retrieval_call_count=1,
            embedding_model_call_count=1,
        )
    return M5RuntimeWork(
        requirement_verifier_call_count=1,
        verifier_model_call_count=1,
        verifier_input_token_count=8,
        verifier_output_token_count=3,
    )


@pytest.mark.parametrize("stage", ("discovery", "verifier"))
@pytest.mark.parametrize("retryable", (True, False))
@pytest.mark.parametrize("nonzero", (False, True))
def test_live_external_failure_composition_and_terminal_reconnect(
    d24_application_db: ApplicationD24Database,
    stage: str,
    retryable: bool,
    nonzero: bool,
) -> None:
    database = d24_application_db
    plan = database.register_plan(tag=f"{stage}-{retryable}-{nonzero}")
    failure_work = _provider_work(stage, nonzero=nonzero)
    discovery_success_work = M5RuntimeWork() if stage == "verifier" else failure_work
    discovery = ControlledDiscovery(
        database,
        include_pair=stage == "verifier",
        failure_reason=(
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE
            if stage == "discovery" and retryable
            else M5RunFailureReason.RETRIEVAL_ERROR
            if stage == "discovery"
            else None
        ),
        retryable=retryable if stage == "discovery" else False,
        call_work=discovery_success_work,
        attempt_timing=_ATTEMPT_TIMING if stage == "discovery" else None,
    )
    verifier = ControlledVerifier(
        database,
        failure_reason=(
            M5RunFailureReason.VERIFIER_UNAVAILABLE
            if stage == "verifier" and retryable
            else M5RunFailureReason.VERIFIER_ERROR
            if stage == "verifier"
            else None
        ),
        retryable=retryable if stage == "verifier" else False,
        call_work=failure_work if stage == "verifier" else M5RuntimeWork(),
        attempt_timing=_ATTEMPT_TIMING if stage == "verifier" else None,
    )
    measurements = ControlledMeasurements()
    application = assemble_application(database, discovery, verifier, measurements)
    result = application.run_event(plan)

    assert len(discovery.calls) == 1
    assert len(verifier.calls) == int(stage == "verifier")
    assert result.call_work == _sum_work(
        discovery_success_work,
        failure_work if stage == "verifier" else M5RuntimeWork(),
    )
    for name in (
        "requirement_forward_retrieval_call_count",
        "requirement_verifier_call_count",
        "embedding_model_call_count",
        "verifier_model_call_count",
        "verifier_input_token_count",
        "verifier_output_token_count",
    ):
        assert getattr(result.event_work, name) == getattr(result.call_work, name)
    assert result.event_timing.neural_wall_ns == _ATTEMPT_TIMING.neural_wall_ns
    assert result.event_timing != result.call_timing
    assert result.event_work != result.call_work
    assert result.event_timing_coverage is not None
    assert result.event_timing_coverage.required_expected_count >= 1
    result.event_timing_coverage.validate_aggregate(result.event_timing)

    expected_reason = (
        M5RunFailureReason.RETRIEVAL_UNAVAILABLE
        if stage == "discovery" and retryable
        else M5RunFailureReason.RETRIEVAL_ERROR
        if stage == "discovery"
        else M5RunFailureReason.VERIFIER_UNAVAILABLE
        if retryable
        else M5RunFailureReason.VERIFIER_ERROR
    )
    assert result.failure_reason is expected_reason
    assert isinstance(result.epoch_id, int)
    expected_job_kind = (
        "forward_requirement_retrieval"
        if stage == "discovery"
        else "verify_requirement_pair"
    )
    with database.connection.transaction():
        attempt_rows = database.connection.execute(
            """
            SELECT attempt.attempt_state, job.job_state,
                   evidence.disposition, evidence.result_or_error_hash,
                   evidence.attempt_work_digest, contribution.work_digest,
                   timing.required_interval_observed,
                   timing.coordinator_non_db_non_neural_ns,
                   timing.neural_wall_ns,
                   timing.postgres_roundtrip_wall_ns,
                   timing.external_io_wall_ns,
                   timing.end_to_end_wall_ns
            FROM groundloop_m5_attempt_execution_evidence AS evidence
            JOIN groundloop_m5_job_attempt AS attempt
              ON attempt.attempt_id = evidence.attempt_id
            JOIN groundloop_m5_semantic_job AS job
              ON job.epoch_id = evidence.epoch_id
             AND job.logical_job_id = attempt.logical_job_id
            JOIN groundloop_m5_runtime_work_contribution AS contribution
              ON contribution.epoch_id = evidence.epoch_id
             AND contribution.contribution_kind = 'm5_attempt_execution'
             AND contribution.source_id = evidence.attempt_id
            JOIN groundloop_m5_runtime_timing_contribution AS timing
              ON timing.epoch_id = evidence.epoch_id
             AND timing.subgraph = evidence.subgraph
             AND timing.attempt_id = evidence.attempt_id
            WHERE evidence.epoch_id = %s AND job.job_kind = %s
            """,
            (result.epoch_id, expected_job_kind),
        ).fetchall()
    assert len(attempt_rows) == 1
    attempt_row = attempt_rows[0]
    assert attempt_row[:3] == (
        "failed",
        "retryable_failed" if retryable else "terminal_failed",
        "retryable_failure" if retryable else "terminal_failure",
    )
    assert isinstance(attempt_row[3], str) and len(attempt_row[3].strip()) == 64
    assert attempt_row[4:7] == (
        failure_work.work_digest,
        failure_work.work_digest,
        True,
    )
    assert attempt_row[7:] == (
        _ATTEMPT_TIMING.coordinator_non_db_non_neural_ns,
        _ATTEMPT_TIMING.neural_wall_ns,
        _ATTEMPT_TIMING.postgres_roundtrip_wall_ns,
        _ATTEMPT_TIMING.external_io_wall_ns,
        _ATTEMPT_TIMING.end_to_end_wall_ns,
    )
    expected_transition_kinds = (
        M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        M5RuntimeWorkContributionKind.M5_ACQUISITION,
        *(
            (
                M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
                M5RuntimeWorkContributionKind.ROOT_BARRIER,
                M5RuntimeWorkContributionKind.M5_ACQUISITION,
            )
            if stage == "verifier"
            else ()
        ),
        M5RuntimeWorkContributionKind.M5_ATTEMPT_EXECUTION,
    )
    transition_rows = _assert_transition_calls_persisted(
        database,
        epoch_id=result.epoch_id,
        measurements=measurements,
        expected_kinds=expected_transition_kinds,
    )
    if retryable:
        assert result.state is M5RunState.BLOCKED
        assert not measurements.terminal_calls
        assert (
            database.store.read_typed_event_result(
                plan.structural_event_id,
                plan.payload_hash,
            )
            is None
        )
        return

    assert result.state is M5RunState.FAILED
    assert len(measurements.terminal_calls) == 1
    canonical = database.store.read_typed_event_result(
        plan.structural_event_id,
        plan.payload_hash,
    )
    assert canonical is not None
    assert canonical.state is M5RunState.REPLAYED
    assert canonical.call_work.is_zero
    assert canonical.event_work == result.event_work
    assert canonical.event_timing == result.event_timing
    assert canonical.logical_result_hash == result.logical_result_hash

    with database.reconnect() as bound:
        reconnect_discovery = ControlledDiscovery(database, include_pair=True)
        reconnect_verifier = ControlledVerifier(database)
        reconnect_measurements = ControlledMeasurements()
        reconnect_application = assemble_application(
            database,
            reconnect_discovery,
            reconnect_verifier,
            reconnect_measurements,
            bound=bound,
        )
        replay = reconnect_application.run_event(plan)
    assert replay.state is M5RunState.REPLAYED
    assert replay.call_work.is_zero
    assert replay.event_work == result.event_work
    assert replay.event_timing == result.event_timing
    assert replay.logical_result_hash == result.logical_result_hash
    assert reconnect_discovery.calls == []
    assert reconnect_verifier.calls == []
    assert reconnect_measurements.transition_calls == []
    assert len(reconnect_measurements.terminal_calls) == 1
    assert (
        _durable_transition_rows(database, epoch_id=result.epoch_id) == transition_rows
    )
    assert relation_rows(
        database.connection,
        "groundloop_m5_postcommit_invocation_telemetry",
        ("structural_event_id", "epoch_id", "terminal_logical_result_hash"),
    ) == (
        (
            plan.structural_event_id,
            result.epoch_id,
            result.logical_result_hash,
        ),
        (
            plan.structural_event_id,
            result.epoch_id,
            result.logical_result_hash,
        ),
    )


def test_successful_requirement_history_stops_before_seal_or_publication(
    d24_application_db: ApplicationD24Database,
) -> None:
    database = d24_application_db
    plan = database.register_plan(tag="pre-seal-success")
    discovery = ControlledDiscovery(
        database,
        include_pair=False,
        call_work=M5RuntimeWork(
            requirement_forward_retrieval_call_count=1,
            embedding_model_call_count=1,
        ),
        attempt_timing=_ATTEMPT_TIMING,
    )
    verifier = ControlledVerifier(database)
    measurements = ControlledMeasurements()
    publication_heads_before = (
        relation_rows(
            database.connection,
            "groundloop_m4_publication_head",
            ("singleton", "epoch_id"),
        ),
        relation_rows(
            database.connection,
            "groundloop_m5_publication_head",
            ("singleton", "epoch_id", "sealed_revision"),
        ),
    )
    application = assemble_application(database, discovery, verifier, measurements)
    with pytest.raises(ValidationError, match=_PRE_SEAL_ERROR):
        application.run_event(plan)

    assert len(discovery.calls) == 1
    assert verifier.calls == []
    assert measurements.terminal_calls == []
    assert publication_heads_before == (
        relation_rows(
            database.connection,
            "groundloop_m4_publication_head",
            ("singleton", "epoch_id"),
        ),
        relation_rows(
            database.connection,
            "groundloop_m5_publication_head",
            ("singleton", "epoch_id", "sealed_revision"),
        ),
    )
    assert (
        relation_rows(
            database.connection,
            "groundloop_m5_event_result",
            ("structural_event_id",),
        )
        == ()
    )
    assert (
        relation_rows(
            database.connection,
            "groundloop_m5_postcommit_invocation_telemetry",
            ("invocation_id",),
        )
        == ()
    )
    runtime_row = relation_rows(
        database.connection,
        "groundloop_m5_runtime_epoch",
        ("epoch_id", "runtime_state", "revision"),
    )
    assert len(runtime_row) == 1
    epoch_id, runtime_state, revision = runtime_row[0]
    assert isinstance(epoch_id, int)
    assert runtime_state == "semantic_pending"
    assert isinstance(revision, int)
    _assert_transition_calls_persisted(
        database,
        epoch_id=epoch_id,
        measurements=measurements,
        expected_kinds=(
            M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
            M5RuntimeWorkContributionKind.M5_ACQUISITION,
            M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
            M5RuntimeWorkContributionKind.ROOT_BARRIER,
        ),
    )
    assert relation_rows(
        database.connection,
        "groundloop_m5_requirement_discovery_result",
        ("staged_epoch_id", "root_job_id"),
    )

    before_rejection = database_snapshot(database.connection)
    with pytest.raises(ValidationError, match=_PRE_SEAL_ERROR):
        database.facade.request_typed_seal_atomically(
            epoch_id,
            revision,
            plan,
            database.store.current_event_work(epoch_id),
        )
    assert database_snapshot(database.connection) == before_rejection


def test_retirement_empty_root_history_reaches_only_fail_closed_pre_seal(
    d24_application_db: ApplicationD24Database,
) -> None:
    database = d24_application_db
    plan = database.retire_plan(tag="pre-seal-retirement")
    discovery = ControlledDiscovery(database)
    verifier = ControlledVerifier(database)
    measurements = ControlledMeasurements()
    heads_before = (
        relation_rows(
            database.connection,
            "groundloop_m4_publication_head",
            ("singleton", "epoch_id"),
        ),
        relation_rows(
            database.connection,
            "groundloop_m5_publication_head",
            ("singleton", "epoch_id", "sealed_revision"),
        ),
    )

    with pytest.raises(ValidationError, match=_PRE_SEAL_ERROR):
        assemble_application(
            database,
            discovery,
            verifier,
            measurements,
        ).run_event(plan)

    assert discovery.calls == []
    assert verifier.calls == []
    assert measurements.terminal_calls == []
    assert heads_before == (
        relation_rows(
            database.connection,
            "groundloop_m4_publication_head",
            ("singleton", "epoch_id"),
        ),
        relation_rows(
            database.connection,
            "groundloop_m5_publication_head",
            ("singleton", "epoch_id", "sealed_revision"),
        ),
    )
    assert (
        relation_rows(
            database.connection,
            "groundloop_m5_semantic_job",
            ("logical_job_id",),
        )
        == ()
    )
    assert (
        relation_rows(
            database.connection,
            "groundloop_m5_event_result",
            ("structural_event_id",),
        )
        == ()
    )
    assert (
        relation_rows(
            database.connection,
            "groundloop_m5_postcommit_invocation_telemetry",
            ("invocation_id",),
        )
        == ()
    )


def test_facade_constructor_requires_exact_concrete_types(
    d24_application_db: ApplicationD24Database,
) -> None:
    database = d24_application_db
    before = database_snapshot(database.connection)
    bad_store = cast(PostgresM5RuntimeStore, object())
    bad_manifest = cast(M5CandidatePolicyManifest, object())
    bad_config = cast(M5RuntimeOperationalConfig, object())
    with pytest.raises(ValidationError, match="store must"):
        PostgresM5GroupRequirementPreSealPorts(
            bad_store, database.manifest, database.operational_config
        )
    with pytest.raises(ValidationError, match="manifest must"):
        PostgresM5GroupRequirementPreSealPorts(
            database.store, bad_manifest, database.operational_config
        )
    with pytest.raises(ValidationError, match="operational_config must"):
        PostgresM5GroupRequirementPreSealPorts(
            database.store, database.manifest, bad_config
        )
    assert database_snapshot(database.connection) == before
