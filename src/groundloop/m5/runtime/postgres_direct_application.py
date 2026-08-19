"""PostgreSQL pre-seal application bridge for typed document events.

The bridge deliberately receives external discovery and verifier providers.
R2e owns their transaction-free orchestration, but does not claim production
model or measurement providers and does not expose a production seal route.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from typing import Any, Protocol

from psycopg import Cursor

from groundloop.domain import (
    ChunkVersion,
    DocumentVersion,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DiscoveryResult,
    DynamicEventPlan,
    ObservationCompletionReceipt,
    OpenEventReceipt,
    StructuralWithdrawal,
    VerificationResult,
)
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
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
    stable_m4_digest,
)
from groundloop.m4.pipeline import (
    InsertedDocument,
    M4ExecutionMode,
    StructuralPayload,
)
from groundloop.m4.runtime.withdrawal import WithdrawalPlan
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5DirectExecutionReceipt,
    M5DirectOpenPlan,
    M5ExternalWorkFailure,
    M5PostSealAuditPort,
    M5RequirementDiscoveryPort,
    M5RequirementRootDeclaration,
    M5RequirementVerifierPort,
    M5RuntimeMeasurementPort,
    M5TypedApplication,
    _append_transition_anchor_checked,
)
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5AcquisitionDisposition,
    M5CandidatePolicyManifest,
    M5CheckedDirectTerminalFailureReceipt,
    M5DirectAttemptReturnReceipt,
    M5DirectCursorContributionReceipt,
    M5DirectLateReturnDisposition,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5EventRunResult,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5LogicalJobSpec,
    M5RequirementFallbackKey,
    M5RequirementWithdrawalPlan,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeOperationalConfig,
    M5RuntimeSubgraph,
    M5RuntimeTiming,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectAcquisitionReceipt,
    M5TypedDirectJobLease,
    M5TypedDirectLateReturnEnvelope,
    M5TypedDirectReturnKind,
    M5TypedDirectScopeKind,
    M5TypedDirectTerminalProjection,
    M5TypedDirectVerificationExecution,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
)
from groundloop.m5.runtime.direct_m4 import (
    PostgresM5DirectM4Adapter,
    _DirectRetryableFailureOuterReceipt,
)
from groundloop.m5.runtime.frontier import (
    M5WithdrawnCandidateEdge,
    M5WithdrawnObservationEdge,
    coalesce_forward_root_keys,
    plan_requirement_withdrawal,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from groundloop.m5.runtime.postgres_application import (
    PostgresM5GroupRequirementPreSealPorts,
)

_DOCUMENT_EVENT_TYPES = (
    InsertDocumentEvent,
    DeleteDocumentVersionEvent,
    ReplaceDocumentVersionEvent,
)
_DocumentEvent = (
    InsertDocumentEvent | DeleteDocumentVersionEvent | ReplaceDocumentVersionEvent
)


def _sum_work(*items: M5RuntimeWork) -> M5RuntimeWork:
    for item in items:
        _validate_exact_work(item)
    return M5RuntimeWork(
        **{
            name: sum(getattr(item, name) for item in items)
            for name in M5RuntimeWork.counter_names()
        }
    )


def _validate_exact_work(work: M5RuntimeWork) -> None:
    if type(work) is not M5RuntimeWork:
        raise ValidationError("direct runtime work must be exact")
    if any(type(value) is not int for value in work.counter_values()):
        raise ValidationError("direct runtime work contains a nonexact counter")
    if type(work.work_digest) is not str:
        raise ValidationError("direct runtime work contains a nonexact digest")
    replace(work)


def _validate_exact_timing(timing: M5RuntimeTiming) -> None:
    if type(timing) is not M5RuntimeTiming:
        raise ValidationError("direct runtime timing must be exact")
    if any(
        value is not None and type(value) is not int
        for descriptor in fields(M5RuntimeTiming)
        for value in (getattr(timing, descriptor.name),)
    ):
        raise ValidationError("direct runtime timing contains a nonexact counter")
    replace(timing)


def _snapshot_exact_work(work: M5RuntimeWork) -> M5RuntimeWork:
    _validate_exact_work(work)
    return M5RuntimeWork(
        **{name: getattr(work, name) for name in M5RuntimeWork.counter_names()}
    )


def _snapshot_exact_timing(timing: M5RuntimeTiming | None) -> M5RuntimeTiming | None:
    if timing is None:
        return None
    _validate_exact_timing(timing)
    return replace(timing)


def _require_positive_int(name: str, value: int) -> None:
    if type(value) is not int or value < 1:
        raise ValidationError(f"{name} must be an exact positive integer")


def _require_exact_text(name: str, value: object, *, nonempty: bool = True) -> str:
    if type(value) is not str or (nonempty and not value.strip()):
        raise ValidationError(f"{name} must be an exact nonempty string")
    return value


def _require_exact_digest(name: str, value: object) -> str:
    checked = _require_exact_text(name, value)
    if len(checked) != 64 or any(
        character not in "0123456789abcdef" for character in checked
    ):
        raise ValidationError(f"{name} must be an exact lowercase SHA-256 digest")
    return checked


def _require_exact_string_tuple(name: str, value: object) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise ValidationError(f"{name} must be an exact tuple")
    for index, item in enumerate(value):
        _require_exact_text(f"{name}[{index}]", item)
    return value


def _validated_pair(pair: PairKey) -> PairKey:
    if type(pair) is not PairKey:
        raise ValidationError("direct pair must be exact")
    return PairKey(
        _require_exact_text("direct pair claim_id", pair.claim_id),
        _require_exact_text("direct pair chunk_version_id", pair.chunk_version_id),
    )


def _validated_dynamic_plan(plan: DynamicEventPlan) -> DynamicEventPlan:
    if type(plan) is not DynamicEventPlan:
        raise ValidationError("direct plan must be exact")
    update = plan.update
    if type(update) is not CorpusUpdateIdentity:
        raise ValidationError("direct update identity must be exact")
    if type(update.update_kind) is not UpdateKind:
        raise ValidationError("direct update kind must be exact")
    previous = update.previous_published_epoch_id
    if previous is not None and (type(previous) is not int or previous < 1):
        raise ValidationError("direct previous epoch must be an exact positive integer")
    checked_update = CorpusUpdateIdentity(
        event_id=_require_exact_text("direct update event_id", update.event_id),
        payload_hash=_require_exact_digest(
            "direct update payload_hash", update.payload_hash
        ),
        update_kind=update.update_kind,
        previous_published_epoch_id=previous,
        candidate_policy_id=_require_exact_text(
            "direct update candidate_policy_id", update.candidate_policy_id
        ),
    )
    checked = DynamicEventPlan(
        update=checked_update,
        inserted_chunk_version_ids=_require_exact_string_tuple(
            "direct inserted chunks", plan.inserted_chunk_version_ids
        ),
        deactivated_chunk_version_ids=_require_exact_string_tuple(
            "direct deactivated chunks", plan.deactivated_chunk_version_ids
        ),
        registered_claim_ids=_require_exact_string_tuple(
            "direct registered claims", plan.registered_claim_ids
        ),
        claim_registry_snapshot_id=_require_exact_text(
            "direct registry snapshot", plan.claim_registry_snapshot_id
        ),
    )
    if checked != plan:
        raise ValidationError("direct plan reconstruction changed")
    return checked


def _validated_logical_job(job: LogicalJobSpec) -> LogicalJobSpec:
    if type(job) is not LogicalJobSpec:
        raise ValidationError("direct logical job must be exact")
    if type(job.kind) is not JobKind or type(job.expandable) is not bool:
        raise ValidationError("direct logical job kind or flag must be exact")
    pair = None if job.pair is None else _validated_pair(job.pair)
    parent = job.parent_job_id
    target_claim = job.target_claim_id
    target_chunk = job.target_chunk_version_id
    for name, value in (
        ("parent_job_id", parent),
        ("target_claim_id", target_claim),
        ("target_chunk_version_id", target_chunk),
    ):
        if value is not None:
            _require_exact_text(f"direct job {name}", value)
    checked = LogicalJobSpec(
        job_id=_require_exact_digest("direct job_id", job.job_id),
        event_id=_require_exact_text("direct job event_id", job.event_id),
        kind=job.kind,
        candidate_policy_id=_require_exact_text(
            "direct job candidate_policy_id", job.candidate_policy_id
        ),
        payload_hash=_require_exact_digest("direct job payload_hash", job.payload_hash),
        execution_spec_hash=_require_exact_digest(
            "direct job execution_spec_hash", job.execution_spec_hash
        ),
        parent_job_id=parent,
        pair=pair,
        target_claim_id=target_claim,
        target_chunk_version_id=target_chunk,
        expandable=job.expandable,
    )
    if checked != job:
        raise ValidationError("direct logical job reconstruction changed")
    return checked


def _validated_discovery_scope(scope: DiscoveryScope) -> DiscoveryScope:
    if type(scope) is not DiscoveryScope or type(scope.closed) is not bool:
        raise ValidationError("direct discovery scope must be exact")
    checked = DiscoveryScope(
        _require_exact_digest("direct scope root_job_id", scope.root_job_id),
        _require_exact_text(
            "direct scope registry_snapshot_id", scope.registry_snapshot_id
        ),
        _require_exact_string_tuple(
            "direct scope registered_claim_ids", scope.registered_claim_ids
        ),
        closed=scope.closed,
    )
    if checked != scope:
        raise ValidationError("direct discovery scope reconstruction changed")
    return checked


def _validated_structural_payload(payload: StructuralPayload) -> StructuralPayload:
    if type(payload) is not StructuralPayload:
        raise ValidationError("direct structural payload must be exact")
    deactivated = payload.deactivated_document_version_id
    if deactivated is not None:
        _require_exact_text("deactivated document version", deactivated)
    inserted = payload.inserted
    if inserted is None:
        checked = StructuralPayload(deactivated_document_version_id=deactivated)
    else:
        if type(inserted) is not InsertedDocument:
            raise ValidationError("direct inserted document must be exact")
        version = inserted.version
        if type(version) is not DocumentVersion:
            raise ValidationError("direct document version must be exact")
        checked_version = DocumentVersion(
            _require_exact_text(
                "direct document_version_id", version.document_version_id
            ),
            _require_exact_text("direct document_id", version.document_id),
            _require_exact_digest("direct content_hash", version.content_hash),
        )
        if type(inserted.chunks) is not tuple:
            raise ValidationError("direct structural chunks must be an exact tuple")
        checked_chunks: list[ChunkVersion] = []
        for chunk in inserted.chunks:
            if type(chunk) is not ChunkVersion or type(chunk.chunk_index) is not int:
                raise ValidationError("direct structural chunk must be exact")
            checked_chunks.append(
                ChunkVersion(
                    _require_exact_text(
                        "direct chunk_version_id", chunk.chunk_version_id
                    ),
                    _require_exact_text(
                        "direct chunk document_version_id",
                        chunk.document_version_id,
                    ),
                    chunk.chunk_index,
                    _require_exact_text(
                        "direct chunk text", chunk.text, nonempty=False
                    ),
                )
            )
        for name in ("source_uri", "chunker_artifact_id", "chunker_input_hash"):
            value = getattr(inserted, name)
            if value is not None:
                _require_exact_text(f"direct inserted {name}", value)
        checked_inserted = InsertedDocument(
            version=checked_version,
            chunks=tuple(checked_chunks),
            source_uri=inserted.source_uri,
            authority_class=_require_exact_text(
                "direct authority_class", inserted.authority_class
            ),
            chunker_version=_require_exact_text(
                "direct chunker_version", inserted.chunker_version
            ),
            chunker_artifact_id=inserted.chunker_artifact_id,
            chunker_input_hash=inserted.chunker_input_hash,
        )
        checked = StructuralPayload(
            inserted=checked_inserted,
            deactivated_document_version_id=deactivated,
        )
    if checked != payload:
        raise ValidationError("direct structural payload reconstruction changed")
    return checked


def _validated_structural_withdrawal(
    withdrawal: StructuralWithdrawal,
) -> StructuralWithdrawal:
    if type(withdrawal) is not StructuralWithdrawal:
        raise ValidationError("direct structural withdrawal must be exact")
    plan = withdrawal.plan
    if type(plan) is not WithdrawalPlan:
        raise ValidationError("direct withdrawal plan must be exact")
    pairs = plan.affected_pairs
    if type(pairs) is not tuple:
        raise ValidationError("direct withdrawal pairs must be an exact tuple")
    counters = (
        plan.chunk_lookups,
        plan.observation_edge_visits,
        plan.candidate_edge_visits,
    )
    if any(type(value) is not int or value < 0 for value in counters):
        raise ValidationError("direct withdrawal counters must be exact")
    checked_plan = WithdrawalPlan(
        deactivated_chunk_ids=_require_exact_string_tuple(
            "direct withdrawal chunks", plan.deactivated_chunk_ids
        ),
        observation_ids=_require_exact_string_tuple(
            "direct withdrawal observations", plan.observation_ids
        ),
        candidate_edge_ids=_require_exact_string_tuple(
            "direct withdrawal candidates", plan.candidate_edge_ids
        ),
        affected_pairs=tuple(_validated_pair(pair) for pair in pairs),
        affected_claim_ids=_require_exact_string_tuple(
            "direct withdrawal claims", plan.affected_claim_ids
        ),
        chunk_lookups=plan.chunk_lookups,
        observation_edge_visits=plan.observation_edge_visits,
        candidate_edge_visits=plan.candidate_edge_visits,
    )
    checked = StructuralWithdrawal(
        checked_plan,
        _require_exact_string_tuple(
            "direct fallback claims", withdrawal.fallback_claim_ids
        ),
    )
    if checked != withdrawal:
        raise ValidationError("direct structural withdrawal reconstruction changed")
    return checked


def _validated_open_receipt(
    receipt: OpenEventReceipt, *, epoch_id: int
) -> OpenEventReceipt:
    if (
        type(receipt) is not OpenEventReceipt
        or type(receipt.epoch_id) is not int
        or receipt.epoch_id < 1
        or type(receipt.replayed) is not bool
        or type(receipt.already_sealed) is not bool
        or type(receipt.already_failed) is not bool
        or (
            receipt.publication_id is not None
            and type(receipt.publication_id) is not str
        )
        or (
            receipt.failure_reason is not None
            and type(receipt.failure_reason) is not str
        )
        or receipt.epoch_id != epoch_id
        or receipt.already_sealed
        or receipt.publication_id is not None
        or receipt.already_failed
        or receipt.failure_reason is not None
    ):
        raise ValidationError(
            "typed-direct runner requires the exact held nonterminal receipt"
        )
    if replace(receipt) != receipt:
        raise ValidationError("held direct receipt reconstruction changed")
    return receipt


def _validated_document_event(event: object) -> _DocumentEvent:
    if type(event) not in _DOCUMENT_EVENT_TYPES or not isinstance(
        event, _DOCUMENT_EVENT_TYPES
    ):
        raise ValidationError("typed-direct facade admits only document events")
    if isinstance(event, (InsertDocumentEvent, ReplaceDocumentVersionEvent)):
        if type(event.chunks) is not tuple:
            raise ValidationError("document chunks must be an immutable tuple")
        chunks: list[ChunkInput] = []
        for chunk in event.chunks:
            if type(chunk) is not ChunkInput or type(chunk.chunk_index) is not int:
                raise ValidationError("document event carries another chunk type")
            chunks.append(
                ChunkInput(
                    _require_exact_text(
                        "document chunk_version_id", chunk.chunk_version_id
                    ),
                    chunk.chunk_index,
                    _require_exact_text(
                        "document chunk text", chunk.text, nonempty=False
                    ),
                )
            )
        if isinstance(event, InsertDocumentEvent):
            return InsertDocumentEvent(
                _require_exact_text("document event_id", event.event_id),
                _require_exact_text("document_id", event.document_id),
                _require_exact_text("document_version_id", event.document_version_id),
                _require_exact_digest("document content_hash", event.content_hash),
                tuple(chunks),
            )
        return ReplaceDocumentVersionEvent(
            _require_exact_text("document event_id", event.event_id),
            _require_exact_text("document_id", event.document_id),
            _require_exact_text(
                "old_document_version_id", event.old_document_version_id
            ),
            _require_exact_text(
                "new_document_version_id", event.new_document_version_id
            ),
            _require_exact_digest("document content_hash", event.content_hash),
            tuple(chunks),
        )
    assert isinstance(event, DeleteDocumentVersionEvent)
    return DeleteDocumentVersionEvent(
        _require_exact_text("document event_id", event.event_id),
        _require_exact_text("document_version_id", event.document_version_id),
    )


def _validated_requirement_registry_snapshot(
    snapshot: RequirementRegistrySnapshot,
) -> RequirementRegistrySnapshot:
    if (
        type(snapshot) is not RequirementRegistrySnapshot
        or type(snapshot.requirement_count) is not int
        or type(snapshot.entries) is not tuple
    ):
        raise ValidationError("requirement registry snapshot must be recursively exact")
    entries: list[RequirementRegistrySnapshotEntry] = []
    for entry in snapshot.entries:
        if type(entry) is not RequirementRegistrySnapshotEntry:
            raise ValidationError("requirement registry entry must be exact")
        checked_entry = RequirementRegistrySnapshotEntry(
            requirement_version_id=_require_exact_text(
                "requirement snapshot requirement_version_id",
                entry.requirement_version_id,
            ),
            group_version_id=_require_exact_text(
                "requirement snapshot group_version_id", entry.group_version_id
            ),
            group_family_id=_require_exact_text(
                "requirement snapshot group_family_id", entry.group_family_id
            ),
            owner_claim_id=_require_exact_text(
                "requirement snapshot owner_claim_id", entry.owner_claim_id
            ),
            normalized_requirement_text=_require_exact_text(
                "requirement snapshot normalized text",
                entry.normalized_requirement_text,
            ),
            requirement_text_hash=_require_exact_digest(
                "requirement snapshot text hash", entry.requirement_text_hash
            ),
        )
        if checked_entry != entry:
            raise ValidationError("requirement registry entry reconstruction changed")
        entries.append(checked_entry)
    checked = RequirementRegistrySnapshot(
        requirement_count=snapshot.requirement_count,
        entries=tuple(entries),
        requirement_registry_snapshot_digest=_require_exact_digest(
            "requirement registry snapshot digest",
            snapshot.requirement_registry_snapshot_digest,
        ),
    )
    if checked != snapshot:
        raise ValidationError("requirement registry snapshot reconstruction changed")
    return checked


def _validated_active_chunk_snapshot(
    snapshot: ActiveChunkSnapshot,
) -> ActiveChunkSnapshot:
    if (
        type(snapshot) is not ActiveChunkSnapshot
        or type(snapshot.chunk_count) is not int
        or type(snapshot.entries) is not tuple
    ):
        raise ValidationError("active chunk snapshot must be recursively exact")
    entries: list[ActiveChunkSnapshotEntry] = []
    for entry in snapshot.entries:
        if type(entry) is not ActiveChunkSnapshotEntry:
            raise ValidationError("active chunk snapshot entry must be exact")
        checked_entry = ActiveChunkSnapshotEntry(
            chunk_version_id=_require_exact_text(
                "active chunk snapshot chunk_version_id", entry.chunk_version_id
            ),
            text_hash=_require_exact_digest(
                "active chunk snapshot text_hash", entry.text_hash
            ),
        )
        if checked_entry != entry:
            raise ValidationError("active chunk snapshot entry reconstruction changed")
        entries.append(checked_entry)
    checked = ActiveChunkSnapshot(
        chunk_count=snapshot.chunk_count,
        entries=tuple(entries),
        active_chunk_snapshot_digest=_require_exact_digest(
            "active chunk snapshot digest", snapshot.active_chunk_snapshot_digest
        ),
    )
    if checked != snapshot:
        raise ValidationError("active chunk snapshot reconstruction changed")
    return checked


def _validated_document_plan(event: M5TypedEventPlan) -> M5TypedEventPlan:
    if type(event) is not M5TypedEventPlan:
        raise ValidationError("event must be an exact M5TypedEventPlan")
    concrete = _validated_document_event(event.event)
    _require_exact_text("typed event structural_event_id", event.structural_event_id)
    _require_exact_digest("typed event payload_hash", event.payload_hash)
    _require_exact_text("typed event candidate_policy_id", event.candidate_policy_id)
    _require_exact_digest(
        "typed event candidate manifest hash",
        event.candidate_policy_manifest_hash,
    )
    if (
        type(event.expected_previous_published_epoch_id) is not int
        or event.expected_previous_published_epoch_id < 1
    ):
        raise ValidationError("typed event predecessor must be exact and positive")
    direct = event.direct_plan
    if direct is None:
        raise ValidationError("document event requires its exact direct plan")
    checked_direct = _validated_dynamic_plan(direct)
    checked = M5TypedEventPlan(
        structural_event_id=_require_exact_text(
            "typed event structural_event_id", event.structural_event_id
        ),
        event=concrete,
        payload_hash=_require_exact_digest(
            "typed event payload_hash", event.payload_hash
        ),
        direct_plan=checked_direct,
        candidate_policy_id=_require_exact_text(
            "typed event candidate_policy_id", event.candidate_policy_id
        ),
        candidate_policy_manifest_hash=_require_exact_digest(
            "typed event candidate manifest hash",
            event.candidate_policy_manifest_hash,
        ),
        requirement_registry_snapshot=_validated_requirement_registry_snapshot(
            event.requirement_registry_snapshot
        ),
        active_chunk_snapshot=_validated_active_chunk_snapshot(
            event.active_chunk_snapshot
        ),
        expected_previous_published_epoch_id=(
            event.expected_previous_published_epoch_id
        ),
    )
    inserted = (
        ()
        if isinstance(concrete, DeleteDocumentVersionEvent)
        else tuple(sorted(chunk.chunk_version_id for chunk in concrete.chunks))
    )
    if inserted != checked_direct.inserted_chunk_version_ids:
        raise ValidationError("typed document chunks disagree with its direct plan")
    if (
        concrete.event_id != event.structural_event_id
        or checked_direct.update.event_id != event.structural_event_id
        or checked_direct.update.payload_hash != event.payload_hash
        or checked_direct.update.candidate_policy_id != event.candidate_policy_id
        or checked_direct.update.previous_published_epoch_id
        != event.expected_previous_published_epoch_id
    ):
        raise ValidationError("typed document event and direct plan are not bound")
    if checked != event:
        raise ValidationError("typed document event reconstruction changed")
    return checked


def _validated_observation_completion(
    receipt: ObservationCompletionReceipt,
) -> ObservationCompletionReceipt:
    if (
        type(receipt) is not ObservationCompletionReceipt
        or type(receipt.artifact_stored) is not bool
        or type(receipt.made_effective) is not bool
    ):
        raise ValidationError("direct observation completion must be exact")
    checked = ObservationCompletionReceipt(
        artifact_stored=receipt.artifact_stored,
        made_effective=receipt.made_effective,
    )
    if checked != receipt:
        raise ValidationError("direct observation completion reconstruction changed")
    return checked


def _validated_transition_anchor(
    anchor: M5TransitionTimingAnchor,
) -> M5TransitionTimingAnchor:
    if (
        type(anchor) is not M5TransitionTimingAnchor
        or type(anchor.epoch_id) is not int
        or type(anchor.contribution_kind) is not M5RuntimeWorkContributionKind
        or type(anchor.source_id) is not str
        or type(anchor.contribution_key_digest) is not str
        or type(anchor.anchor_revision) is not int
        or type(anchor.terminal_transition) is not bool
    ):
        raise ValidationError("direct transition anchor must be exact")
    checked = replace(anchor)
    if checked != anchor:
        raise ValidationError("direct transition anchor reconstruction changed")
    return checked


def _validated_direct_cursor_receipt(
    receipt: M5DirectCursorContributionReceipt,
) -> M5DirectCursorContributionReceipt:
    if (
        type(receipt) is not M5DirectCursorContributionReceipt
        or type(receipt.epoch_id) is not int
        or type(receipt.job_id) is not str
        or type(receipt.attempt_id) is not str
        or type(receipt.execution_evidence_digest) is not str
        or type(receipt.attempt_execution_contribution_key_digest) is not str
    ):
        raise ValidationError("direct cursor contribution receipt must be exact")
    for name in (
        "direct_transition_source_id",
        "direct_transition_source_identity_hash",
        "direct_transition_contribution_key_digest",
    ):
        value = getattr(receipt, name)
        if value is not None and type(value) is not str:
            raise ValidationError("direct cursor contribution contains nonexact text")
    observation = (
        None
        if receipt.observation_completion is None
        else _validated_observation_completion(receipt.observation_completion)
    )
    checked = M5DirectCursorContributionReceipt(
        epoch_id=receipt.epoch_id,
        job_id=receipt.job_id,
        attempt_id=receipt.attempt_id,
        execution_evidence_digest=receipt.execution_evidence_digest,
        attempt_execution_contribution_key_digest=(
            receipt.attempt_execution_contribution_key_digest
        ),
        direct_transition_source_id=receipt.direct_transition_source_id,
        direct_transition_source_identity_hash=(
            receipt.direct_transition_source_identity_hash
        ),
        direct_transition_contribution_key_digest=(
            receipt.direct_transition_contribution_key_digest
        ),
        observation_completion=observation,
    )
    if checked != receipt:
        raise ValidationError("direct cursor contribution reconstruction changed")
    return checked


def _validated_direct_acquisition(
    receipt: M5TypedDirectAcquisitionReceipt,
) -> M5TypedDirectAcquisitionReceipt:
    if type(receipt) is not M5TypedDirectAcquisitionReceipt:
        raise ValidationError("typed-direct acquisition must have its exact type")
    job = _validated_logical_job(receipt.job)
    lease = receipt.lease
    if type(lease) is not M5TypedDirectJobLease:
        raise ValidationError("typed-direct lease must have its exact type")
    projection = lease.terminal_projection
    checked_projection: M5TypedDirectTerminalProjection | None = None
    if projection is not None:
        if type(projection) is not M5TypedDirectTerminalProjection:
            raise ValidationError("typed-direct terminal projection must be exact")
        checked_projection = M5TypedDirectTerminalProjection(
            terminal_state=projection.terminal_state,
            terminal_reason=projection.terminal_reason,
            m4_completion_digest=projection.m4_completion_digest,
            completed_revision=projection.completed_revision,
            terminal_identity_hash=projection.terminal_identity_hash,
        )
        if checked_projection != projection:
            raise ValidationError("typed-direct terminal projection changed")
    checked_lease = M5TypedDirectJobLease(
        job_id=lease.job_id,
        attempt_id=lease.attempt_id,
        lease_token_hash=lease.lease_token_hash,
        lease_expires_at=lease.lease_expires_at,
        dispatch_record_digest=lease.dispatch_record_digest,
        resulting_revision=lease.resulting_revision,
        disposition=lease.disposition,
        should_execute=lease.should_execute,
        exact_replay=lease.exact_replay,
        already_completed=lease.already_completed,
        terminal_projection=checked_projection,
    )
    attempt = receipt.attempt
    checked_attempt = (
        None
        if attempt is None
        else JobAttempt(
            attempt_id=attempt.attempt_id,
            job_id=attempt.job_id,
            execution_spec_hash=attempt.execution_spec_hash,
            attempt_ordinal=attempt.attempt_ordinal,
            lease_token_hash=attempt.lease_token_hash,
        )
    )
    checked = M5TypedDirectAcquisitionReceipt(
        epoch_id=receipt.epoch_id,
        job=job,
        lease=checked_lease,
        attempt=checked_attempt,
    )
    if checked != receipt:
        raise ValidationError("typed-direct acquisition reconstruction changed")
    return checked


def _validated_direct_attempt_return(
    receipt: M5DirectAttemptReturnReceipt,
) -> M5DirectAttemptReturnReceipt:
    if (
        type(receipt) is not M5DirectAttemptReturnReceipt
        or type(receipt.return_kind) is not M5TypedDirectReturnKind
    ):
        raise ValidationError("direct settlement returned another receipt type")
    normal: M5DirectNormalReturnReceipt | None = None
    late: M5DirectLateReturnReceipt | None = None
    if receipt.normal is not None:
        normal_branch = receipt.normal
        if (
            type(normal_branch) is not M5DirectNormalReturnReceipt
            or type(normal_branch.epoch_id) is not int
            or type(normal_branch.job_id) is not str
            or type(normal_branch.attempt_id) is not str
            or type(normal_branch.resulting_revision) is not int
            or type(normal_branch.exact_replay) is not bool
        ):
            raise ValidationError("normal direct settlement branch must be exact")
        for name in (
            "execution_evidence_digest",
            "return_artifact_digest",
            "direct_transition_source_id",
            "direct_transition_source_identity_hash",
            "direct_transition_contribution_key_digest",
        ):
            if type(getattr(normal_branch, name)) is not str:
                raise ValidationError("normal direct settlement contains nonexact text")
        if (
            normal_branch.current_terminal_logical_result_hash is not None
            and type(normal_branch.current_terminal_logical_result_hash) is not str
        ):
            raise ValidationError("normal direct terminal hash must be exact")
        observation = (
            None
            if normal_branch.observation_completion is None
            else _validated_observation_completion(normal_branch.observation_completion)
        )
        anchor = (
            None
            if normal_branch.transition_anchor is None
            else _validated_transition_anchor(normal_branch.transition_anchor)
        )
        normal = M5DirectNormalReturnReceipt(
            epoch_id=normal_branch.epoch_id,
            job_id=normal_branch.job_id,
            attempt_id=normal_branch.attempt_id,
            resulting_revision=normal_branch.resulting_revision,
            exact_replay=normal_branch.exact_replay,
            execution_evidence_digest=normal_branch.execution_evidence_digest,
            return_artifact_digest=normal_branch.return_artifact_digest,
            direct_transition_source_id=normal_branch.direct_transition_source_id,
            direct_transition_source_identity_hash=(
                normal_branch.direct_transition_source_identity_hash
            ),
            direct_transition_contribution_key_digest=(
                normal_branch.direct_transition_contribution_key_digest
            ),
            observation_completion=observation,
            current_terminal_logical_result_hash=(
                normal_branch.current_terminal_logical_result_hash
            ),
            transition_anchor=anchor,
        )
    elif receipt.late is not None:
        late_branch = receipt.late
        if (
            type(late_branch) is not M5DirectLateReturnReceipt
            or type(late_branch.disposition) is not M5DirectLateReturnDisposition
            or type(late_branch.epoch_id) is not int
            or type(late_branch.job_id) is not str
            or type(late_branch.attempt_id) is not str
            or type(late_branch.resulting_revision) is not int
            or type(late_branch.exact_replay) is not bool
            or type(late_branch.envelope_digest) is not str
            or type(late_branch.execution_evidence_digest) is not str
        ):
            raise ValidationError("late direct settlement branch must be exact")
        for name in (
            "expired_return_digest",
            "current_terminal_logical_result_hash",
        ):
            value = getattr(late_branch, name)
            if value is not None and type(value) is not str:
                raise ValidationError("late direct settlement contains nonexact text")
        anchor = (
            None
            if late_branch.transition_anchor is None
            else _validated_transition_anchor(late_branch.transition_anchor)
        )
        late = M5DirectLateReturnReceipt(
            disposition=late_branch.disposition,
            epoch_id=late_branch.epoch_id,
            job_id=late_branch.job_id,
            attempt_id=late_branch.attempt_id,
            resulting_revision=late_branch.resulting_revision,
            exact_replay=late_branch.exact_replay,
            envelope_digest=late_branch.envelope_digest,
            execution_evidence_digest=late_branch.execution_evidence_digest,
            expired_return_digest=late_branch.expired_return_digest,
            current_terminal_logical_result_hash=(
                late_branch.current_terminal_logical_result_hash
            ),
            transition_anchor=anchor,
        )
    checked = M5DirectAttemptReturnReceipt(receipt.return_kind, normal, late)
    if checked != receipt:
        raise ValidationError("direct settlement receipt reconstruction changed")
    return checked


def _structural_payload(event: M5TypedEventPlan) -> StructuralPayload:
    concrete = event.event
    if isinstance(concrete, DeleteDocumentVersionEvent):
        return StructuralPayload(
            deactivated_document_version_id=concrete.document_version_id
        )
    if isinstance(concrete, InsertDocumentEvent):
        version_id = concrete.document_version_id
        deactivated = None
    elif isinstance(concrete, ReplaceDocumentVersionEvent):
        version_id = concrete.new_document_version_id
        deactivated = concrete.old_document_version_id
    else:
        raise ValidationError("direct structural payload requires a document event")
    chunks = tuple(
        sorted(
            (
                ChunkVersion(
                    chunk_version_id=item.chunk_version_id,
                    document_version_id=version_id,
                    chunk_index=item.chunk_index,
                    text=item.text,
                )
                for item in concrete.chunks
            ),
            key=lambda item: item.chunk_version_id,
        )
    )
    return StructuralPayload(
        inserted=InsertedDocument(
            version=DocumentVersion(
                version_id,
                concrete.document_id,
                concrete.content_hash,
            ),
            chunks=chunks,
        ),
        deactivated_document_version_id=deactivated,
    )


def _m4_job(
    event: M5TypedEventPlan,
    policy: ApplicationExecutionPolicy,
    *,
    kind: JobKind,
    parent_job_id: str | None = None,
    claim_id: str = "",
    chunk_id: str = "",
) -> LogicalJobSpec:
    if kind is JobKind.IMPACT_DISCOVERY:
        execution_hash = policy.impact_discovery_execution_spec_hash
    elif kind is JobKind.FRONTIER_RETRIEVE:
        execution_hash = policy.frontier_retrieval_execution_spec_hash
    else:
        execution_hash = policy.verifier_execution_spec_hash
    job_id = LogicalJobSpec.derive_job_id(
        event_id=event.structural_event_id,
        kind=kind,
        candidate_policy_id=event.candidate_policy_id,
        execution_spec_hash=execution_hash,
        parent_job_id=parent_job_id or "",
        claim_id=claim_id,
        chunk_version_id=chunk_id,
    )
    return LogicalJobSpec(
        job_id=job_id,
        event_id=event.structural_event_id,
        kind=kind,
        candidate_policy_id=event.candidate_policy_id,
        payload_hash=stable_m4_digest(
            "m4-application-job-payload-v1",
            event.payload_hash,
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


def _m4_roots(
    event: M5TypedEventPlan,
    withdrawal: StructuralWithdrawal,
    policy: ApplicationExecutionPolicy,
) -> tuple[LogicalJobSpec, ...]:
    assert event.direct_plan is not None
    roots = [
        _m4_job(event, policy, kind=JobKind.IMPACT_DISCOVERY, chunk_id=chunk_id)
        for chunk_id in event.direct_plan.inserted_chunk_version_ids
    ]
    roots.extend(
        _m4_job(event, policy, kind=JobKind.FRONTIER_RETRIEVE, claim_id=claim_id)
        for claim_id in withdrawal.fallback_claim_ids
    )
    ordered = tuple(sorted(roots, key=lambda item: item.job_id))
    if len({item.job_id for item in ordered}) != len(ordered):
        raise ValidationError("direct root declaration repeats a logical job")
    return ordered


def _requirement_roots(
    event: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
    withdrawal: M5RequirementWithdrawalPlan,
) -> tuple[M5RequirementRootDeclaration, ...]:
    forward = coalesce_forward_root_keys(
        new_requirement_keys=(), fallback_keys=withdrawal.fallback_keys
    )
    roots: list[M5RequirementRootDeclaration] = []
    for key in forward:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.FORWARD_REQUIREMENT,
            requirement_version_id=key.requirement_version_id,
            inserted_chunk_version_id=None,
            candidate_policy_id=event.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                event.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                event.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        scope.validate_snapshots(
            event.requirement_registry_snapshot, event.active_chunk_snapshot
        )
        roots.append(
            M5RequirementRootDeclaration(
                scope,
                M5LogicalJobSpec.build(
                    structural_event_id=event.structural_event_id,
                    job_kind=M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL,
                    manifest=manifest,
                    scope=scope,
                ),
            )
        )
    assert event.direct_plan is not None
    for chunk_id in event.direct_plan.inserted_chunk_version_ids:
        scope = M5DiscoveryScopeContract.build(
            direction=M5DiscoveryDirection.REVERSE_CHUNK,
            requirement_version_id=None,
            inserted_chunk_version_id=chunk_id,
            candidate_policy_id=event.candidate_policy_id,
            requirement_registry_snapshot_digest=(
                event.requirement_registry_snapshot.requirement_registry_snapshot_digest
            ),
            active_chunk_snapshot_digest=(
                event.active_chunk_snapshot.active_chunk_snapshot_digest
            ),
        )
        scope.validate_snapshots(
            event.requirement_registry_snapshot, event.active_chunk_snapshot
        )
        roots.append(
            M5RequirementRootDeclaration(
                scope,
                M5LogicalJobSpec.build(
                    structural_event_id=event.structural_event_id,
                    job_kind=M5JobKind.REVERSE_REQUIREMENT_DISCOVERY,
                    manifest=manifest,
                    scope=scope,
                ),
            )
        )
    ordered = tuple(sorted(roots, key=lambda item: item.job.logical_job_id))
    if len({item.job.logical_job_id for item in ordered}) != len(ordered):
        raise ValidationError("typed root declaration contains an identity collision")
    return ordered


def _validate_success_metadata(
    disposition: M5ExecutionEvidenceDisposition,
    call_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> None:
    if type(disposition) is not M5ExecutionEvidenceDisposition or disposition not in {
        M5ExecutionEvidenceDisposition.RETURNED,
        M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
    }:
        raise ValidationError("direct success requires returned or reused evidence")
    _validate_exact_work(call_work)
    if attempt_timing is not None:
        _validate_exact_timing(attempt_timing)
    if disposition is M5ExecutionEvidenceDisposition.REUSED_ARTIFACT and any(
        getattr(call_work, name)
        for name in M5RuntimeWork.counter_names()
        if name not in {"bytes_hashed", "bytes_serialized"}
    ):
        raise ValidationError("reused direct work may contain only byte totals")


def _expected_direct_execution_evidence_digest(
    *,
    epoch_id: int,
    attempt_id: str,
    disposition: M5ExecutionEvidenceDisposition,
    result_or_error_hash: str,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> str:
    _require_positive_int("direct evidence epoch_id", epoch_id)
    _require_exact_digest("direct evidence attempt_id", attempt_id)
    if type(disposition) is not M5ExecutionEvidenceDisposition:
        raise ValidationError("direct evidence disposition must be exact")
    _require_exact_digest("direct evidence result hash", result_or_error_hash)
    work = _snapshot_exact_work(attempt_work)
    timing = _snapshot_exact_timing(attempt_timing)
    observation = M5RuntimeTimingObservation.build(timing)
    timing_digest = digests.attempt_runtime_timing_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        observation_digest=observation.observation_digest,
    )
    return digests.attempt_execution_evidence_digest(
        epoch_id=epoch_id,
        subgraph=M5RuntimeSubgraph.DIRECT,
        attempt_id=attempt_id,
        disposition=disposition,
        result_or_error_hash=result_or_error_hash,
        attempt_work_digest=work.work_digest,
        attempt_timing_digest=timing_digest,
    )


def _validated_admitted_pair(admitted: AdmittedPair) -> AdmittedPair:
    if (
        type(admitted) is not AdmittedPair
        or type(admitted.epoch_id) is not int
        or type(admitted.fused_rank) is not int
        or type(admitted.mandatory_lineage) is not bool
        or type(admitted.reasons) is not tuple
        or any(type(reason) is not AdmissionChannel for reason in admitted.reasons)
    ):
        raise ValidationError("direct admitted pair must be recursively exact")
    checked = AdmittedPair(
        admitted.epoch_id,
        _validated_pair(admitted.pair),
        _require_exact_text(
            "direct admitted candidate_policy_id", admitted.candidate_policy_id
        ),
        admitted.fused_rank,
        admitted.reasons,
        admitted.mandatory_lineage,
    )
    if checked != admitted:
        raise ValidationError("direct admitted pair reconstruction changed")
    return checked


def _validated_channel_hit(hit: ChannelHit) -> ChannelHit:
    if (
        type(hit) is not ChannelHit
        or type(hit.epoch_id) is not int
        or type(hit.channel) is not AdmissionChannel
        or type(hit.rank) is not int
        or (hit.score is not None and type(hit.score) is not float)
    ):
        raise ValidationError("direct channel hit must be recursively exact")
    checked = ChannelHit(
        hit.epoch_id,
        _validated_pair(hit.pair),
        _require_exact_text("direct hit candidate_policy_id", hit.candidate_policy_id),
        hit.channel,
        hit.rank,
        hit.score,
        _require_exact_digest(
            "direct hit channel_artifact_hash", hit.channel_artifact_hash
        ),
    )
    if checked != hit:
        raise ValidationError("direct channel hit reconstruction changed")
    return checked


def _validated_discovery_result(result: DiscoveryResult) -> DiscoveryResult:
    if (
        type(result) is not DiscoveryResult
        or type(result.admitted_pairs) is not tuple
        or type(result.channel_hits) is not tuple
        or type(result.fallback_satisfied) is not bool
    ):
        raise ValidationError("direct discovery result must be recursively exact")
    checked = DiscoveryResult(
        root_job_id=_require_exact_digest(
            "direct discovery root_job_id", result.root_job_id
        ),
        result_artifact_id=_require_exact_text(
            "direct discovery result_artifact_id", result.result_artifact_id
        ),
        result_artifact_hash=_require_exact_digest(
            "direct discovery result_artifact_hash", result.result_artifact_hash
        ),
        admitted_pairs=tuple(
            _validated_admitted_pair(admitted) for admitted in result.admitted_pairs
        ),
        fallback_satisfied=result.fallback_satisfied,
        channel_hits=tuple(_validated_channel_hit(hit) for hit in result.channel_hits),
    )
    if checked != result:
        raise ValidationError("direct discovery result reconstruction changed")
    return checked


def _validated_semantic_observation(
    observation: SemanticObservation,
) -> SemanticObservation:
    if (
        type(observation) is not SemanticObservation
        or type(observation.subject_kind) is not SubjectKind
        or type(observation.support_score) is not float
        or type(observation.refute_score) is not float
        or type(observation.neutral_score) is not float
        or type(observation.producer) is not ModelStamp
    ):
        raise ValidationError("direct observation must be recursively exact")
    producer = observation.producer
    checked = SemanticObservation(
        observation_id=_require_exact_text(
            "direct observation_id", observation.observation_id
        ),
        subject_kind=observation.subject_kind,
        subject_id=_require_exact_text(
            "direct observation subject_id", observation.subject_id
        ),
        chunk_version_id=_require_exact_text(
            "direct observation chunk_version_id",
            observation.chunk_version_id,
        ),
        task_type=_require_exact_text(
            "direct observation task_type", observation.task_type
        ),
        support_score=observation.support_score,
        refute_score=observation.refute_score,
        neutral_score=observation.neutral_score,
        producer=ModelStamp(
            _require_exact_text("direct model_id", producer.model_id),
            _require_exact_text("direct model_version", producer.model_version),
            _require_exact_text("direct prompt_version", producer.prompt_version),
        ),
        input_hash=_require_exact_digest(
            "direct observation input_hash", observation.input_hash
        ),
    )
    if checked != observation:
        raise ValidationError("direct observation reconstruction changed")
    return checked


def _validated_verification_result(result: VerificationResult) -> VerificationResult:
    if type(result) is not VerificationResult:
        raise ValidationError("direct verification result must be exact")
    checked = VerificationResult(
        _require_exact_text(
            "direct verification result_artifact_id", result.result_artifact_id
        ),
        _require_exact_digest(
            "direct verification result_artifact_hash", result.result_artifact_hash
        ),
        _validated_semantic_observation(result.observation),
    )
    if checked != result:
        raise ValidationError("direct verification result reconstruction changed")
    return checked


def _validated_verification_execution(
    execution: M5TypedDirectVerificationExecution,
) -> M5TypedDirectVerificationExecution:
    if (
        type(execution) is not M5TypedDirectVerificationExecution
        or type(execution.temperature) is not float
        or type(execution.raw_logits) is not tuple
        or any(type(logit) is not float for logit in execution.raw_logits)
    ):
        raise ValidationError("direct verification execution must be recursively exact")
    for name in (
        "observation_id",
        "job_id",
        "model_artifact_id",
        "prompt_artifact_id",
        "calibration_version",
    ):
        _require_exact_text(f"direct verification {name}", getattr(execution, name))
    for name in (
        "admitted_pair_id",
        "execution_spec_hash",
        "pair_input_hash",
        "calibration_artifact_sha256",
        "raw_output_hash",
    ):
        _require_exact_digest(f"direct verification {name}", getattr(execution, name))
    if execution.reused_from_observation_id is not None:
        _require_exact_text(
            "direct verification reused_from_observation_id",
            execution.reused_from_observation_id,
        )
    checked = replace(execution)
    if checked != execution:
        raise ValidationError("direct verification execution reconstruction changed")
    return checked


class M5PostgresDirectExternalWorkFailure(M5ExternalWorkFailure):
    """Direct provider failure with an independent exact M4 terminal reason."""

    def __init__(
        self,
        requested_failure_reason: M5RunFailureReason,
        *,
        retryable: bool,
        direct_terminal_reason: str | None,
        call_work: M5RuntimeWork,
        attempt_timing: M5RuntimeTiming | None,
        error_hash: str,
    ) -> None:
        if type(requested_failure_reason) is not M5RunFailureReason:
            raise ValidationError("direct requested failure reason must be exact")
        if type(retryable) is not bool:
            raise ValidationError("direct failure retryability must be exact")
        if retryable:
            if direct_terminal_reason is not None:
                raise ValidationError(
                    "retryable direct failure cannot carry a terminal reason"
                )
        else:
            _require_exact_text(
                "direct provider terminal reason", direct_terminal_reason
            )
        _validate_exact_work(call_work)
        if attempt_timing is not None:
            _validate_exact_timing(attempt_timing)
        _require_exact_digest("direct provider error_hash", error_hash)
        self.direct_terminal_reason = direct_terminal_reason
        super().__init__(
            requested_failure_reason,
            retryable=retryable,
            call_work=call_work,
            attempt_timing=attempt_timing,
            error_hash=error_hash,
        )


@dataclass(frozen=True, slots=True)
class M5PostgresDirectDiscoveryExecution:
    result: DiscoveryResult
    execution_disposition: M5ExecutionEvidenceDisposition
    call_work: M5RuntimeWork
    attempt_timing: M5RuntimeTiming | None

    def __post_init__(self) -> None:
        _validated_discovery_result(self.result)
        _validate_success_metadata(
            self.execution_disposition, self.call_work, self.attempt_timing
        )


@dataclass(frozen=True, slots=True)
class M5PostgresDirectVerifierExecution:
    result: VerificationResult
    verification_execution: M5TypedDirectVerificationExecution | None
    execution_disposition: M5ExecutionEvidenceDisposition
    call_work: M5RuntimeWork
    attempt_timing: M5RuntimeTiming | None

    def __post_init__(self) -> None:
        _validated_verification_result(self.result)
        if self.verification_execution is not None:
            _validated_verification_execution(self.verification_execution)
        _validate_success_metadata(
            self.execution_disposition, self.call_work, self.attempt_timing
        )


def _validated_discovery_execution(
    execution: M5PostgresDirectDiscoveryExecution,
) -> M5PostgresDirectDiscoveryExecution:
    if type(execution) is not M5PostgresDirectDiscoveryExecution:
        raise ValidationError("direct discovery provider returned another type")
    result = _validated_discovery_result(execution.result)
    _validate_success_metadata(
        execution.execution_disposition,
        execution.call_work,
        execution.attempt_timing,
    )
    checked = M5PostgresDirectDiscoveryExecution(
        result=result,
        execution_disposition=execution.execution_disposition,
        call_work=_snapshot_exact_work(execution.call_work),
        attempt_timing=_snapshot_exact_timing(execution.attempt_timing),
    )
    if checked != execution:
        raise ValidationError("direct discovery execution reconstruction changed")
    return checked


def _validated_verifier_execution(
    execution: M5PostgresDirectVerifierExecution,
) -> M5PostgresDirectVerifierExecution:
    if type(execution) is not M5PostgresDirectVerifierExecution:
        raise ValidationError("direct verifier provider returned another type")
    result = _validated_verification_result(execution.result)
    verification = (
        None
        if execution.verification_execution is None
        else _validated_verification_execution(execution.verification_execution)
    )
    _validate_success_metadata(
        execution.execution_disposition,
        execution.call_work,
        execution.attempt_timing,
    )
    checked = M5PostgresDirectVerifierExecution(
        result=result,
        verification_execution=verification,
        execution_disposition=execution.execution_disposition,
        call_work=_snapshot_exact_work(execution.call_work),
        attempt_timing=_snapshot_exact_timing(execution.attempt_timing),
    )
    if checked != execution:
        raise ValidationError("direct verifier execution reconstruction changed")
    return checked


class M5PostgresDirectDiscoveryPort(Protocol):
    def discover_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectDiscoveryExecution: ...


class M5PostgresDirectVerifierPort(Protocol):
    def verify_direct(
        self,
        epoch_id: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
        event: M5TypedEventPlan,
    ) -> M5PostgresDirectVerifierExecution: ...


@dataclass(frozen=True, slots=True)
class _PendingDiscovery:
    acquisition: M5TypedDirectAcquisitionReceipt
    execution: M5PostgresDirectDiscoveryExecution


@dataclass(frozen=True, slots=True)
class _DirectReturnAuthority:
    return_kind: M5TypedDirectReturnKind
    epoch_id: int
    job_id: str
    attempt_id: str
    result_artifact_hash: str
    completion_digest: str
    envelope_digest: str
    requested_make_effective: bool | None
    normal_execution_evidence_digest: str
    late_execution_evidence_digest: str

    def __post_init__(self) -> None:
        if type(self.return_kind) is not M5TypedDirectReturnKind:
            raise ValidationError("direct return authority kind must be exact")
        _require_positive_int("direct return authority epoch", self.epoch_id)
        for name in (
            "job_id",
            "attempt_id",
            "result_artifact_hash",
            "completion_digest",
            "envelope_digest",
            "normal_execution_evidence_digest",
            "late_execution_evidence_digest",
        ):
            _require_exact_digest(
                f"direct return authority {name}", getattr(self, name)
            )
        if (
            self.requested_make_effective is not None
            and type(self.requested_make_effective) is not bool
        ):
            raise ValidationError("direct return effective flag must be exact")


def _direct_return_authority(
    envelope: M5TypedDirectLateReturnEnvelope,
    *,
    disposition: M5ExecutionEvidenceDisposition,
    attempt_work: M5RuntimeWork,
    attempt_timing: M5RuntimeTiming | None,
) -> _DirectReturnAuthority:
    if (
        type(envelope) is not M5TypedDirectLateReturnEnvelope
        or type(envelope.return_kind) is not M5TypedDirectReturnKind
        or type(envelope.epoch_id) is not int
        or type(envelope.job_id) is not str
        or type(envelope.attempt_id) is not str
        or type(envelope.result_artifact_hash) is not str
        or type(envelope.completion) is not JobCompletion
        or type(envelope.completion.completion_digest) is not str
        or type(envelope.envelope_digest) is not str
        or (
            envelope.requested_make_effective is not None
            and type(envelope.requested_make_effective) is not bool
        )
        or replace(envelope) != envelope
    ):
        raise ValidationError("direct return envelope authority must be exact")
    _validate_success_metadata(disposition, attempt_work, attempt_timing)
    return _DirectReturnAuthority(
        return_kind=envelope.return_kind,
        epoch_id=envelope.epoch_id,
        job_id=envelope.job_id,
        attempt_id=envelope.attempt_id,
        result_artifact_hash=envelope.result_artifact_hash,
        completion_digest=envelope.completion.completion_digest,
        envelope_digest=envelope.envelope_digest,
        requested_make_effective=envelope.requested_make_effective,
        normal_execution_evidence_digest=(
            _expected_direct_execution_evidence_digest(
                epoch_id=envelope.epoch_id,
                attempt_id=envelope.attempt_id,
                disposition=disposition,
                result_or_error_hash=envelope.result_artifact_hash,
                attempt_work=attempt_work,
                attempt_timing=attempt_timing,
            )
        ),
        late_execution_evidence_digest=(
            _expected_direct_execution_evidence_digest(
                epoch_id=envelope.epoch_id,
                attempt_id=envelope.attempt_id,
                disposition=disposition,
                result_or_error_hash=envelope.envelope_digest,
                attempt_work=attempt_work,
                attempt_timing=attempt_timing,
            )
        ),
    )


class PostgresM5TypedDirectPreSealPorts(PostgresM5GroupRequirementPreSealPorts):
    """Exact document/direct bridge that intentionally cannot seal."""

    def __init__(
        self,
        store: PostgresM5RuntimeStore,
        adapter: PostgresM5DirectM4Adapter,
        manifest: M5CandidatePolicyManifest,
        operational_config: M5RuntimeOperationalConfig,
        execution_policy: ApplicationExecutionPolicy,
        direct_discovery: M5PostgresDirectDiscoveryPort,
        direct_verifier: M5PostgresDirectVerifierPort,
        *,
        measurements: M5RuntimeMeasurementPort,
    ) -> None:
        super().__init__(store, manifest, operational_config)
        if type(adapter) is not PostgresM5DirectM4Adapter:
            raise ValidationError("adapter must be a PostgresM5DirectM4Adapter")
        if type(execution_policy) is not ApplicationExecutionPolicy:
            raise ValidationError("execution policy must be exact")
        if store._connection is not adapter._ports.connection:
            raise ValidationError("direct store and M4 adapter must share a connection")
        if adapter._ports.execution_mode is not M4ExecutionMode.MEASURED:
            raise ValidationError("typed-direct facade requires measured M4 mode")
        if not callable(getattr(direct_discovery, "discover_direct", None)):
            raise ValidationError("direct discovery provider is missing")
        if not callable(getattr(direct_verifier, "verify_direct", None)):
            raise ValidationError("direct verifier provider is missing")
        if not callable(getattr(measurements, "transition_call_timing", None)):
            raise ValidationError("direct transition measurement provider is missing")
        if not callable(getattr(measurements, "terminal_invocation", None)):
            raise ValidationError("direct terminal measurement provider is missing")
        self._adapter = adapter
        self._execution_policy = replace(execution_policy)
        self._direct_discovery = direct_discovery
        self._direct_verifier = direct_verifier
        self._measurements = measurements

    def build_application(
        self,
        *,
        discovery: M5RequirementDiscoveryPort,
        verifier: M5RequirementVerifierPort,
        measurements: M5RuntimeMeasurementPort | None,
        post_seal_audit: M5PostSealAuditPort | None,
    ) -> M5TypedApplication:
        """Require one recorder for structural, direct, and terminal points."""

        selected_measurements = (
            self._measurements if measurements is None else measurements
        )
        if selected_measurements is not self._measurements:
            raise ValidationError(
                "typed-direct application must share its measurement provider"
            )
        return super().build_application(
            discovery=discovery,
            verifier=verifier,
            measurements=selected_measurements,
            post_seal_audit=post_seal_audit,
        )

    def _validate_bound_event(self, event: M5TypedEventPlan) -> M5TypedEventPlan:
        checked = _validated_document_plan(event)
        if (
            checked.candidate_policy_id != self._manifest.candidate_policy_id
            or checked.candidate_policy_manifest_hash != self._manifest.manifest_hash
        ):
            raise ValidationError("typed event binds another candidate manifest")
        return checked

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        checked = self._validate_bound_event(event)
        assert checked.direct_plan is not None
        deactivated = checked.direct_plan.deactivated_chunk_version_ids
        with self._store._connection.transaction():
            rows = self._store._connection.execute(
                """
                SELECT published.observation_id, published.subject_id,
                       published.chunk_version_id
                FROM groundloop_published_observation_currency AS published
                WHERE published.subject_kind = 'requirement'
                  AND published.valid_from_epoch <= %s
                  AND (
                      published.valid_to_epoch IS NULL
                      OR %s < published.valid_to_epoch
                  )
                  AND published.chunk_version_id = ANY(%s)
                ORDER BY published.observation_id COLLATE "C"
                """,
                (
                    checked.expected_previous_published_epoch_id,
                    checked.expected_previous_published_epoch_id,
                    list(deactivated),
                ),
            ).fetchall()
        candidate_edges: list[M5WithdrawnCandidateEdge] = []
        observation_edges: list[M5WithdrawnObservationEdge] = []
        for row in rows:
            requirement_id = str(row[1])
            chunk_id = str(row[2])
            pair = SemanticPairKey(SubjectKind.REQUIREMENT, requirement_id, chunk_id)
            candidate_edges.append(
                M5WithdrawnCandidateEdge(
                    pair.semantic_pair_digest,
                    requirement_id,
                    chunk_id,
                    checked.candidate_policy_id,
                )
            )
            observation_edges.append(
                M5WithdrawnObservationEdge(
                    str(row[0]),
                    requirement_id,
                    chunk_id,
                    checked.candidate_policy_id,
                )
            )
        return plan_requirement_withdrawal(
            event_id=checked.structural_event_id,
            deactivated_chunk_version_ids=deactivated,
            candidate_edges=tuple(candidate_edges),
            observation_edges=tuple(observation_edges),
            cancelled_job_ids=(),
            active_requirement_version_ids=tuple(
                entry.requirement_version_id
                for entry in checked.requirement_registry_snapshot.entries
            ),
        )

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan:
        checked = self._validate_bound_event(event)
        assert checked.direct_plan is not None
        with self._adapter._ports.connection.transaction():
            withdrawal = self._adapter._ports.plan_exact_withdrawal(checked.direct_plan)
        roots = _m4_roots(checked, withdrawal, self._execution_policy)
        scope_members = (
            checked.direct_plan.registered_claim_ids
            if self._adapter._ports.execution_mode is M4ExecutionMode.AUDIT
            else ()
        )
        scopes = tuple(
            DiscoveryScope(
                root.job_id,
                checked.direct_plan.claim_registry_snapshot_id,
                scope_members,
            )
            for root in roots
            if root.kind is JobKind.IMPACT_DISCOVERY
        )
        return M5DirectOpenPlan(_structural_payload(checked), withdrawal, roots, scopes)

    def _impact_scope_members(self, event: M5TypedEventPlan) -> tuple[str, ...]:
        direct = event.direct_plan
        if direct is None:
            raise ValidationError("direct scope membership requires a direct plan")
        members = (
            direct.registered_claim_ids
            if self._adapter._ports.execution_mode is M4ExecutionMode.AUDIT
            else self._adapter.direct_claim_registry_members(
                direct.claim_registry_snapshot_id
            )
        )
        if type(members) is not tuple or any(type(item) is not str for item in members):
            raise ValidationError("direct scope members have another shape")
        return members

    def open_typed_event_atomically(
        self,
        event: M5TypedEventPlan,
        direct_payload: StructuralPayload | None,
        direct_withdrawal: StructuralWithdrawal | None,
        requirement_withdrawal: M5RequirementWithdrawalPlan,
        direct_roots: tuple[LogicalJobSpec, ...],
        direct_scopes: tuple[DiscoveryScope, ...],
        requirement_roots: tuple[M5RequirementRootDeclaration, ...],
        requirement_root_set_hash: str,
    ) -> OpenEventReceipt:
        checked = self._validate_bound_event(event)
        if direct_payload is None or direct_withdrawal is None:
            raise ValidationError("document direct open requires its M4 inputs")
        checked_payload = _validated_structural_payload(direct_payload)
        checked_direct_withdrawal = _validated_structural_withdrawal(direct_withdrawal)
        if type(direct_roots) is not tuple:
            raise ValidationError("direct roots must be an exact tuple")
        checked_direct_roots = tuple(
            _validated_logical_job(root) for root in direct_roots
        )
        if type(direct_scopes) is not tuple:
            raise ValidationError("direct scopes must be an exact tuple")
        checked_direct_scopes = tuple(
            _validated_discovery_scope(scope) for scope in direct_scopes
        )
        _require_exact_digest("requirement root-set hash", requirement_root_set_hash)
        expected_direct = self.plan_direct_open(checked)
        if (
            checked_payload != expected_direct.structural_payload
            or checked_direct_withdrawal != expected_direct.withdrawal
            or checked_direct_roots != expected_direct.root_jobs
            or checked_direct_scopes != expected_direct.discovery_scopes
        ):
            raise ValidationError("direct open inputs differ from deterministic plan")
        expected_withdrawal = self.plan_exact_requirement_withdrawal(checked)
        if (
            type(requirement_withdrawal) is not M5RequirementWithdrawalPlan
            or replace(requirement_withdrawal) != requirement_withdrawal
            or requirement_withdrawal != expected_withdrawal
        ):
            raise ValidationError("requirement withdrawal differs from exact plan")
        if type(requirement_roots) is not tuple:
            raise ValidationError("requirement roots must be an immutable tuple")
        supplied_roots = tuple(
            replace(item, scope=replace(item.scope), job=replace(item.job))
            if type(item) is M5RequirementRootDeclaration
            else self._invalid_requirement_root()
            for item in requirement_roots
        )
        expected_roots = _requirement_roots(
            checked, self._manifest, expected_withdrawal
        )
        if supplied_roots != expected_roots:
            raise ValidationError("requirement roots differ from deterministic plan")
        expected_hash = digests.requirement_root_set_digest(
            item.job.logical_job_id for item in expected_roots
        )
        if requirement_root_set_hash != expected_hash:
            raise ValidationError("requirement root-set hash disagrees with roots")
        fallback_keys = set(expected_withdrawal.fallback_keys)
        fallback_map: dict[str, bool] = {}
        for declaration in expected_roots:
            if declaration.job.job_kind is not M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL:
                continue
            requirement_id = declaration.scope.requirement_version_id
            assert requirement_id is not None
            fallback_map[declaration.job.logical_job_id] = (
                M5RequirementFallbackKey(
                    requirement_id, declaration.job.candidate_policy_id
                )
                in fallback_keys
            )

        assert checked.direct_plan is not None
        direct_plan = checked.direct_plan

        def stage(cursor: Cursor[Any]) -> tuple[OpenEventReceipt, M5RuntimeWork]:
            receipt = self._adapter.stage_direct_open(
                cursor,
                direct_plan,
                checked_payload,
                checked_direct_withdrawal,
                checked_direct_roots,
                checked_direct_scopes,
            )
            return receipt, self._adapter.measure_direct_open_work(
                cursor, receipt.epoch_id
            )

        try:
            receipt = self._store.open_typed_direct_event_atomically(
                checked,
                direct_stage=stage,
                requirement_roots=supplied_roots,
                requirement_root_set_hash=requirement_root_set_hash,
                recovery_operational_config=self._operational_config,
                recovery_root_fallback_required=fallback_map,
            )
        except Exception:
            self._adapter._after_outer_rollback()
            raise
        self._adapter._after_outer_commit()
        if type(receipt) is not OpenEventReceipt or replace(receipt) != receipt:
            raise ValidationError("typed direct open returned another receipt type")
        return receipt

    @staticmethod
    def _invalid_requirement_root() -> M5RequirementRootDeclaration:
        raise ValidationError("requirement root has another declaration type")

    def fail_typed_epoch_with_open_receipt_atomically(
        self,
        epoch_id: int,
        expected_revision: int,
        failure_reason: M5RunFailureReason,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> M5EventRunResult:
        self._validate_bound_epoch_call(
            epoch_id, expected_revision, open_receipt, call_work
        )
        return self._adapter.fail_typed_epoch_with_open_receipt_atomically(
            epoch_id,
            expected_revision,
            failure_reason,
            open_receipt,
            call_work,
        )

    def run_pending_direct(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        checked = self._validate_bound_event(event)
        self._validate_bound_epoch_call(
            epoch_id, expected_revision, open_receipt, M5RuntimeWork()
        )
        if self.current_revision(epoch_id) != expected_revision:
            raise EventConflictError("stale typed-direct application revision")
        roots = self.plan_direct_open(checked).root_jobs
        revision = expected_revision
        call_work = M5RuntimeWork()
        pending: list[_PendingDiscovery] = []
        existing_children: list[LogicalJobSpec] = []

        for root in roots:
            acquisition, revision = self._acquire(epoch_id, revision, root)
            terminal = self._terminal_acquisition_result(
                acquisition, call_work=call_work
            )
            if terminal is not None:
                if acquisition.lease.already_completed:
                    existing_children.extend(
                        self._adapter.direct_children_of(epoch_id, root.job_id)
                    )
                else:
                    return terminal
                continue
            if acquisition.lease.disposition is M5AcquisitionDisposition.LIVE_LEASE:
                return M5DirectExecutionReceipt(
                    revision,
                    call_work=call_work,
                    blocked_reason=M5RunFailureReason.WORK_IN_PROGRESS,
                )
            callback_acquisition = _validated_direct_acquisition(acquisition)
            callback_event = _validated_document_plan(checked)
            try:
                discovery_execution = self._direct_discovery.discover_direct(
                    epoch_id, callback_acquisition, callback_event
                )
            except M5PostgresDirectExternalWorkFailure as error:
                self._require_provider_inputs_unchanged(
                    callback_acquisition=callback_acquisition,
                    callback_event=callback_event,
                    trusted_acquisition=acquisition,
                    trusted_event=checked,
                )
                return self._settle_failure(
                    epoch_id,
                    revision,
                    checked,
                    open_receipt,
                    acquisition,
                    error,
                    call_work,
                )
            except M5ExternalWorkFailure as error:
                self._require_provider_inputs_unchanged(
                    callback_acquisition=callback_acquisition,
                    callback_event=callback_event,
                    trusted_acquisition=acquisition,
                    trusted_event=checked,
                )
                raise ValidationError(
                    "direct provider raised a generic external failure"
                ) from error
            self._require_provider_inputs_unchanged(
                callback_acquisition=callback_acquisition,
                callback_event=callback_event,
                trusted_acquisition=acquisition,
                trusted_event=checked,
            )
            discovery_execution = _validated_discovery_execution(discovery_execution)
            self._validate_discovery(epoch_id, root, discovery_execution.result)
            if root.kind is JobKind.FRONTIER_RETRIEVE and not (
                discovery_execution.result.fallback_satisfied
            ):
                failure = M5PostgresDirectExternalWorkFailure(
                    M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
                    retryable=True,
                    direct_terminal_reason=None,
                    call_work=discovery_execution.call_work,
                    attempt_timing=discovery_execution.attempt_timing,
                    error_hash=(discovery_execution.result.result_artifact_hash),
                )
                return self._settle_failure(
                    epoch_id,
                    revision,
                    checked,
                    open_receipt,
                    acquisition,
                    failure,
                    call_work,
                )
            call_work = _sum_work(call_work, discovery_execution.call_work)
            pending.append(_PendingDiscovery(acquisition, discovery_execution))
            observed_revision = self.current_revision(epoch_id)
            if observed_revision < revision:
                raise ValidationError(
                    "direct runtime revision moved backwards after provider return"
                )
            revision = observed_revision

        existing_pairs = {
            child.pair for child in existing_children if child.pair is not None
        }
        owner_by_pair: dict[PairKey, str] = {}
        for item in pending:
            for admitted in item.execution.result.admitted_pairs:
                if admitted.pair in existing_pairs:
                    continue
                owner = owner_by_pair.get(admitted.pair)
                root_id = item.acquisition.job.job_id
                if owner is None or root_id < owner:
                    owner_by_pair[admitted.pair] = root_id

        for item in pending:
            root = item.acquisition.job
            execution = item.execution
            owned = tuple(
                admitted
                for admitted in execution.result.admitted_pairs
                if owner_by_pair.get(admitted.pair) == root.job_id
            )
            active = (
                True
                if root.target_chunk_version_id is None
                else self._adapter.direct_chunk_is_active(root.target_chunk_version_id)
            )
            children = (
                tuple(
                    sorted(
                        (
                            _m4_job(
                                checked,
                                self._execution_policy,
                                kind=JobKind.VERIFY_PAIR,
                                parent_job_id=root.job_id,
                                claim_id=admitted.pair.claim_id,
                                chunk_id=admitted.pair.chunk_version_id,
                            )
                            for admitted in owned
                        ),
                        key=lambda child: child.job_id,
                    )
                )
                if active
                else ()
            )
            result = replace(execution.result, admitted_pairs=owned)
            closure = ChildClosure.build(
                parent_job_id=root.job_id,
                result_artifact_hash=result.result_artifact_hash,
                child_job_ids=tuple(child.job_id for child in children),
            )
            completion = JobCompletion.build(
                job_id=root.job_id,
                payload_hash=root.payload_hash,
                execution_spec_hash=root.execution_spec_hash,
                result_artifact_id=result.result_artifact_id,
                result_artifact_hash=result.result_artifact_hash,
                terminal_state=(
                    JobState.COMPLETED_ACTIVE if active else JobState.COMPLETED_INACTIVE
                ),
                child_closure=closure,
            )
            envelope = self._discovery_envelope(
                epoch_id,
                checked,
                item.acquisition,
                result,
                completion,
            )
            authority = _direct_return_authority(
                envelope,
                disposition=execution.execution_disposition,
                attempt_work=execution.call_work,
                attempt_timing=execution.attempt_timing,
            )
            settled = self._adapter.settle_direct_expansion_atomically(
                epoch_id,
                revision,
                item.acquisition.lease,
                envelope,
                children,
                execution.execution_disposition,
                _snapshot_exact_work(execution.call_work),
                _snapshot_exact_timing(execution.attempt_timing),
            )
            revision, early = self._successful_return_result(
                settled,
                expected_revision=revision,
                expected_kind=M5TypedDirectReturnKind.DISCOVERY,
                expected_acquisition=item.acquisition,
                expected_authority=authority,
                call_work=call_work,
            )
            if early is not None:
                return early

        all_children = tuple(
            child
            for root in roots
            for child in self._adapter.direct_children_of(epoch_id, root.job_id)
        )
        self._validate_global_children(all_children)
        for child in sorted(all_children, key=lambda item: item.job_id):
            acquisition, revision = self._acquire(epoch_id, revision, child)
            terminal = self._terminal_acquisition_result(
                acquisition, call_work=call_work
            )
            if terminal is not None:
                if acquisition.lease.already_completed:
                    continue
                return terminal
            if acquisition.lease.disposition is M5AcquisitionDisposition.LIVE_LEASE:
                return M5DirectExecutionReceipt(
                    revision,
                    call_work=call_work,
                    blocked_reason=M5RunFailureReason.WORK_IN_PROGRESS,
                )
            callback_acquisition = _validated_direct_acquisition(acquisition)
            callback_event = _validated_document_plan(checked)
            try:
                verifier_result = self._direct_verifier.verify_direct(
                    epoch_id, callback_acquisition, callback_event
                )
            except M5PostgresDirectExternalWorkFailure as error:
                self._require_provider_inputs_unchanged(
                    callback_acquisition=callback_acquisition,
                    callback_event=callback_event,
                    trusted_acquisition=acquisition,
                    trusted_event=checked,
                )
                return self._settle_failure(
                    epoch_id,
                    revision,
                    checked,
                    open_receipt,
                    acquisition,
                    error,
                    call_work,
                )
            except M5ExternalWorkFailure as error:
                self._require_provider_inputs_unchanged(
                    callback_acquisition=callback_acquisition,
                    callback_event=callback_event,
                    trusted_acquisition=acquisition,
                    trusted_event=checked,
                )
                raise ValidationError(
                    "direct provider raised a generic external failure"
                ) from error
            self._require_provider_inputs_unchanged(
                callback_acquisition=callback_acquisition,
                callback_event=callback_event,
                trusted_acquisition=acquisition,
                trusted_event=checked,
            )
            verifier_result = _validated_verifier_execution(verifier_result)
            self._validate_verification(child, verifier_result.result)
            call_work = _sum_work(call_work, verifier_result.call_work)
            assert child.pair is not None
            active = self._adapter.direct_chunk_is_active(child.pair.chunk_version_id)
            completion = JobCompletion.build(
                job_id=child.job_id,
                payload_hash=child.payload_hash,
                execution_spec_hash=child.execution_spec_hash,
                result_artifact_id=verifier_result.result.result_artifact_id,
                result_artifact_hash=verifier_result.result.result_artifact_hash,
                terminal_state=(
                    JobState.COMPLETED_ACTIVE if active else JobState.COMPLETED_INACTIVE
                ),
            )
            attempt = self._attempt(acquisition)
            verifier_execution = verifier_result.verification_execution
            envelope = M5TypedDirectLateReturnEnvelope.build_verifier(
                epoch_id=epoch_id,
                job=child,
                attempt=attempt,
                completion=completion,
                verification_execution=verifier_execution,
                observation=verifier_result.result.observation,
                observation_produced_epoch=epoch_id,
                observation_raw_output_hash=(
                    completion.result_artifact_hash
                    if verifier_execution is None
                    else verifier_execution.raw_output_hash
                ),
                observation_eligible_for_currency=True,
                requested_make_effective=active,
            )
            authority = _direct_return_authority(
                envelope,
                disposition=verifier_result.execution_disposition,
                attempt_work=verifier_result.call_work,
                attempt_timing=verifier_result.attempt_timing,
            )
            settled = self._adapter.settle_direct_verifier_atomically(
                epoch_id,
                revision,
                acquisition.lease,
                envelope,
                verifier_result.execution_disposition,
                _snapshot_exact_work(verifier_result.call_work),
                _snapshot_exact_timing(verifier_result.attempt_timing),
            )
            revision, early = self._successful_return_result(
                settled,
                expected_revision=revision,
                expected_kind=M5TypedDirectReturnKind.VERIFIER,
                expected_acquisition=acquisition,
                expected_authority=authority,
                call_work=call_work,
            )
            if early is not None:
                return early
        return M5DirectExecutionReceipt(revision, call_work=call_work)

    def _validate_bound_epoch_call(
        self,
        epoch_id: int,
        expected_revision: int,
        open_receipt: OpenEventReceipt,
        call_work: M5RuntimeWork,
    ) -> None:
        _require_positive_int("epoch_id", epoch_id)
        _require_positive_int("expected_revision", expected_revision)
        _validated_open_receipt(open_receipt, epoch_id=epoch_id)
        _validate_exact_work(call_work)

    @staticmethod
    def _require_provider_inputs_unchanged(
        *,
        callback_acquisition: M5TypedDirectAcquisitionReceipt,
        callback_event: M5TypedEventPlan,
        trusted_acquisition: M5TypedDirectAcquisitionReceipt,
        trusted_event: M5TypedEventPlan,
    ) -> None:
        if (
            _validated_direct_acquisition(callback_acquisition) != trusted_acquisition
            or _validated_document_plan(callback_event) != trusted_event
        ):
            raise ValidationError("direct provider mutated its callback inputs")

    def _acquire(
        self, epoch_id: int, revision: int, job: LogicalJobSpec
    ) -> tuple[M5TypedDirectAcquisitionReceipt, int]:
        receipt = _validated_direct_acquisition(
            self._adapter.acquire_direct_job_atomically(epoch_id, revision, job)
        )
        if receipt.epoch_id != epoch_id or receipt.job != job:
            raise ValidationError("typed-direct acquisition changed its request")
        resulting_revision = receipt.lease.resulting_revision
        if resulting_revision < revision:
            raise ValidationError("direct acquisition revision moved backwards")
        if receipt.lease.disposition in {
            M5AcquisitionDisposition.DISPATCH_NEW,
            M5AcquisitionDisposition.DISPATCH_TAKEOVER,
        }:
            if resulting_revision != revision + 1:
                raise ValidationError("direct dispatch advanced an unexpected revision")
            source_id = receipt.lease.dispatch_record_digest
            if type(source_id) is not str:
                raise ValidationError("direct acquisition lacks its dispatch digest")
            resulting_revision = self._append_transition_anchor(
                M5TransitionTimingAnchor.build(
                    epoch_id=epoch_id,
                    contribution_kind=(
                        M5RuntimeWorkContributionKind.DIRECT_ACQUISITION
                    ),
                    source_id=source_id,
                    anchor_revision=resulting_revision,
                    terminal_transition=False,
                )
            )
        return receipt, resulting_revision

    def _append_transition_anchor(self, anchor: M5TransitionTimingAnchor) -> int:
        return _append_transition_anchor_checked(
            anchor=anchor,
            measurements=self._measurements,
            append_transition_call_timing=self.append_transition_call_timing,
        )

    @staticmethod
    def _terminal_acquisition_result(
        acquisition: M5TypedDirectAcquisitionReceipt,
        *,
        call_work: M5RuntimeWork,
    ) -> M5DirectExecutionReceipt | None:
        lease = acquisition.lease
        if lease.disposition is not M5AcquisitionDisposition.TERMINAL:
            if lease.disposition not in {
                M5AcquisitionDisposition.DISPATCH_NEW,
                M5AcquisitionDisposition.DISPATCH_TAKEOVER,
                M5AcquisitionDisposition.LIVE_LEASE,
            }:
                raise ValidationError("direct acquisition has an unsupported branch")
            return None
        projection = lease.terminal_projection
        assert projection is not None
        failing = (
            projection.terminal_state is JobState.TERMINAL_FAILED
            or projection.terminal_reason == "epoch_failed"
        )
        if failing:
            return M5DirectExecutionReceipt(
                lease.resulting_revision,
                call_work=call_work,
                selected_terminal_acquisition_receipt=acquisition,
            )
        if projection.terminal_state not in {
            JobState.COMPLETED_ACTIVE,
            JobState.COMPLETED_INACTIVE,
        }:
            raise EventConflictError("direct terminal acquisition is not total")
        return M5DirectExecutionReceipt(
            lease.resulting_revision,
            call_work=call_work,
        )

    def _settle_failure(
        self,
        epoch_id: int,
        revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
        acquisition: M5TypedDirectAcquisitionReceipt,
        error: M5PostgresDirectExternalWorkFailure,
        prior_work: M5RuntimeWork,
    ) -> M5DirectExecutionReceipt:
        error = self._validated_external_failure(error)
        call_work = _sum_work(prior_work, error.call_work)
        attempt_id = acquisition.lease.attempt_id
        if attempt_id is None:
            raise ValidationError("direct provider failure lacks its acquired attempt")
        if error.retryable:
            expected_evidence_digest = _expected_direct_execution_evidence_digest(
                epoch_id=epoch_id,
                attempt_id=attempt_id,
                disposition=M5ExecutionEvidenceDisposition.RETRYABLE_FAILURE,
                result_or_error_hash=error.error_hash,
                attempt_work=error.call_work,
                attempt_timing=error.attempt_timing,
            )
            outcome = self._adapter.mark_direct_retryable_failure_atomically(
                epoch_id,
                revision,
                acquisition.lease,
                error.error_hash,
                error.call_work,
                error.attempt_timing,
            )
            if (
                type(outcome) is not _DirectRetryableFailureOuterReceipt
                or replace(outcome) != outcome
            ):
                raise ValidationError("retryable direct failure outcome drifted")
            receipt = _validated_direct_cursor_receipt(outcome.receipt)
            resulting_revision = outcome.resulting_revision
            anchor = outcome.transition_anchor
            if (
                receipt.epoch_id != epoch_id
                or receipt.job_id != acquisition.job.job_id
                or receipt.attempt_id != acquisition.lease.attempt_id
                or receipt.execution_evidence_digest != expected_evidence_digest
                or receipt.direct_transition_source_id is not None
                or receipt.direct_transition_source_identity_hash is not None
                or receipt.direct_transition_contribution_key_digest is not None
                or receipt.observation_completion is not None
            ):
                raise ValidationError("retryable direct failure receipt drifted")
            _require_positive_int(
                "retryable direct failure resulting revision", resulting_revision
            )
            if resulting_revision < revision:
                raise ValidationError(
                    "retryable direct failure revision moved backwards"
                )
            if not outcome.exact_replay and resulting_revision != revision + 1:
                raise ValidationError("retryable direct failure skipped its revision")
            if anchor is not None:
                anchor = _validated_transition_anchor(anchor)
                if (
                    anchor.epoch_id != epoch_id
                    or anchor.contribution_kind
                    is not M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION
                    or anchor.source_id != receipt.attempt_id
                    or anchor.contribution_key_digest
                    != receipt.attempt_execution_contribution_key_digest
                    or anchor.anchor_revision != resulting_revision
                ):
                    raise ValidationError(
                        "retryable direct failure anchor changed its receipt"
                    )
                resulting_revision = self._append_transition_anchor(anchor)
            return M5DirectExecutionReceipt(
                resulting_revision,
                call_work=call_work,
                blocked_reason=error.reason,
            )
        direct_terminal_reason = error.direct_terminal_reason
        if direct_terminal_reason is None:
            raise ValidationError("terminal direct failure lacks its M4 reason")
        expected_evidence_digest = _expected_direct_execution_evidence_digest(
            epoch_id=epoch_id,
            attempt_id=attempt_id,
            disposition=M5ExecutionEvidenceDisposition.TERMINAL_FAILURE,
            result_or_error_hash=error.error_hash,
            attempt_work=error.call_work,
            attempt_timing=error.attempt_timing,
        )
        combined = (
            self._adapter.fail_typed_epoch_after_direct_terminal_failure_atomically(
                epoch_id,
                revision,
                acquisition.job,
                acquisition.lease,
                direct_terminal_reason,
                error.error_hash,
                error.call_work,
                error.attempt_timing,
                error.reason,
                open_receipt,
                call_work,
            )
        )
        if type(combined) is M5TypedDirectAcquisitionReceipt:
            combined = self._validate_terminal_failure_loser(
                combined,
                epoch_id=epoch_id,
                expected_revision=revision,
                acquisition=acquisition,
            )
            return M5DirectExecutionReceipt(
                combined.lease.resulting_revision,
                call_work=call_work,
                selected_terminal_acquisition_receipt=combined,
            )
        if type(combined) is not M5CheckedDirectTerminalFailureReceipt:
            raise ValidationError("combined direct failure returned another type")
        self._validate_checked_terminal_failure(
            combined,
            epoch_id=epoch_id,
            expected_revision=revision,
            expected_evidence_digest=expected_evidence_digest,
            event=event,
            open_receipt=open_receipt,
            acquisition=acquisition,
            error=error,
        )
        return M5DirectExecutionReceipt(
            combined.resulting_revision,
            call_work=call_work,
            selected_checked_combined_failure_receipt=combined,
        )

    @staticmethod
    def _validate_terminal_failure_loser(
        combined: M5TypedDirectAcquisitionReceipt,
        *,
        epoch_id: int,
        expected_revision: int,
        acquisition: M5TypedDirectAcquisitionReceipt,
    ) -> M5TypedDirectAcquisitionReceipt:
        combined = _validated_direct_acquisition(combined)
        acquisition = _validated_direct_acquisition(acquisition)
        if combined.lease.disposition is not M5AcquisitionDisposition.TERMINAL:
            raise ValidationError("direct failure loser is not terminal")
        terminal_lease = combined.lease
        projection = terminal_lease.terminal_projection
        if (
            projection is None
            or projection.terminal_state is not JobState.CANCELLED
            or projection.terminal_reason != "epoch_failed"
            or projection.m4_completion_digest is not None
            or projection.completed_revision != terminal_lease.resulting_revision
            or terminal_lease.already_completed is not False
        ):
            raise ValidationError(
                "direct failure loser lacks the epoch-failed cancellation"
            )
        if combined.epoch_id != epoch_id:
            raise ValidationError("direct failure loser changed its epoch")
        if terminal_lease.resulting_revision != expected_revision + 1:
            raise ValidationError("direct failure loser changed its terminal revision")
        if combined.job != acquisition.job:
            raise ValidationError("direct failure loser changed its target job")
        if combined.attempt != acquisition.attempt:
            raise ValidationError("direct failure loser changed its acquired attempt")
        original_lease = acquisition.lease
        if (
            terminal_lease.job_id,
            terminal_lease.attempt_id,
            terminal_lease.lease_token_hash,
            terminal_lease.lease_expires_at,
            terminal_lease.dispatch_record_digest,
        ) != (
            original_lease.job_id,
            original_lease.attempt_id,
            original_lease.lease_token_hash,
            original_lease.lease_expires_at,
            original_lease.dispatch_record_digest,
        ):
            raise ValidationError("direct failure loser changed its lease provenance")
        return combined

    @staticmethod
    def _validate_checked_terminal_failure(
        combined: M5CheckedDirectTerminalFailureReceipt,
        *,
        epoch_id: int,
        expected_revision: int,
        expected_evidence_digest: str,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
        acquisition: M5TypedDirectAcquisitionReceipt,
        error: M5PostgresDirectExternalWorkFailure,
    ) -> None:
        replace(combined)
        replace(acquisition)
        direct_failure = combined.direct_failure
        if (
            direct_failure.epoch_id,
            direct_failure.job_id,
            direct_failure.attempt_id,
        ) != (
            epoch_id,
            acquisition.job.job_id,
            acquisition.lease.attempt_id,
        ):
            raise ValidationError("checked direct failure changed its acquired attempt")
        if direct_failure.execution_evidence_digest != expected_evidence_digest:
            raise ValidationError(
                "checked direct failure changed its execution evidence"
            )
        if combined.resulting_revision != expected_revision + 1:
            raise ValidationError(
                "checked direct failure changed its terminal revision"
            )
        if combined.requested_failure_reason is not error.reason:
            raise ValidationError("checked direct failure changed its requested reason")
        terminal_result = combined.terminal_result
        if (
            terminal_result.event_id,
            terminal_result.payload_hash,
            terminal_result.epoch_id,
        ) != (
            event.structural_event_id,
            event.payload_hash,
            epoch_id,
        ):
            raise ValidationError("checked direct failure changed its event provenance")
        if (
            terminal_result.state is M5RunState.FAILED
            and terminal_result.open_receipt is not open_receipt
        ):
            raise ValidationError(
                "checked direct failure changed its held open receipt"
            )

    @staticmethod
    def _validated_external_failure(
        error: M5PostgresDirectExternalWorkFailure,
    ) -> M5PostgresDirectExternalWorkFailure:
        if type(error) is not M5PostgresDirectExternalWorkFailure:
            raise ValidationError("direct provider raised another failure type")
        checked = M5PostgresDirectExternalWorkFailure(
            error.reason,
            retryable=error.retryable,
            direct_terminal_reason=error.direct_terminal_reason,
            call_work=_snapshot_exact_work(error.call_work),
            attempt_timing=_snapshot_exact_timing(error.attempt_timing),
            error_hash=error.error_hash,
        )
        return checked

    @staticmethod
    def _validate_discovery(
        epoch_id: int, root: LogicalJobSpec, result: DiscoveryResult
    ) -> None:
        if result.root_job_id != root.job_id:
            raise ValidationError("direct discovery belongs to another root")
        for admitted in result.admitted_pairs:
            if (
                admitted.epoch_id != epoch_id
                or admitted.candidate_policy_id != root.candidate_policy_id
            ):
                raise ValidationError("direct admission escaped epoch or policy")
            if root.kind is JobKind.IMPACT_DISCOVERY and (
                admitted.pair.chunk_version_id != root.target_chunk_version_id
            ):
                raise ValidationError("direct impact admission escaped its chunk")
            if root.kind is JobKind.FRONTIER_RETRIEVE and (
                admitted.pair.claim_id != root.target_claim_id
            ):
                raise ValidationError("direct frontier admission escaped its claim")

    @staticmethod
    def _validate_global_children(children: tuple[LogicalJobSpec, ...]) -> None:
        ids = tuple(child.job_id for child in children)
        pairs = tuple(child.pair for child in children)
        if len(set(ids)) != len(ids) or len(set(pairs)) != len(pairs):
            raise ValidationError("direct verifier child set is not globally unique")

    @staticmethod
    def _validate_verification(
        child: LogicalJobSpec, result: VerificationResult
    ) -> None:
        pair = child.pair
        if pair is None:
            raise ValidationError("direct verifier job lacks its pair")
        observation: SemanticObservation = result.observation
        if (
            observation.subject_kind is not SubjectKind.CLAIM
            or observation.subject_id != pair.claim_id
            or observation.chunk_version_id != pair.chunk_version_id
        ):
            raise ValidationError("direct verifier observation targets another pair")

    @staticmethod
    def _attempt(acquisition: M5TypedDirectAcquisitionReceipt) -> JobAttempt:
        attempt = acquisition.attempt
        if attempt is None:
            raise ValidationError("executable direct acquisition lacks its attempt")
        return attempt

    def _discovery_envelope(
        self,
        epoch_id: int,
        event: M5TypedEventPlan,
        acquisition: M5TypedDirectAcquisitionReceipt,
        result: DiscoveryResult,
        completion: JobCompletion,
    ) -> M5TypedDirectLateReturnEnvelope:
        direct = event.direct_plan
        assert direct is not None
        job = acquisition.job
        if job.kind is JobKind.IMPACT_DISCOVERY:
            members = self._impact_scope_members(event)
            kind = M5TypedDirectScopeKind.ALL_REGISTERED_CLAIMS
            explicit = None
        else:
            target = job.target_claim_id
            if target is None:
                raise ValidationError("frontier direct job lacks its target claim")
            members = (target,)
            kind = M5TypedDirectScopeKind.EXPLICIT_CLAIMS
            explicit = members
        return M5TypedDirectLateReturnEnvelope.build_discovery(
            epoch_id=epoch_id,
            job=job,
            attempt=self._attempt(acquisition),
            completion=completion,
            discovery=result,
            scope=DiscoveryScope(
                job.job_id, direct.claim_registry_snapshot_id, members
            ),
            persisted_scope_kind=kind,
            explicit_claim_ids=explicit,
            closed_revision=None,
        )

    def _successful_return_result(
        self,
        receipt: M5DirectAttemptReturnReceipt,
        *,
        expected_revision: int,
        expected_kind: M5TypedDirectReturnKind,
        expected_acquisition: M5TypedDirectAcquisitionReceipt,
        expected_authority: _DirectReturnAuthority,
        call_work: M5RuntimeWork,
    ) -> tuple[int, M5DirectExecutionReceipt | None]:
        _require_positive_int("direct settlement expected revision", expected_revision)
        if type(expected_authority) is not _DirectReturnAuthority:
            raise ValidationError("direct settlement authority must be exact")
        receipt = _validated_direct_attempt_return(receipt)
        if (
            receipt.return_kind is not expected_kind
            or expected_authority.return_kind is not expected_kind
        ):
            raise ValidationError("direct settlement changed its return kind")
        branch = receipt.normal if receipt.normal is not None else receipt.late
        if branch is None:
            raise ValidationError("direct settlement lacks one selected branch")
        if (
            branch.epoch_id != expected_acquisition.epoch_id
            or branch.job_id != expected_acquisition.job.job_id
            or branch.attempt_id != expected_acquisition.lease.attempt_id
            or branch.epoch_id != expected_authority.epoch_id
            or branch.job_id != expected_authority.job_id
            or branch.attempt_id != expected_authority.attempt_id
        ):
            raise ValidationError("direct settlement changed its acquired attempt")
        if receipt.normal is not None:
            normal = receipt.normal
            if (
                normal.return_artifact_digest != expected_authority.result_artifact_hash
                or normal.direct_transition_source_id
                != expected_authority.completion_digest
                or normal.execution_evidence_digest
                != expected_authority.normal_execution_evidence_digest
            ):
                raise ValidationError("normal direct settlement changed its provenance")
            if expected_kind is M5TypedDirectReturnKind.VERIFIER:
                observation = normal.observation_completion
                if (
                    observation is None
                    or observation.made_effective
                    is not expected_authority.requested_make_effective
                ):
                    raise ValidationError(
                        "normal verifier settlement changed its effective request"
                    )
        else:
            assert receipt.late is not None
            if (
                receipt.late.envelope_digest != expected_authority.envelope_digest
                or receipt.late.execution_evidence_digest
                != expected_authority.late_execution_evidence_digest
            ):
                raise ValidationError("late direct settlement changed its provenance")
        if branch.resulting_revision < expected_revision:
            raise ValidationError("direct settlement revision moved backwards")
        if (
            receipt.normal is not None
            and not receipt.normal.exact_replay
            and branch.resulting_revision != expected_revision + 1
        ):
            raise ValidationError("normal direct settlement skipped its revision")
        resulting_revision = branch.resulting_revision
        if branch.transition_anchor is not None:
            resulting_revision = self._append_transition_anchor(
                branch.transition_anchor
            )
        if branch.current_terminal_logical_result_hash is not None:
            return (
                resulting_revision,
                M5DirectExecutionReceipt(
                    resulting_revision,
                    call_work=call_work,
                    selected_successful_outer_receipt=receipt,
                    selected_successful_outer_return_kind=expected_kind,
                    selected_successful_outer_job_id=expected_acquisition.job.job_id,
                ),
            )
        if receipt.late is not None and (
            receipt.late.disposition
            is M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
        ):
            return (
                resulting_revision,
                M5DirectExecutionReceipt(
                    resulting_revision,
                    call_work=call_work,
                    blocked_reason=M5RunFailureReason.WORK_IN_PROGRESS,
                ),
            )
        return resulting_revision, None


__all__ = [
    "M5PostgresDirectDiscoveryExecution",
    "M5PostgresDirectDiscoveryPort",
    "M5PostgresDirectExternalWorkFailure",
    "M5PostgresDirectVerifierExecution",
    "M5PostgresDirectVerifierPort",
    "PostgresM5TypedDirectPreSealPorts",
]
