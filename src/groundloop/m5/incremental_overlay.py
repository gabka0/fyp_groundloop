"""Sparse in-memory composition of direct and bounded M5 group maintenance.

The repository event is already committed when this overlay is invoked.  A
prepared overlay patch is therefore a transaction over *derived* state: it
composes one direct-engine patch with one coalesced matching-index patch per
affected group.  Matching patches publish first and the direct patch publishes
last.  A failure before that final checkpoint restores every derived value and
matching generation exactly.

This module is intentionally independent of :mod:`groundloop.m5.reference`.
Full recomputation remains a test oracle, never an update path.
"""

from __future__ import annotations

import hashlib
import struct
from collections import Counter
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass, replace
from enum import Enum
from typing import Generic, TypeVar

from groundloop.domain import (
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    DecisionPolicy,
    SemanticObservation,
    StatusDelta,
    VerificationLabel,
)
from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import (
    DeleteDocumentVersionEvent,
    Event,
    PolicyChangeEvent,
    ReplaceDocumentVersionEvent,
)
from groundloop.incremental import (
    IncrementalMaintenanceEngine,
    IncrementalStatePatch,
    MaintenanceStats,
)
from groundloop.m5.claim_certificates import (
    WorkingClaimCertificateBinding,
    transition_claim_certificate,
    transition_claim_certificate_for_selected_support,
)
from groundloop.m5.digests import normalized_text_hash_v1
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    CombinedAnswerState,
    CombinedClaimState,
    EvidenceGroupVersion,
    GroupMatchingCertificateArtifact,
    GroupState,
    RequirementState,
    SnapshotPoint,
)
from groundloop.m5.events import (
    M5Event,
    ObserveRequirementEvent,
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
    legacy_event_payload_digest,
    m5_event_payload_digest,
)
from groundloop.m5.matching import (
    CertificateRebuildRequired,
    HallMaskState,
    MaintainedCertificateIndex,
    MaintainedIndexRollbackToken,
    MatchingWorkCounters,
    ObservationMembershipDelta,
    PersistentStringSet,
    PreparedObservationIndexPatch,
    WorkingGroupCertificateBinding,
    apply_hash_mask_transitions,
    build_or_rebuild_certificate,
    carry_forward_certificate_epoch,
    close_incomplete_certificate,
    initialize_hall_mask_state,
    policy_range_probe_work,
    rebind_certificate_policy,
    repair_selected_observations,
    requirement_observation_work,
    touched_state_work,
)
from groundloop.m5.repository import M5Repository
from groundloop.policy import decide

CommittedOverlayEvent = Event | M5Event
OverlayFailureInjector = Callable[[str], None]
CANONICAL_REQUIREMENT_TASK = "verify_requirement_v1"
_T = TypeVar("_T")
_K = TypeVar("_K", bound=Hashable)


class _OrderedKeys(Generic[_K]):
    """First-touch ordered unique keys for measured event propagation."""

    __slots__ = ("_values",)

    def __init__(self, values: Iterable[_K] = ()) -> None:
        self._values: dict[_K, None] = {}
        self.update(values)

    def add(self, key: _K) -> None:
        self._values.setdefault(key, None)

    def update(self, values: Iterable[_K]) -> None:
        for value in values:
            self.add(value)

    def __iter__(self) -> Iterator[_K]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __contains__(self, key: object) -> bool:
        return key in self._values


@dataclass(frozen=True, slots=True)
class M5OverlayWork:
    """Signed M5-T2 work, excluding database/WAL and neural execution.

    ``matching`` carries the primitive U/P/probe/Z/R/Y and G/C/A terms.
    The explicit state/certificate-only terms make constant-status publication
    work inspectable instead of misclassifying it as a public status delta.
    """

    matching: MatchingWorkCounters = field(default_factory=MatchingWorkCounters)
    requirement_state_only_changes: int = 0
    group_state_only_changes: int = 0
    claim_state_only_changes: int = 0
    group_certificate_only_changes: int = 0
    claim_certificate_only_changes: int = 0
    public_status_deltas: int = 0

    def __add__(self, other: M5OverlayWork) -> M5OverlayWork:
        if not isinstance(other, M5OverlayWork):
            return NotImplemented
        return M5OverlayWork(
            matching=self.matching + other.matching,
            requirement_state_only_changes=(
                self.requirement_state_only_changes
                + other.requirement_state_only_changes
            ),
            group_state_only_changes=(
                self.group_state_only_changes + other.group_state_only_changes
            ),
            claim_state_only_changes=(
                self.claim_state_only_changes + other.claim_state_only_changes
            ),
            group_certificate_only_changes=(
                self.group_certificate_only_changes
                + other.group_certificate_only_changes
            ),
            claim_certificate_only_changes=(
                self.claim_certificate_only_changes
                + other.claim_certificate_only_changes
            ),
            public_status_deltas=(
                self.public_status_deltas + other.public_status_deltas
            ),
        )

    def __sub__(self, other: M5OverlayWork) -> M5OverlayWork:
        if not isinstance(other, M5OverlayWork):
            return NotImplemented
        return self + (-other)

    def __neg__(self) -> M5OverlayWork:
        return M5OverlayWork(
            matching=-self.matching,
            requirement_state_only_changes=-self.requirement_state_only_changes,
            group_state_only_changes=-self.group_state_only_changes,
            claim_state_only_changes=-self.claim_state_only_changes,
            group_certificate_only_changes=-self.group_certificate_only_changes,
            claim_certificate_only_changes=-self.claim_certificate_only_changes,
            public_status_deltas=-self.public_status_deltas,
        )

    def assert_nonnegative(self) -> None:
        self.matching.assert_nonnegative()
        values = (
            self.requirement_state_only_changes,
            self.group_state_only_changes,
            self.claim_state_only_changes,
            self.group_certificate_only_changes,
            self.claim_certificate_only_changes,
            self.public_status_deltas,
        )
        if any(value < 0 for value in values):
            raise ValidationError("overlay work counters must be nonnegative")

    @property
    def u(self) -> int:
        return self.matching.requirement_observation_changes_processed

    @property
    def p(self) -> int:
        return self.matching.ordered_policy_range_probes

    @property
    def probes(self) -> int:
        return self.matching.ordered_policy_range_probes

    @property
    def policy_candidates(self) -> int:
        return self.matching.policy_candidate_observations

    @property
    def z(self) -> int:
        return self.matching.hash_mask_transitions

    @property
    def hall_entries_examined(self) -> int:
        return self.matching.hall_subset_entries_examined

    @property
    def r(self) -> int:
        return self.matching.certificate_repairs

    @property
    def y(self) -> int:
        return self.matching.certificate_reconstructions

    @property
    def groups(self) -> int:
        return self.matching.groups_touched

    @property
    def claims(self) -> int:
        return self.matching.claims_touched

    @property
    def answers(self) -> int:
        return self.matching.answers_touched

    @property
    def output_bytes(self) -> int:
        return self.matching.output_bytes


@dataclass(frozen=True, slots=True)
class M5OverlayApplyResult:
    event_id: str
    point: SnapshotPoint
    deltas: tuple[StatusDelta, ...]
    changed_requirement_ids: tuple[str, ...]
    changed_group_ids: tuple[str, ...]
    changed_claim_ids: tuple[str, ...]
    changed_answer_ids: tuple[str, ...]
    state_only_requirement_ids: tuple[str, ...]
    state_only_group_ids: tuple[str, ...]
    state_only_claim_ids: tuple[str, ...]
    certificate_only_group_ids: tuple[str, ...]
    certificate_only_claim_ids: tuple[str, ...]
    published_group_bindings: tuple[WorkingGroupCertificateBinding, ...]
    published_claim_bindings: tuple[WorkingClaimCertificateBinding, ...]
    logical_output_digest: str
    work: M5OverlayWork
    direct_stats: MaintenanceStats
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class _PointChange(Generic[_K, _T]):
    key: _K
    before: _T | None
    after: _T | None


@dataclass(frozen=True, slots=True)
class _RequirementLocal:
    witness_hashes: PersistentStringSet = field(default_factory=PersistentStringSet)
    observation_ids: PersistentStringSet = field(default_factory=PersistentStringSet)


@dataclass(frozen=True, slots=True)
class _HistoryNode(Generic[_T]):
    """Persistent append/replace-tail log node with O(1) event transitions."""

    value: _T
    previous: _HistoryNode[_T] | None
    length: int


def _history_append(
    root: _HistoryNode[_T] | None,
    value: _T,
) -> _HistoryNode[_T]:
    return _HistoryNode(value, root, 1 if root is None else root.length + 1)


def _history_materialize(root: _HistoryNode[_T] | None) -> tuple[_T, ...]:
    """Audit/read adapter; never called by the measured update path."""

    reverse_values: list[_T] = []
    current = root
    while current is not None:
        reverse_values.append(current.value)
        current = current.previous
    reverse_values.reverse()
    return tuple(reverse_values)


@dataclass(frozen=True, slots=True)
class _ObservationInfo:
    observation: SemanticObservation
    group_version_id: str
    requirement_version_id: str
    requirement_ordinal: int
    text_hash: str


_ScoreKey = tuple[float, str]


@dataclass(frozen=True, slots=True)
class _ScoreNode:
    key: _ScoreKey
    left: _ScoreNode | None = None
    right: _ScoreNode | None = None
    height: int = 1


def _score_height(node: _ScoreNode | None) -> int:
    return 0 if node is None else node.height


def _score_node(
    key: _ScoreKey,
    left: _ScoreNode | None,
    right: _ScoreNode | None,
) -> _ScoreNode:
    return _ScoreNode(
        key, left, right, 1 + max(_score_height(left), _score_height(right))
    )


def _score_rotate_left(node: _ScoreNode) -> _ScoreNode:
    pivot = node.right
    assert pivot is not None
    return _score_node(
        pivot.key,
        _score_node(node.key, node.left, pivot.left),
        pivot.right,
    )


def _score_rotate_right(node: _ScoreNode) -> _ScoreNode:
    pivot = node.left
    assert pivot is not None
    return _score_node(
        pivot.key,
        pivot.left,
        _score_node(node.key, pivot.right, node.right),
    )


def _score_balance(node: _ScoreNode) -> _ScoreNode:
    balance = _score_height(node.left) - _score_height(node.right)
    if balance > 1:
        left = node.left
        assert left is not None
        if _score_height(left.left) < _score_height(left.right):
            left = _score_rotate_left(left)
        return _score_rotate_right(_score_node(node.key, left, node.right))
    if balance < -1:
        right = node.right
        assert right is not None
        if _score_height(right.right) < _score_height(right.left):
            right = _score_rotate_right(right)
        return _score_rotate_left(_score_node(node.key, node.left, right))
    return node


def _score_add(node: _ScoreNode | None, key: _ScoreKey) -> _ScoreNode:
    if node is None:
        return _ScoreNode(key)
    if key == node.key:
        raise ValidationError("duplicate requirement score-index entry")
    if key < node.key:
        return _score_balance(
            _score_node(node.key, _score_add(node.left, key), node.right)
        )
    return _score_balance(_score_node(node.key, node.left, _score_add(node.right, key)))


def _score_least(node: _ScoreNode) -> _ScoreNode:
    current = node
    while current.left is not None:
        current = current.left
    return current


def _score_remove(node: _ScoreNode | None, key: _ScoreKey) -> _ScoreNode | None:
    if node is None:
        raise ValidationError("missing requirement score-index entry")
    if key < node.key:
        return _score_balance(
            _score_node(node.key, _score_remove(node.left, key), node.right)
        )
    if key > node.key:
        return _score_balance(
            _score_node(node.key, node.left, _score_remove(node.right, key))
        )
    if node.left is None:
        return node.right
    if node.right is None:
        return node.left
    successor = _score_least(node.right)
    return _score_balance(
        _score_node(
            successor.key,
            node.left,
            _score_remove(node.right, successor.key),
        )
    )


def _score_range(
    node: _ScoreNode | None,
    lower: float,
    upper: float,
    result: dict[str, None],
) -> None:
    if node is None:
        return
    lower_key = (lower, "")
    upper_key = (upper, "")
    if node.key >= lower_key:
        _score_range(node.left, lower, upper, result)
    if lower_key <= node.key < upper_key:
        result.setdefault(node.key[1], None)
    if node.key < upper_key:
        _score_range(node.right, lower, upper, result)


@dataclass(frozen=True, slots=True)
class _RequirementScoreIndex:
    support: _ScoreNode | None = None
    refute: _ScoreNode | None = None
    size: int = 0

    def add(self, observation: SemanticObservation) -> _RequirementScoreIndex:
        return _RequirementScoreIndex(
            _score_add(
                self.support,
                (observation.support_score, observation.observation_id),
            ),
            _score_add(
                self.refute,
                (observation.refute_score, observation.observation_id),
            ),
            self.size + 1,
        )

    def remove(self, observation: SemanticObservation) -> _RequirementScoreIndex:
        return _RequirementScoreIndex(
            _score_remove(
                self.support,
                (observation.support_score, observation.observation_id),
            ),
            _score_remove(
                self.refute,
                (observation.refute_score, observation.observation_id),
            ),
            self.size - 1,
        )

    def policy_candidates(
        self,
        old: DecisionPolicy,
        new: DecisionPolicy,
    ) -> tuple[str, ...]:
        if old.tie_rule_version != new.tie_rule_version:
            raise ValidationError("the frozen overlay supports tie rule v1 only")
        result: dict[str, None] = {}
        if old.support_threshold != new.support_threshold:
            lower = min(old.support_threshold, new.support_threshold)
            upper = max(old.support_threshold, new.support_threshold)
            _score_range(self.support, lower, upper, result)
        if old.refute_threshold != new.refute_threshold:
            lower = min(old.refute_threshold, new.refute_threshold)
            upper = max(old.refute_threshold, new.refute_threshold)
            _score_range(self.refute, lower, upper, result)
        return tuple(result)


class _AfterMapping(Mapping[str, _T]):
    """Point-overlay mapping used by certificate builders without a full copy."""

    def __init__(
        self,
        base: Mapping[str, _T],
        changes: Mapping[str, _T | None],
    ) -> None:
        self._base = base
        self._changes = changes

    def __getitem__(self, key: str) -> _T:
        if key in self._changes:
            value = self._changes[key]
            if value is None:
                raise KeyError(key)
            return value
        return self._base[key]

    def __iter__(self) -> Iterator[str]:
        yielded: set[str] = set()
        for key, value in self._changes.items():
            if value is not None:
                yielded.add(key)
                yield key
        for key in self._base:
            if key not in yielded and key not in self._changes:
                yield key

    def __len__(self) -> int:
        removed = sum(
            1
            for key, value in self._changes.items()
            if value is None and key in self._base
        )
        added = sum(
            1
            for key, value in self._changes.items()
            if value is not None and key not in self._base
        )
        return len(self._base) + added - removed


@dataclass(frozen=True, slots=True)
class _PreparedGroupPatch:
    group_version_id: str
    index: MaintainedCertificateIndex
    patch: PreparedObservationIndexPatch


@dataclass(slots=True)
class PreparedM5OverlayPatch:
    """Opaque, owner/revision-bound derived-state microtransaction."""

    event_id: str
    payload_hash: str
    expected_revision: int
    expected_point: SnapshotPoint
    point: SnapshotPoint
    policy: DecisionPolicy
    direct_patch: IncrementalStatePatch | None
    matching_patches: tuple[_PreparedGroupPatch, ...]
    index_changes: tuple[_PointChange[str, MaintainedCertificateIndex], ...]
    hall_changes: tuple[_PointChange[str, HallMaskState], ...]
    requirement_local_changes: tuple[_PointChange[str, _RequirementLocal], ...]
    edge_count_changes: tuple[_PointChange[tuple[str, str], int], ...]
    requirement_state_changes: tuple[_PointChange[str, RequirementState], ...]
    group_state_changes: tuple[_PointChange[str, GroupState], ...]
    complete_group_changes: tuple[_PointChange[str, PersistentStringSet], ...]
    claim_state_changes: tuple[_PointChange[str, CombinedClaimState], ...]
    answer_count_changes: tuple[
        _PointChange[str, tuple[tuple[ClaimStatus, int], ...]], ...
    ]
    answer_state_changes: tuple[_PointChange[str, CombinedAnswerState], ...]
    group_artifact_changes: tuple[
        _PointChange[str, GroupMatchingCertificateArtifact], ...
    ]
    group_binding_changes: tuple[_PointChange[str, WorkingGroupCertificateBinding], ...]
    group_history_changes: tuple[
        _PointChange[str, _HistoryNode[WorkingGroupCertificateBinding]], ...
    ]
    claim_artifact_changes: tuple[_PointChange[str, ClaimCertificateArtifact], ...]
    claim_binding_changes: tuple[_PointChange[str, WorkingClaimCertificateBinding], ...]
    claim_history_changes: tuple[
        _PointChange[str, _HistoryNode[WorkingClaimCertificateBinding]], ...
    ]
    group_binding_rows: tuple[WorkingGroupCertificateBinding, ...]
    claim_binding_rows: tuple[WorkingClaimCertificateBinding, ...]
    observation_changes: tuple[_PointChange[str, _ObservationInfo], ...]
    observations_by_requirement_changes: tuple[
        _PointChange[str, PersistentStringSet], ...
    ]
    observations_by_chunk_changes: tuple[_PointChange[str, PersistentStringSet], ...]
    score_index_before: _RequirementScoreIndex
    score_index_after: _RequirementScoreIndex
    result: M5OverlayApplyResult
    replay_result: M5OverlayApplyResult | None
    _owner: M5IncrementalOverlay
    _state: str = "prepared"


def _claim_status(*, supported: bool, refuted: bool) -> ClaimStatus:
    if supported and refuted:
        return ClaimStatus.CONFLICTED
    if supported:
        return ClaimStatus.SUPPORTED
    if refuted:
        return ClaimStatus.REFUTED
    return ClaimStatus.UNSUPPORTED


def _answer_status(counts: Counter[ClaimStatus], required: int) -> AnswerStatus:
    if counts[ClaimStatus.REFUTED] > 0:
        return AnswerStatus.CONTRADICTED
    if counts[ClaimStatus.CONFLICTED] > 0:
        return AnswerStatus.CONFLICTED
    if required > 0 and counts[ClaimStatus.SUPPORTED] == required:
        return AnswerStatus.VALID
    if counts[ClaimStatus.SUPPORTED] > 0:
        return AnswerStatus.PARTIALLY_SUPPORTED
    return AnswerStatus.UNSUPPORTED


def _selected_group_certificate(
    state: CombinedClaimState,
    group_certificates: Mapping[str, GroupMatchingCertificateArtifact],
) -> GroupMatchingCertificateArtifact | None:
    if state.supporting_observation_ids or not state.complete_group_ids:
        return None
    return group_certificates[state.complete_group_ids[0]]


def _claim_certificate_inputs_changed(
    state: CombinedClaimState,
    *,
    decision_policy_version: str,
    selected_group_certificate: GroupMatchingCertificateArtifact | None,
    prior_artifact: ClaimCertificateArtifact | None,
) -> bool:
    """Compare only the O(1) selector fields represented by a v2 artifact."""

    direct_support_id: str | None = None
    group_id: str | None = None
    group_digest: str | None = None
    if state.supporting_observation_ids:
        support_kind = ClaimSupportKind.DIRECT
        direct_support_id = state.supporting_observation_ids[0]
    elif state.complete_group_ids:
        support_kind = ClaimSupportKind.GROUP
        group_id = state.complete_group_ids[0]
        if selected_group_certificate is None:
            raise AssertionError("selected complete group has no certificate")
        group_digest = selected_group_certificate.certificate_digest
    else:
        support_kind = ClaimSupportKind.NONE
    direct_refute_id = (
        state.refuting_observation_ids[0] if state.refuting_observation_ids else None
    )
    return prior_artifact is None or (
        prior_artifact.decision_policy_version != decision_policy_version
        or prior_artifact.support_kind is not support_kind
        or prior_artifact.direct_support_observation_id != direct_support_id
        or prior_artifact.group_version_id != group_id
        or prior_artifact.group_certificate_digest != group_digest
        or prior_artifact.direct_refute_observation_id != direct_refute_id
    )


def _counter_image(
    counts: Counter[ClaimStatus],
) -> tuple[tuple[ClaimStatus, int], ...]:
    return tuple(
        (status, counts[status])
        for status in (
            ClaimStatus.CONFLICTED,
            ClaimStatus.REFUTED,
            ClaimStatus.SUPPORTED,
            ClaimStatus.UNSUPPORTED,
        )
        if status in counts
    )


def _event_digest(event: CommittedOverlayEvent) -> str:
    if isinstance(
        event,
        (
            RegisterGroupEvent,
            ReplaceGroupEvent,
            RetireGroupEvent,
            ObserveRequirementEvent,
        ),
    ):
        return m5_event_payload_digest(event)
    return legacy_event_payload_digest(event)


def _point_changes(
    current: Mapping[_K, _T],
    after: Mapping[_K, _T | None],
) -> tuple[_PointChange[_K, _T], ...]:
    changes: list[_PointChange[_K, _T]] = []
    for key, value in after.items():
        before = current.get(key)
        if before is value:
            continue
        if isinstance(before, _HistoryNode) or isinstance(value, _HistoryNode):
            changes.append(_PointChange(key, before, value))
        elif before != value:
            changes.append(_PointChange(key, before, value))
    return tuple(changes)


def _history_after_group_transition(
    history: _HistoryNode[WorkingGroupCertificateBinding] | None,
    prior: WorkingGroupCertificateBinding | None,
    closed: WorkingGroupCertificateBinding | None,
    opened: WorkingGroupCertificateBinding | None,
) -> tuple[
    _HistoryNode[WorkingGroupCertificateBinding] | None,
    tuple[WorkingGroupCertificateBinding, ...],
]:
    result = history
    rows: list[WorkingGroupCertificateBinding] = []
    if closed is not None:
        if prior is None or result is None or result.value != prior:
            raise AssertionError("group certificate history lost its open tail")
        result = _HistoryNode(closed, result.previous, result.length)
        rows.append(closed)
    if opened is not None and opened != prior:
        result = _history_append(result, opened)
        rows.append(opened)
    return result, tuple(rows)


def _history_after_claim_transition(
    history: _HistoryNode[WorkingClaimCertificateBinding] | None,
    prior: WorkingClaimCertificateBinding | None,
    closed: WorkingClaimCertificateBinding | None,
    opened: WorkingClaimCertificateBinding,
) -> tuple[
    _HistoryNode[WorkingClaimCertificateBinding] | None,
    tuple[WorkingClaimCertificateBinding, ...],
]:
    result = history
    rows: list[WorkingClaimCertificateBinding] = []
    if closed is not None:
        if prior is None or result is None or result.value != prior:
            raise AssertionError("claim certificate history lost its open tail")
        result = _HistoryNode(closed, result.previous, result.length)
        rows.append(closed)
    if opened != prior:
        result = _history_append(result, opened)
        rows.append(opened)
    return result, tuple(rows)


def _frame_bytes(payload: bytes) -> bytes:
    return len(payload).to_bytes(8, "big") + payload


def _logical_value_bytes(value: object) -> bytes:
    """Canonical typed bytes for the versioned in-memory output contract."""

    if value is None:
        return b"n"
    if isinstance(value, bool):
        return b"b\x01" if value else b"b\x00"
    if isinstance(value, Enum):
        return b"e" + _frame_bytes(str(value.value).encode("utf-8"))
    if isinstance(value, str):
        return b"s" + _frame_bytes(value.encode("utf-8"))
    if isinstance(value, int):
        return b"i" + _frame_bytes(str(value).encode("ascii"))
    if isinstance(value, float):
        return b"f" + struct.pack(">d", value)
    if isinstance(value, tuple):
        return (
            b"q"
            + len(value).to_bytes(8, "big")
            + b"".join(_frame_bytes(_logical_value_bytes(item)) for item in value)
        )
    if is_dataclass(value) and not isinstance(value, type):
        record_fields = fields(value)
        return (
            b"d"
            + _frame_bytes(type(value).__qualname__.encode("utf-8"))
            + len(record_fields).to_bytes(8, "big")
            + b"".join(
                _frame_bytes(field_info.name.encode("utf-8"))
                + _frame_bytes(_logical_value_bytes(getattr(value, field_info.name)))
                for field_info in record_fields
            )
        )
    raise TypeError(f"unsupported M5 logical output value: {type(value)!r}")


def _logical_output_image(records: Sequence[tuple[str, str, object]]) -> bytes:
    """Encode deterministic first-touch output as logical-output-v2 bytes.

    Producers emit fixed relation blocks. Within each block they preserve
    first-touch key order; repeated binding rows retain close-before-open
    order. Empty output remains explicitly domain separated.
    """

    return _logical_value_bytes(("m5-overlay-logical-output-v2", tuple(records)))


@dataclass(slots=True)
class M5IncrementalOverlay:
    """Maintained direct-plus-group state for one in-memory M5 repository."""

    direct_engine: IncrementalMaintenanceEngine
    _point: SnapshotPoint
    _policy: DecisionPolicy
    _group_indexes: dict[str, MaintainedCertificateIndex] = field(default_factory=dict)
    _hall_states: dict[str, HallMaskState] = field(default_factory=dict)
    _requirement_locals: dict[str, _RequirementLocal] = field(default_factory=dict)
    _edge_counts: dict[tuple[str, str], int] = field(default_factory=dict)
    _requirement_states: dict[str, RequirementState] = field(default_factory=dict)
    _group_states: dict[str, GroupState] = field(default_factory=dict)
    _complete_groups_by_claim: dict[str, PersistentStringSet] = field(
        default_factory=dict
    )
    _claim_states: dict[str, CombinedClaimState] = field(default_factory=dict)
    _answer_counts: dict[str, Counter[ClaimStatus]] = field(default_factory=dict)
    _answer_states: dict[str, CombinedAnswerState] = field(default_factory=dict)
    _group_artifacts: dict[str, GroupMatchingCertificateArtifact] = field(
        default_factory=dict
    )
    _group_bindings: dict[str, WorkingGroupCertificateBinding] = field(
        default_factory=dict
    )
    _group_binding_history: dict[str, _HistoryNode[WorkingGroupCertificateBinding]] = (
        field(default_factory=dict)
    )
    _claim_artifacts: dict[str, ClaimCertificateArtifact] = field(default_factory=dict)
    _claim_bindings: dict[str, WorkingClaimCertificateBinding] = field(
        default_factory=dict
    )
    _claim_binding_history: dict[str, _HistoryNode[WorkingClaimCertificateBinding]] = (
        field(default_factory=dict)
    )
    _observations: dict[str, _ObservationInfo] = field(default_factory=dict)
    _observations_by_requirement: dict[str, PersistentStringSet] = field(
        default_factory=dict
    )
    _observations_by_chunk: dict[str, PersistentStringSet] = field(default_factory=dict)
    _score_index: _RequirementScoreIndex = field(default_factory=_RequirementScoreIndex)
    _claim_to_answer: dict[str, str] = field(default_factory=dict)
    _claim_required: dict[str, bool] = field(default_factory=dict)
    _answer_required_count: dict[str, int] = field(default_factory=dict)
    _processed_events: dict[str, tuple[str, M5OverlayApplyResult]] = field(
        default_factory=dict
    )
    _state_revision: int = 0
    last_work: M5OverlayWork = field(default_factory=M5OverlayWork)

    @classmethod
    def from_repository(cls, repository: M5Repository) -> M5IncrementalOverlay:
        """Bootstrap maintained state; full scans are confined to this adapter."""

        point = repository.current_point
        policy = repository.base.current_policy()
        overlay = cls(
            direct_engine=IncrementalMaintenanceEngine.from_repository(repository.base),
            _point=point,
            _policy=policy,
        )

        for claim_id in repository.base.all_claim_ids():
            claim = repository.base.claim(claim_id)
            overlay._claim_to_answer[claim_id] = claim.answer_version_id
            overlay._claim_required[claim_id] = claim.required
            overlay._complete_groups_by_claim[claim_id] = PersistentStringSet()
        for answer_id in repository.base.all_answer_ids():
            overlay._answer_required_count[answer_id] = sum(
                1
                for claim_id in repository.base.claim_ids_of_answer(answer_id)
                if overlay._claim_required[claim_id]
            )

        for group_id in repository.active_group_ids(point.epoch_id):
            group = repository.group(group_id)
            overlay._install_empty_group(group)

        bootstrap_deltas: dict[str, list[ObservationMembershipDelta]] = {}
        for record in repository.current_requirement_observations(point):
            info = overlay._info_if_active(repository, record.observation, point)
            if info is None:
                continue
            overlay._add_bootstrap_info(info)
            if decide(info.observation, policy) is VerificationLabel.SUPPORT:
                bootstrap_deltas.setdefault(info.group_version_id, []).append(
                    ObservationMembershipDelta(
                        info.requirement_ordinal,
                        info.text_hash,
                        info.observation.observation_id,
                        1,
                    )
                )
                overlay._add_bootstrap_membership(info)

        for group_id, index in overlay._group_indexes.items():
            patch = index.prepare_observation_deltas(bootstrap_deltas.get(group_id, ()))
            index.apply_prepared_observation_deltas(patch)
            hall = apply_hash_mask_transitions(
                overlay._hall_states[group_id], patch.transitions
            ).state
            overlay._hall_states[group_id] = hall

        for requirement_id in tuple(overlay._requirement_locals):
            overlay._requirement_states[requirement_id] = (
                overlay._requirement_state_from_local(
                    requirement_id,
                    overlay._requirement_locals[requirement_id],
                )
            )

        for group_id in tuple(overlay._group_indexes):
            group = repository.group(group_id)
            bootstrap_group_state = overlay._group_state_from_parts(
                group,
                overlay._hall_states[group_id],
                overlay._requirement_states,
            )
            overlay._group_states[group_id] = bootstrap_group_state
            if not bootstrap_group_state.complete:
                continue
            view = overlay._group_indexes[group_id].current_view(
                point=point,
                decision_policy_version=policy.policy_version,
            )
            group_transition = build_or_rebuild_certificate(view)
            group_artifact = group_transition.artifact
            group_binding = group_transition.open_binding
            assert group_artifact is not None and group_binding is not None
            overlay._group_artifacts[group_id] = group_artifact
            overlay._group_bindings[group_id] = group_binding
            overlay._group_binding_history[group_id] = _history_append(
                None, group_binding
            )
            claim_id = group.owner_claim_id
            values, added = overlay._complete_groups_by_claim[claim_id].add(group_id)
            assert added
            overlay._complete_groups_by_claim[claim_id] = values

        for claim_id in repository.base.all_claim_ids():
            direct = overlay.direct_engine.claim_state(claim_id)
            combined = overlay._combined_claim_from_parts(
                direct,
                overlay._complete_groups_by_claim[claim_id],
            )
            overlay._claim_states[claim_id] = combined

        overlay._rebuild_answers(repository)
        for claim_id, bootstrap_claim_state in overlay._claim_states.items():
            claim_transition = transition_claim_certificate(
                bootstrap_claim_state,
                point=point,
                decision_policy_version=policy.policy_version,
                group_certificates=overlay._group_artifacts,
                prior_binding=None,
                prior_artifact=None,
            )
            overlay._claim_artifacts[claim_id] = claim_transition.artifact
            overlay._claim_bindings[claim_id] = claim_transition.binding
            overlay._claim_binding_history[claim_id] = _history_append(
                None, claim_transition.binding
            )
        overlay.last_work = M5OverlayWork()
        return overlay

    @property
    def point(self) -> SnapshotPoint:
        return self._point

    @property
    def state_revision(self) -> int:
        return self._state_revision

    @property
    def requirement_states(self) -> dict[str, RequirementState]:
        return dict(self._requirement_states)

    @property
    def group_states(self) -> dict[str, GroupState]:
        return dict(self._group_states)

    @property
    def claim_states(self) -> dict[str, CombinedClaimState]:
        return dict(self._claim_states)

    @property
    def answer_states(self) -> dict[str, CombinedAnswerState]:
        return dict(self._answer_states)

    @property
    def group_certificates(self) -> dict[str, GroupMatchingCertificateArtifact]:
        return dict(self._group_artifacts)

    @property
    def claim_certificates(self) -> dict[str, ClaimCertificateArtifact]:
        return dict(self._claim_artifacts)

    def requirement_state(self, requirement_version_id: str) -> RequirementState:
        return self._requirement_states[requirement_version_id]

    def group_state(self, group_version_id: str) -> GroupState:
        return self._group_states[group_version_id]

    def claim_state(self, claim_id: str) -> CombinedClaimState:
        return self._claim_states[claim_id]

    def answer_state(self, answer_id: str) -> CombinedAnswerState:
        return self._answer_states[answer_id]

    def group_binding_history(
        self, group_version_id: str
    ) -> tuple[WorkingGroupCertificateBinding, ...]:
        return _history_materialize(self._group_binding_history.get(group_version_id))

    def claim_binding_history(
        self, claim_id: str
    ) -> tuple[WorkingClaimCertificateBinding, ...]:
        return _history_materialize(self._claim_binding_history.get(claim_id))

    def matching_audit_issues(self) -> dict[str, tuple[str, ...]]:
        """Run explicit out-of-band full index audits."""

        return {
            group_id: issues
            for group_id, index in self._group_indexes.items()
            if (issues := index.audit_issues())
        }

    def _install_empty_group(self, group: EvidenceGroupVersion) -> None:
        group_id = group.group_version_id
        index = MaintainedCertificateIndex(
            group_version_id=group_id,
            requirement_version_ids=tuple(
                requirement.requirement_version_id for requirement in group.requirements
            ),
        )
        self._group_indexes[group_id] = index
        self._hall_states[group_id] = initialize_hall_mask_state(
            len(group.requirements), ()
        ).state
        for requirement in group.requirements:
            self._requirement_locals[requirement.requirement_version_id] = (
                _RequirementLocal()
            )

    def _info_if_active(
        self,
        repository: M5Repository,
        observation: SemanticObservation,
        point: SnapshotPoint,
    ) -> _ObservationInfo | None:
        if observation.task_type != CANONICAL_REQUIREMENT_TASK:
            return None
        if point != repository.current_point:
            raise ValidationError(
                "incremental observation activation requires the current point"
            )
        if not repository.is_requirement_active(observation.subject_id, point.epoch_id):
            return None
        if not repository.base.is_chunk_active(observation.chunk_version_id):
            return None
        requirement = repository.requirement(observation.subject_id)
        group = repository.group(requirement.group_version_id)
        text_hash = normalized_text_hash_v1(
            repository.base.chunk_version(observation.chunk_version_id).text
        )
        return _ObservationInfo(
            observation=observation,
            group_version_id=group.group_version_id,
            requirement_version_id=requirement.requirement_version_id,
            requirement_ordinal=requirement.ordinal,
            text_hash=text_hash,
        )

    def _add_bootstrap_info(self, info: _ObservationInfo) -> None:
        observation_id = info.observation.observation_id
        self._observations[observation_id] = info
        requirement_values = self._observations_by_requirement.get(
            info.requirement_version_id, PersistentStringSet()
        )
        requirement_values, added = requirement_values.add(observation_id)
        assert added
        self._observations_by_requirement[info.requirement_version_id] = (
            requirement_values
        )
        chunk_values = self._observations_by_chunk.get(
            info.observation.chunk_version_id, PersistentStringSet()
        )
        chunk_values, added = chunk_values.add(observation_id)
        assert added
        self._observations_by_chunk[info.observation.chunk_version_id] = chunk_values
        self._score_index = self._score_index.add(info.observation)

    def _add_bootstrap_membership(self, info: _ObservationInfo) -> None:
        requirement_id = info.requirement_version_id
        edge = (requirement_id, info.text_hash)
        old_count = self._edge_counts.get(edge, 0)
        self._edge_counts[edge] = old_count + 1
        local = self._requirement_locals[requirement_id]
        hashes = local.witness_hashes
        if old_count == 0:
            hashes, added = hashes.add(info.text_hash)
            assert added
        observations, added = local.observation_ids.add(info.observation.observation_id)
        assert added
        self._requirement_locals[requirement_id] = _RequirementLocal(
            hashes, observations
        )

    @staticmethod
    def _requirement_state_from_local(
        requirement_id: str,
        local: _RequirementLocal,
    ) -> RequirementState:
        hashes = local.witness_hashes.items()
        observations = local.observation_ids.items()
        return RequirementState(
            requirement_version_id=requirement_id,
            witness_hashes=hashes,
            supporting_observation_ids=observations,
            witness_count=len(hashes),
            satisfied=bool(hashes),
        )

    @staticmethod
    def _group_state_from_parts(
        group: EvidenceGroupVersion,
        hall: HallMaskState,
        requirement_states: Mapping[str, RequirementState],
    ) -> GroupState:
        satisfied = sum(
            int(requirement_states[requirement.requirement_version_id].satisfied)
            for requirement in group.requirements
        )
        return GroupState(
            group_version_id=group.group_version_id,
            requirement_count=len(group.requirements),
            satisfied_count=satisfied,
            matching_size=hall.matching_size,
            complete=hall.complete,
        )

    @staticmethod
    def _combined_claim_from_parts(
        direct: ClaimState,
        complete_groups: PersistentStringSet,
    ) -> CombinedClaimState:
        group_ids = complete_groups.items()
        supported = direct.support_count > 0 or bool(group_ids)
        refuted = direct.refute_count > 0
        return CombinedClaimState(
            claim_id=direct.claim_id,
            support_count=direct.support_count,
            refute_count=direct.refute_count,
            best_support_score=direct.best_support_score,
            best_refute_score=direct.best_refute_score,
            supporting_observation_ids=direct.supporting_observation_ids,
            refuting_observation_ids=direct.refuting_observation_ids,
            complete_group_count=len(group_ids),
            complete_group_ids=group_ids,
            status=_claim_status(supported=supported, refuted=refuted),
        )

    def _rebuild_answers(self, repository: M5Repository) -> None:
        for answer_id in repository.base.all_answer_ids():
            counts: Counter[ClaimStatus] = Counter()
            for claim_id in repository.base.claim_ids_of_answer(answer_id):
                if self._claim_required[claim_id]:
                    counts[self._claim_states[claim_id].status] += 1
            self._answer_counts[answer_id] = counts
            self._answer_states[answer_id] = self._answer_from_counts(answer_id, counts)

    def _answer_from_counts(
        self,
        answer_id: str,
        counts: Counter[ClaimStatus],
    ) -> CombinedAnswerState:
        required = self._answer_required_count[answer_id]
        return CombinedAnswerState(
            answer_version_id=answer_id,
            required_claim_count=required,
            supported_count=counts[ClaimStatus.SUPPORTED],
            unsupported_count=counts[ClaimStatus.UNSUPPORTED],
            refuted_count=counts[ClaimStatus.REFUTED],
            conflicted_count=counts[ClaimStatus.CONFLICTED],
            status=_answer_status(counts, required),
        )

    def prepare_committed_event_patch(
        self,
        event: CommittedOverlayEvent,
        before: M5Repository,
        after: M5Repository,
    ) -> PreparedM5OverlayPatch:
        """Prepare one nonmutating, coalesced committed-event transaction."""

        if not event.event_id.strip():
            raise ValidationError("event_id must be a nonempty identifier")
        payload_hash = _event_digest(event)
        recorded = self._processed_events.get(event.event_id)
        if recorded is not None:
            recorded_hash, stored = recorded
            if recorded_hash != payload_hash:
                raise EventConflictError(
                    f"event {event.event_id} was already processed with another payload"
                )
            replay = replace(
                stored,
                published_group_bindings=(),
                published_claim_bindings=(),
                work=M5OverlayWork(),
                direct_stats=MaintenanceStats(),
                replayed=True,
            )
            return PreparedM5OverlayPatch(
                event_id=event.event_id,
                payload_hash=payload_hash,
                expected_revision=self._state_revision,
                expected_point=self._point,
                point=self._point,
                policy=self._policy,
                direct_patch=None,
                matching_patches=(),
                index_changes=(),
                hall_changes=(),
                requirement_local_changes=(),
                edge_count_changes=(),
                requirement_state_changes=(),
                group_state_changes=(),
                complete_group_changes=(),
                claim_state_changes=(),
                answer_count_changes=(),
                answer_state_changes=(),
                group_artifact_changes=(),
                group_binding_changes=(),
                group_history_changes=(),
                claim_artifact_changes=(),
                claim_binding_changes=(),
                claim_history_changes=(),
                observation_changes=(),
                observations_by_requirement_changes=(),
                observations_by_chunk_changes=(),
                score_index_before=self._score_index,
                score_index_after=self._score_index,
                result=replay,
                replay_result=replay,
                group_binding_rows=(),
                claim_binding_rows=(),
                _owner=self,
            )

        before_point = before.current_point
        after_point = after.current_point
        if before_point != self._point:
            raise ValidationError("overlay head does not match the before repository")
        if after_point <= before_point:
            raise ValidationError("a committed non-replay event must advance its point")

        if isinstance(
            event,
            (
                RegisterGroupEvent,
                ReplaceGroupEvent,
                RetireGroupEvent,
                ObserveRequirementEvent,
            ),
        ):
            direct_patch = self.direct_engine.prepare_noop_event_patch(event.event_id)
        else:
            direct_patch = self.direct_engine.prepare_committed_event_patch(
                event, before.base, after.base
            )
        policy_before = before.base.current_policy()
        policy_after = after.base.current_policy()
        policy_changed = policy_before.policy_version != policy_after.policy_version

        observation_values: dict[str, _ObservationInfo | None] = {}
        by_requirement_values: dict[str, PersistentStringSet | None] = {}
        by_chunk_values: dict[str, PersistentStringSet | None] = {}
        score_after = self._score_index
        score_point_operations = 0
        membership_by_group: dict[str, list[ObservationMembershipDelta]] = {}
        observation_changes_processed = 0
        touched_groups = _OrderedKeys[str]()
        removed_groups: dict[str, EvidenceGroupVersion] = {}
        added_groups: dict[str, EvidenceGroupVersion] = {}

        def current_observation(observation_id: str) -> _ObservationInfo | None:
            if observation_id in observation_values:
                return observation_values[observation_id]
            return self._observations.get(observation_id)

        def current_reverse(
            base: Mapping[str, PersistentStringSet],
            values: Mapping[str, PersistentStringSet | None],
            key: str,
        ) -> PersistentStringSet:
            if key in values:
                return values[key] or PersistentStringSet()
            return base.get(key, PersistentStringSet())

        def stage_reverse(
            base: Mapping[str, PersistentStringSet],
            values: dict[str, PersistentStringSet | None],
            key: str,
            observation_id: str,
            *,
            add: bool,
        ) -> None:
            current = current_reverse(base, values, key)
            if add:
                updated, changed = current.add(observation_id)
            else:
                updated, changed = current.remove(observation_id)
            if not changed:
                raise AssertionError("requirement observation reverse index drift")
            values[key] = updated if len(updated) else None

        def emit_membership(info: _ObservationInfo, delta: int) -> None:
            membership_by_group.setdefault(info.group_version_id, []).append(
                ObservationMembershipDelta(
                    info.requirement_ordinal,
                    info.text_hash,
                    info.observation.observation_id,
                    delta,
                )
            )
            touched_groups.add(info.group_version_id)

        def withdraw_info(observation_id: str) -> None:
            nonlocal score_after, score_point_operations
            nonlocal observation_changes_processed
            info = current_observation(observation_id)
            if info is None:
                return
            observation_values[observation_id] = None
            stage_reverse(
                self._observations_by_requirement,
                by_requirement_values,
                info.requirement_version_id,
                observation_id,
                add=False,
            )
            stage_reverse(
                self._observations_by_chunk,
                by_chunk_values,
                info.observation.chunk_version_id,
                observation_id,
                add=False,
            )
            score_after = score_after.remove(info.observation)
            score_point_operations += 2
            observation_changes_processed += 1
            if decide(info.observation, policy_before) is VerificationLabel.SUPPORT:
                emit_membership(info, -1)

        def add_info(info: _ObservationInfo) -> None:
            nonlocal score_after, score_point_operations
            nonlocal observation_changes_processed
            observation_id = info.observation.observation_id
            if current_observation(observation_id) is not None:
                raise AssertionError("requirement observation is already maintained")
            observation_values[observation_id] = info
            stage_reverse(
                self._observations_by_requirement,
                by_requirement_values,
                info.requirement_version_id,
                observation_id,
                add=True,
            )
            stage_reverse(
                self._observations_by_chunk,
                by_chunk_values,
                info.observation.chunk_version_id,
                observation_id,
                add=True,
            )
            score_after = score_after.add(info.observation)
            score_point_operations += 2
            observation_changes_processed += 1
            if decide(info.observation, policy_after) is VerificationLabel.SUPPORT:
                emit_membership(info, 1)

        def withdraw_requirement(requirement_id: str) -> None:
            values = current_reverse(
                self._observations_by_requirement,
                by_requirement_values,
                requirement_id,
            )
            for observation_id in values.items():
                withdraw_info(observation_id)

        if isinstance(event, RegisterGroupEvent):
            added_groups[event.group.group_version_id] = event.group
            touched_groups.add(event.group.group_version_id)
        elif isinstance(event, ReplaceGroupEvent):
            old = before.group(event.old_group_version_id)
            removed_groups[old.group_version_id] = old
            for requirement in old.requirements:
                withdraw_requirement(requirement.requirement_version_id)
            added_groups[event.successor.group_version_id] = event.successor
            touched_groups.update(
                (old.group_version_id, event.successor.group_version_id)
            )
        elif isinstance(event, RetireGroupEvent):
            old = before.group(event.group_version_id)
            removed_groups[old.group_version_id] = old
            for requirement in old.requirements:
                withdraw_requirement(requirement.requirement_version_id)
            touched_groups.add(old.group_version_id)
        elif isinstance(event, ObserveRequirementEvent):
            key = event.observation.key
            old_id = before.currency_observation_id_at(key, before_point)
            new_id = after.currency_observation_id_at(key, after_point)
            if old_id != new_id:
                if old_id is not None:
                    withdraw_info(old_id)
                if new_id is not None:
                    record = after.requirement_observation(new_id)
                    info = self._info_if_active(after, record.observation, after_point)
                    if info is not None:
                        add_info(info)
        elif isinstance(event, DeleteDocumentVersionEvent):
            for chunk_id in before.base.chunk_ids_of_document_version(
                event.document_version_id
            ):
                values = current_reverse(
                    self._observations_by_chunk, by_chunk_values, chunk_id
                )
                for observation_id in values.items():
                    withdraw_info(observation_id)
        elif isinstance(event, ReplaceDocumentVersionEvent):
            for chunk_id in before.base.chunk_ids_of_document_version(
                event.old_document_version_id
            ):
                values = current_reverse(
                    self._observations_by_chunk, by_chunk_values, chunk_id
                )
                for observation_id in values.items():
                    withdraw_info(observation_id)

        policy_candidates: tuple[str, ...] = ()
        changed_threshold_dimensions = 0
        if isinstance(event, PolicyChangeEvent):
            changed_threshold_dimensions = int(
                policy_before.support_threshold != policy_after.support_threshold
            ) + int(policy_before.refute_threshold != policy_after.refute_threshold)
            policy_candidates = self._score_index.policy_candidates(
                policy_before, policy_after
            )
            for observation_id in policy_candidates:
                info = self._observations[observation_id]
                old_support = (
                    decide(info.observation, policy_before) is VerificationLabel.SUPPORT
                )
                new_support = (
                    decide(info.observation, policy_after) is VerificationLabel.SUPPORT
                )
                if old_support != new_support:
                    emit_membership(info, 1 if new_support else -1)
            # Policy identity, not just edge flips, rebinds every complete
            # group and every typed-v2 claim.
            touched_groups.update(after.active_group_ids(after_point.epoch_id))

        requirement_local_values: dict[str, _RequirementLocal | None] = {}
        edge_count_values: dict[tuple[str, str], int | None] = {}
        touched_requirements = _OrderedKeys[str]()

        for group in added_groups.values():
            for requirement in group.requirements:
                requirement_local_values[requirement.requirement_version_id] = (
                    _RequirementLocal()
                )
                touched_requirements.add(requirement.requirement_version_id)

        def current_local(requirement_id: str) -> _RequirementLocal:
            if requirement_id in requirement_local_values:
                value = requirement_local_values[requirement_id]
                if value is None:
                    raise AssertionError("cannot update a retired requirement")
                return value
            return self._requirement_locals[requirement_id]

        def current_edge_count(edge: tuple[str, str]) -> int:
            if edge in edge_count_values:
                return edge_count_values[edge] or 0
            return self._edge_counts.get(edge, 0)

        for group_id, membership_deltas in membership_by_group.items():
            if group_id in added_groups:
                group = added_groups[group_id]
            else:
                group = before.group(group_id)
            requirement_ids = tuple(
                requirement.requirement_version_id for requirement in group.requirements
            )
            for delta in membership_deltas:
                requirement_id = requirement_ids[delta.requirement_ordinal]
                local = current_local(requirement_id)
                edge = (requirement_id, delta.text_hash)
                old_count = current_edge_count(edge)
                new_count = old_count + delta.delta
                if new_count < 0:
                    raise ValidationError("requirement edge refcount underflow")
                edge_count_values[edge] = new_count or None
                hashes = local.witness_hashes
                if old_count == 0 and new_count > 0:
                    hashes, changed = hashes.add(delta.text_hash)
                    if not changed:
                        raise AssertionError("missing requirement hash crossing")
                elif old_count > 0 and new_count == 0:
                    hashes, changed = hashes.remove(delta.text_hash)
                    if not changed:
                        raise AssertionError("missing requirement hash crossing")
                observations = local.observation_ids
                if delta.delta > 0:
                    observations, changed = observations.add(delta.observation_id)
                else:
                    observations, changed = observations.remove(delta.observation_id)
                if not changed:
                    raise AssertionError("requirement provenance membership drift")
                requirement_local_values[requirement_id] = _RequirementLocal(
                    hashes, observations
                )
                touched_requirements.add(requirement_id)

        for group in removed_groups.values():
            for requirement in group.requirements:
                requirement_id = requirement.requirement_version_id
                requirement_local_values[requirement_id] = None
                touched_requirements.add(requirement_id)

        requirement_state_values: dict[str, RequirementState | None] = {}
        for requirement_id in touched_requirements:
            requirement_local: _RequirementLocal | None = requirement_local_values.get(
                requirement_id, self._requirement_locals.get(requirement_id)
            )
            requirement_state_values[requirement_id] = (
                None
                if requirement_local is None
                else self._requirement_state_from_local(
                    requirement_id, requirement_local
                )
            )

        matching_work = requirement_observation_work(
            changes_processed=observation_changes_processed,
            ordered_index_operations=score_point_operations,
        ) + policy_range_probe_work(
            changed_threshold_dimensions=changed_threshold_dimensions,
            candidate_observations=len(policy_candidates),
        )
        matching_patches: list[_PreparedGroupPatch] = []
        index_values: dict[str, MaintainedCertificateIndex | None] = {}
        hall_values: dict[str, HallMaskState | None] = {}
        hall_after_by_group: dict[str, HallMaskState] = {}

        for group_id in touched_groups:
            if group_id in added_groups:
                group = added_groups[group_id]
                index = MaintainedCertificateIndex(
                    group_version_id=group_id,
                    requirement_version_ids=tuple(
                        requirement.requirement_version_id
                        for requirement in group.requirements
                    ),
                )
                hall_before = initialize_hall_mask_state(
                    len(group.requirements), ()
                ).state
                index_values[group_id] = index
            else:
                index = self._group_indexes[group_id]
                hall_before = self._hall_states[group_id]
            index_patch = index.prepare_observation_deltas(
                membership_by_group.get(group_id, ())
            )
            hall_result = apply_hash_mask_transitions(
                hall_before, index_patch.transitions
            )
            matching_work += index_patch.work + hall_result.work
            matching_patches.append(_PreparedGroupPatch(group_id, index, index_patch))
            if group_id in removed_groups:
                index_values[group_id] = None
                hall_values[group_id] = None
            else:
                hall_values[group_id] = hall_result.state
                hall_after_by_group[group_id] = hall_result.state

        requirement_state_after = _AfterMapping(
            self._requirement_states, requirement_state_values
        )
        group_state_values: dict[str, GroupState | None] = {}
        group_artifact_values: dict[str, GroupMatchingCertificateArtifact | None] = {}
        group_binding_values: dict[str, WorkingGroupCertificateBinding | None] = {}
        group_history_values: dict[
            str, _HistoryNode[WorkingGroupCertificateBinding] | None
        ] = {}
        group_binding_rows: list[WorkingGroupCertificateBinding] = []
        certificate_changed_groups = _OrderedKeys[str]()
        complete_group_values: dict[str, PersistentStringSet | None] = {}
        claim_state_dirty = _OrderedKeys(direct_patch.touched_claim_ids)

        def current_complete_groups(claim_id: str) -> PersistentStringSet:
            if claim_id in complete_group_values:
                value = complete_group_values[claim_id]
                assert value is not None
                return value
            return self._complete_groups_by_claim[claim_id]

        def set_group_completeness(
            claim_id: str,
            group_id: str,
            *,
            old_complete: bool,
            new_complete: bool,
        ) -> bool:
            if old_complete == new_complete:
                return False
            values = current_complete_groups(claim_id)
            if new_complete:
                values, changed = values.add(group_id)
            else:
                values, changed = values.remove(group_id)
            if not changed:
                raise AssertionError("complete-group index drift")
            complete_group_values[claim_id] = values
            return True

        group_patch_by_id = {item.group_version_id: item for item in matching_patches}
        for group_id in touched_groups:
            old_state = self._group_states.get(group_id)
            if group_id in removed_groups:
                group = removed_groups[group_id]
                group_state_values[group_id] = None
                group_artifact_values[group_id] = None
                group_binding_values[group_id] = None
                completeness_changed = set_group_completeness(
                    group.owner_claim_id,
                    group_id,
                    old_complete=old_state is not None and old_state.complete,
                    new_complete=False,
                )
                if completeness_changed:
                    claim_state_dirty.add(group.owner_claim_id)
                continue

            group = added_groups.get(group_id, after.group(group_id))
            new_state = self._group_state_from_parts(
                group,
                hall_after_by_group[group_id],
                requirement_state_after,
            )
            group_state_values[group_id] = new_state
            completeness_changed = set_group_completeness(
                group.owner_claim_id,
                group_id,
                old_complete=old_state is not None and old_state.complete,
                new_complete=new_state.complete,
            )
            if completeness_changed:
                claim_state_dirty.add(group.owner_claim_id)

            prepared_group = group_patch_by_id[group_id]
            view = prepared_group.patch.preview_view(
                point=after_point,
                decision_policy_version=policy_after.policy_version,
            )
            prior_artifact = self._group_artifacts.get(group_id)
            prior_binding = self._group_bindings.get(group_id)
            transition = None
            if prior_artifact is None:
                if new_state.complete:
                    transition = build_or_rebuild_certificate(view)
            elif prior_binding is None:
                raise AssertionError("group artifact is missing its binding")
            elif after_point.epoch_id > prior_binding.epoch_id:
                transition = carry_forward_certificate_epoch(
                    view,
                    prior_artifact=prior_artifact,
                    prior_binding=prior_binding,
                )
            elif policy_changed:
                transition = rebind_certificate_policy(
                    view,
                    prior_artifact=prior_artifact,
                    prior_binding=prior_binding,
                )
            elif not new_state.complete:
                transition = close_incomplete_certificate(
                    view,
                    hall_state=hall_after_by_group[group_id],
                    prior_artifact=prior_artifact,
                    prior_binding=prior_binding,
                )
            else:
                try:
                    transition = repair_selected_observations(
                        view,
                        prior_artifact=prior_artifact,
                        prior_binding=prior_binding,
                    )
                except CertificateRebuildRequired:
                    transition = build_or_rebuild_certificate(
                        view,
                        prior_artifact=prior_artifact,
                        prior_binding=prior_binding,
                    )

            if transition is not None:
                matching_work += transition.work
                group_artifact_values[group_id] = transition.artifact
                group_binding_values[group_id] = transition.open_binding
                history = self._group_binding_history.get(group_id)
                next_history, group_rows = _history_after_group_transition(
                    history,
                    prior_binding,
                    transition.closed_binding,
                    transition.open_binding,
                )
                group_binding_rows.extend(group_rows)
                if next_history is not history:
                    group_history_values[group_id] = next_history
                if transition.artifact != prior_artifact:
                    certificate_changed_groups.add(group_id)

        complete_groups_after = _AfterMapping(
            self._complete_groups_by_claim, complete_group_values
        )
        direct_after = self.direct_engine.preview_claim_states_after_patch(
            direct_patch, claim_state_dirty
        )
        claim_state_values: dict[str, CombinedClaimState | None] = {}
        dirty_answers = _OrderedKeys[str]()
        answer_count_values: dict[str, tuple[tuple[ClaimStatus, int], ...] | None] = {}
        answer_counter_after: dict[str, Counter[ClaimStatus]] = {}
        claim_status_changes = 0
        for claim_id in claim_state_dirty:
            next_claim_state = self._combined_claim_from_parts(
                direct_after[claim_id], complete_groups_after[claim_id]
            )
            claim_state_values[claim_id] = next_claim_state
            old_claim_state = self._claim_states[claim_id]
            if old_claim_state.status is next_claim_state.status:
                continue
            claim_status_changes += 1
            if self._claim_required[claim_id]:
                answer_id = self._claim_to_answer[claim_id]
                counts = answer_counter_after.setdefault(
                    answer_id, Counter(self._answer_counts[answer_id])
                )
                counts[old_claim_state.status] -= 1
                counts[next_claim_state.status] += 1
                dirty_answers.add(answer_id)

        answer_state_values: dict[str, CombinedAnswerState | None] = {}
        answer_status_changes = 0
        for answer_id in dirty_answers:
            counts = answer_counter_after[answer_id]
            answer_count_values[answer_id] = _counter_image(counts)
            next_answer_state = self._answer_from_counts(answer_id, counts)
            answer_state_values[answer_id] = next_answer_state
            if self._answer_states[answer_id].status is not next_answer_state.status:
                answer_status_changes += 1

        group_artifacts_after = _AfterMapping(
            self._group_artifacts, group_artifact_values
        )
        claim_states_after = _AfterMapping(self._claim_states, claim_state_values)
        claim_certificate_candidates = _OrderedKeys(claim_state_dirty)
        if isinstance(event, PolicyChangeEvent):
            claim_certificate_candidates.update(after.base.all_claim_ids())
        for group_id in certificate_changed_groups:
            if group_id in added_groups:
                claim_id = added_groups[group_id].owner_claim_id
            elif group_id in removed_groups:
                claim_id = removed_groups[group_id].owner_claim_id
            else:
                claim_id = after.group(group_id).owner_claim_id
            claim_certificate_candidates.add(claim_id)

        claim_certificate_dirty = _OrderedKeys[str]()
        selected_group_certificates: dict[
            str, GroupMatchingCertificateArtifact | None
        ] = {}
        for claim_id in claim_certificate_candidates:
            claim_state = claim_states_after[claim_id]
            selected = _selected_group_certificate(
                claim_state,
                group_artifacts_after,
            )
            if _claim_certificate_inputs_changed(
                claim_state,
                decision_policy_version=policy_after.policy_version,
                selected_group_certificate=selected,
                prior_artifact=self._claim_artifacts.get(claim_id),
            ):
                claim_certificate_dirty.add(claim_id)
                selected_group_certificates[claim_id] = selected

        claim_artifact_values: dict[str, ClaimCertificateArtifact | None] = {}
        claim_binding_values: dict[str, WorkingClaimCertificateBinding | None] = {}
        claim_history_values: dict[
            str, _HistoryNode[WorkingClaimCertificateBinding] | None
        ] = {}
        claim_binding_rows: list[WorkingClaimCertificateBinding] = []
        for claim_id in claim_certificate_dirty:
            claim_state = claim_states_after[claim_id]
            prior_claim_artifact = self._claim_artifacts.get(claim_id)
            prior_claim_binding = self._claim_bindings.get(claim_id)
            claim_transition = transition_claim_certificate_for_selected_support(
                claim_state,
                point=after_point,
                decision_policy_version=policy_after.policy_version,
                selected_group_certificate=selected_group_certificates[claim_id],
                prior_binding=prior_claim_binding,
                prior_artifact=prior_claim_artifact,
            )
            claim_artifact_values[claim_id] = claim_transition.artifact
            claim_binding_values[claim_id] = claim_transition.binding
            claim_history = self._claim_binding_history.get(claim_id)
            next_claim_history, claim_rows = _history_after_claim_transition(
                claim_history,
                prior_claim_binding,
                claim_transition.closed_prior_binding,
                claim_transition.binding,
            )
            claim_binding_rows.extend(claim_rows)
            if next_claim_history is not claim_history:
                claim_history_values[claim_id] = next_claim_history

        requirement_state_changes = _point_changes(
            self._requirement_states, requirement_state_values
        )
        group_state_changes = _point_changes(self._group_states, group_state_values)
        claim_state_changes = _point_changes(self._claim_states, claim_state_values)
        answer_state_changes = _point_changes(self._answer_states, answer_state_values)
        group_artifact_changes = _point_changes(
            self._group_artifacts, group_artifact_values
        )
        group_binding_changes = _point_changes(
            self._group_bindings, group_binding_values
        )
        group_history_changes = _point_changes(
            self._group_binding_history, group_history_values
        )
        claim_artifact_changes = _point_changes(
            self._claim_artifacts, claim_artifact_values
        )
        claim_binding_changes = _point_changes(
            self._claim_bindings, claim_binding_values
        )
        claim_history_changes = _point_changes(
            self._claim_binding_history, claim_history_values
        )

        status_deltas: list[StatusDelta] = []
        reason = f"event={event.event_id} op={type(event).__name__}"
        for claim_change in claim_state_changes:
            if (
                claim_change.before is not None
                and claim_change.after is not None
                and claim_change.before.status is not claim_change.after.status
            ):
                status_deltas.append(
                    StatusDelta(
                        event_id=event.event_id,
                        object_type="claim",
                        object_id=claim_change.key,
                        old_status=claim_change.before.status.value,
                        new_status=claim_change.after.status.value,
                        reason=reason,
                    )
                )
        for answer_change in answer_state_changes:
            if (
                answer_change.before is not None
                and answer_change.after is not None
                and answer_change.before.status is not answer_change.after.status
            ):
                status_deltas.append(
                    StatusDelta(
                        event_id=event.event_id,
                        object_type="answer",
                        object_id=answer_change.key,
                        old_status=answer_change.before.status.value,
                        new_status=answer_change.after.status.value,
                        reason=reason,
                    )
                )

        requirement_state_only = tuple(
            change.key
            for change in requirement_state_changes
            if change.before is not None
            and change.after is not None
            and change.before.satisfied == change.after.satisfied
        )
        group_state_only = tuple(
            change.key
            for change in group_state_changes
            if change.before is not None
            and change.after is not None
            and change.before.complete == change.after.complete
        )
        claim_state_only = tuple(
            change.key
            for change in claim_state_changes
            if change.before is not None
            and change.after is not None
            and change.before.status is change.after.status
        )
        group_state_change_keys = _OrderedKeys(
            change.key for change in group_state_changes
        )
        claim_state_change_keys = _OrderedKeys(
            change.key for change in claim_state_changes
        )
        group_certificate_keys = _OrderedKeys(
            change.key for change in group_artifact_changes
        )
        group_certificate_keys.update(change.key for change in group_binding_changes)
        group_certificate_keys.update(change.key for change in group_history_changes)
        claim_certificate_keys = _OrderedKeys(
            change.key for change in claim_artifact_changes
        )
        claim_certificate_keys.update(change.key for change in claim_binding_changes)
        claim_certificate_keys.update(change.key for change in claim_history_changes)
        group_certificate_only = tuple(
            key for key in group_certificate_keys if key not in group_state_change_keys
        )
        claim_certificate_only = tuple(
            key for key in claim_certificate_keys if key not in claim_state_change_keys
        )

        changed_group_keys = _OrderedKeys(group_state_change_keys)
        changed_group_keys.update(group_certificate_keys)
        changed_group_ids = tuple(changed_group_keys)
        changed_claim_keys = _OrderedKeys(claim_state_change_keys)
        changed_claim_keys.update(claim_certificate_keys)
        changed_claim_ids = tuple(changed_claim_keys)
        claims_touched = _OrderedKeys(claim_state_dirty)
        claims_touched.update(claim_certificate_dirty)
        output_records: list[tuple[str, str, object]] = []
        for kind, changes in (
            ("requirement_state", requirement_state_changes),
            ("group_state", group_state_changes),
            ("claim_state", claim_state_changes),
            ("answer_state", answer_state_changes),
            ("group_certificate", group_artifact_changes),
            ("claim_certificate", claim_artifact_changes),
        ):
            output_records.extend(
                (kind, change.key, change.after) for change in changes
            )
        output_records.extend(
            ("group_binding", binding.group_version_id, binding)
            for binding in group_binding_rows
        )
        output_records.extend(
            ("claim_binding", binding.claim_id, binding)
            for binding in claim_binding_rows
        )
        output_records.extend(
            ("status_delta", delta.object_id, delta) for delta in status_deltas
        )
        output_image = _logical_output_image(output_records)
        output_bytes = len(output_image)
        logical_output_digest = hashlib.sha256(output_image).hexdigest()
        matching_work += touched_state_work(
            groups_touched=len(touched_groups),
            claims_touched=len(claims_touched),
            answers_touched=len(dirty_answers),
            claim_status_changes=claim_status_changes,
            answer_status_changes=answer_status_changes,
            output_bytes=output_bytes,
        )
        work = M5OverlayWork(
            matching=matching_work,
            requirement_state_only_changes=len(requirement_state_only),
            group_state_only_changes=len(group_state_only),
            claim_state_only_changes=len(claim_state_only),
            group_certificate_only_changes=len(group_certificate_only),
            claim_certificate_only_changes=len(claim_certificate_only),
            public_status_deltas=len(status_deltas),
        )
        work.assert_nonnegative()
        result = M5OverlayApplyResult(
            event_id=event.event_id,
            point=after_point,
            deltas=tuple(status_deltas),
            changed_requirement_ids=tuple(
                change.key for change in requirement_state_changes
            ),
            changed_group_ids=changed_group_ids,
            changed_claim_ids=changed_claim_ids,
            changed_answer_ids=tuple(change.key for change in answer_state_changes),
            state_only_requirement_ids=requirement_state_only,
            state_only_group_ids=group_state_only,
            state_only_claim_ids=claim_state_only,
            certificate_only_group_ids=group_certificate_only,
            certificate_only_claim_ids=claim_certificate_only,
            published_group_bindings=tuple(group_binding_rows),
            published_claim_bindings=tuple(claim_binding_rows),
            logical_output_digest=logical_output_digest,
            work=work,
            direct_stats=direct_patch.stats,
        )
        return PreparedM5OverlayPatch(
            event_id=event.event_id,
            payload_hash=payload_hash,
            expected_revision=self._state_revision,
            expected_point=self._point,
            point=after_point,
            policy=policy_after,
            direct_patch=direct_patch,
            matching_patches=tuple(matching_patches),
            index_changes=_point_changes(self._group_indexes, index_values),
            hall_changes=_point_changes(self._hall_states, hall_values),
            requirement_local_changes=_point_changes(
                self._requirement_locals, requirement_local_values
            ),
            edge_count_changes=_point_changes(self._edge_counts, edge_count_values),
            requirement_state_changes=requirement_state_changes,
            group_state_changes=group_state_changes,
            complete_group_changes=_point_changes(
                self._complete_groups_by_claim, complete_group_values
            ),
            claim_state_changes=claim_state_changes,
            answer_count_changes=_point_changes(
                {
                    key: _counter_image(self._answer_counts[key])
                    for key in answer_count_values
                },
                answer_count_values,
            ),
            answer_state_changes=answer_state_changes,
            group_artifact_changes=group_artifact_changes,
            group_binding_changes=group_binding_changes,
            group_history_changes=group_history_changes,
            claim_artifact_changes=claim_artifact_changes,
            claim_binding_changes=claim_binding_changes,
            claim_history_changes=claim_history_changes,
            observation_changes=_point_changes(self._observations, observation_values),
            observations_by_requirement_changes=_point_changes(
                self._observations_by_requirement, by_requirement_values
            ),
            observations_by_chunk_changes=_point_changes(
                self._observations_by_chunk, by_chunk_values
            ),
            score_index_before=self._score_index,
            score_index_after=score_after,
            result=result,
            replay_result=None,
            group_binding_rows=tuple(group_binding_rows),
            claim_binding_rows=tuple(claim_binding_rows),
            _owner=self,
        )

    @staticmethod
    def _validate_changes(
        current: Mapping[_K, _T],
        changes: Sequence[_PointChange[_K, _T]],
        *,
        name: str,
    ) -> None:
        for change in changes:
            value = current.get(change.key)
            if value is not change.before and value != change.before:
                raise ValidationError(
                    f"prepared overlay {name} precondition failed for {change.key!r}"
                )

    @staticmethod
    def _apply_changes(
        current: dict[_K, _T],
        changes: Sequence[_PointChange[_K, _T]],
        undo: list[Callable[[], None]],
        checkpoint: Callable[[str], None],
        *,
        name: str,
    ) -> None:
        for change in changes:
            if change.after is None:
                current.pop(change.key, None)
            else:
                current[change.key] = change.after

            if change.before is None:

                def remove_new(
                    target: dict[_K, _T] = current,
                    key: _K = change.key,
                ) -> None:
                    target.pop(key, None)

                undo.append(remove_new)
            else:

                def restore_old(
                    target: dict[_K, _T] = current,
                    key: _K = change.key,
                    value: _T = change.before,
                ) -> None:
                    target[key] = value

                undo.append(restore_old)
            checkpoint(f"overlay:{name}:{change.key!r}")

    def _validate_prepared_overlay_patch(
        self,
        patch: PreparedM5OverlayPatch,
    ) -> None:
        if patch._owner is not self:
            raise ValidationError("prepared overlay patch belongs to another engine")
        if patch._state != "prepared":
            raise ValidationError(f"prepared overlay patch is already {patch._state}")
        if patch.expected_revision != self._state_revision:
            raise ValidationError("prepared overlay patch is stale")
        if patch.expected_point != self._point:
            raise ValidationError("prepared overlay patch point is stale")
        if self._score_index is not patch.score_index_before:
            raise ValidationError("prepared overlay score index is stale")

        self._validate_changes(
            self._group_indexes, patch.index_changes, name="group index"
        )
        self._validate_changes(self._hall_states, patch.hall_changes, name="Hall")
        self._validate_changes(
            self._requirement_locals,
            patch.requirement_local_changes,
            name="requirement local",
        )
        self._validate_changes(
            self._edge_counts, patch.edge_count_changes, name="edge count"
        )
        self._validate_changes(
            self._requirement_states,
            patch.requirement_state_changes,
            name="requirement state",
        )
        self._validate_changes(
            self._group_states, patch.group_state_changes, name="group state"
        )
        self._validate_changes(
            self._complete_groups_by_claim,
            patch.complete_group_changes,
            name="complete group",
        )
        self._validate_changes(
            self._claim_states, patch.claim_state_changes, name="claim state"
        )
        self._validate_changes(
            self._answer_states, patch.answer_state_changes, name="answer state"
        )
        self._validate_changes(
            self._group_artifacts,
            patch.group_artifact_changes,
            name="group artifact",
        )
        self._validate_changes(
            self._group_bindings,
            patch.group_binding_changes,
            name="group binding",
        )
        self._validate_changes(
            self._group_binding_history,
            patch.group_history_changes,
            name="group history",
        )
        self._validate_changes(
            self._claim_artifacts,
            patch.claim_artifact_changes,
            name="claim artifact",
        )
        self._validate_changes(
            self._claim_bindings,
            patch.claim_binding_changes,
            name="claim binding",
        )
        self._validate_changes(
            self._claim_binding_history,
            patch.claim_history_changes,
            name="claim history",
        )
        self._validate_changes(
            self._observations,
            patch.observation_changes,
            name="requirement observation",
        )
        self._validate_changes(
            self._observations_by_requirement,
            patch.observations_by_requirement_changes,
            name="requirement reverse index",
        )
        self._validate_changes(
            self._observations_by_chunk,
            patch.observations_by_chunk_changes,
            name="chunk reverse index",
        )
        for change in patch.answer_count_changes:
            current = _counter_image(self._answer_counts[change.key])
            if current != change.before:
                raise ValidationError(
                    f"prepared overlay answer counts failed for {change.key!r}"
                )

        # These calls perform generation/point checks without mutation.  Do
        # every fallible precondition check before the first matching apply.
        for prepared_group in patch.matching_patches:
            prepared_group.patch.preview_view(
                point=patch.point,
                decision_policy_version=patch.policy.policy_version,
            )
        if patch.direct_patch is not None:
            self.direct_engine.preview_claim_states_after_patch(patch.direct_patch, ())

    def apply_prepared_event(
        self,
        patch: PreparedM5OverlayPatch,
        *,
        failure_injector: OverlayFailureInjector | None = None,
    ) -> M5OverlayApplyResult:
        """Apply with the direct patch as the final publication checkpoint."""

        self._validate_prepared_overlay_patch(patch)
        if patch.replay_result is not None:
            patch._state = "applied"
            return patch.replay_result

        undo: list[Callable[[], None]] = []
        matching_tokens: list[
            tuple[MaintainedCertificateIndex, MaintainedIndexRollbackToken]
        ] = []

        def checkpoint(name: str) -> None:
            if failure_injector is not None:
                failure_injector(name)

        try:
            for prepared_group in patch.matching_patches:
                token = prepared_group.index.apply_prepared_observation_deltas(
                    prepared_group.patch
                )
                matching_tokens.append((prepared_group.index, token))
                checkpoint(f"matching:{prepared_group.group_version_id}")

            self._apply_changes(
                self._group_indexes,
                patch.index_changes,
                undo,
                checkpoint,
                name="group_index",
            )
            self._apply_changes(
                self._hall_states,
                patch.hall_changes,
                undo,
                checkpoint,
                name="hall",
            )
            self._apply_changes(
                self._requirement_locals,
                patch.requirement_local_changes,
                undo,
                checkpoint,
                name="requirement_local",
            )
            self._apply_changes(
                self._edge_counts,
                patch.edge_count_changes,
                undo,
                checkpoint,
                name="edge_count",
            )
            self._apply_changes(
                self._requirement_states,
                patch.requirement_state_changes,
                undo,
                checkpoint,
                name="requirement_state",
            )
            self._apply_changes(
                self._group_states,
                patch.group_state_changes,
                undo,
                checkpoint,
                name="group_state",
            )
            self._apply_changes(
                self._complete_groups_by_claim,
                patch.complete_group_changes,
                undo,
                checkpoint,
                name="complete_group",
            )
            self._apply_changes(
                self._claim_states,
                patch.claim_state_changes,
                undo,
                checkpoint,
                name="claim_state",
            )
            for change in patch.answer_count_changes:
                before_image = change.before
                after_image = change.after
                assert before_image is not None and after_image is not None
                self._answer_counts[change.key] = Counter(dict(after_image))

                def restore_answer_count(
                    key: str = change.key,
                    value: tuple[tuple[ClaimStatus, int], ...] = before_image,
                ) -> None:
                    self._answer_counts[key] = Counter(dict(value))

                undo.append(restore_answer_count)
                checkpoint(f"overlay:answer_count:{change.key!r}")
            self._apply_changes(
                self._answer_states,
                patch.answer_state_changes,
                undo,
                checkpoint,
                name="answer_state",
            )
            self._apply_changes(
                self._group_artifacts,
                patch.group_artifact_changes,
                undo,
                checkpoint,
                name="group_artifact",
            )
            self._apply_changes(
                self._group_bindings,
                patch.group_binding_changes,
                undo,
                checkpoint,
                name="group_binding",
            )
            self._apply_changes(
                self._group_binding_history,
                patch.group_history_changes,
                undo,
                checkpoint,
                name="group_history",
            )
            self._apply_changes(
                self._claim_artifacts,
                patch.claim_artifact_changes,
                undo,
                checkpoint,
                name="claim_artifact",
            )
            self._apply_changes(
                self._claim_bindings,
                patch.claim_binding_changes,
                undo,
                checkpoint,
                name="claim_binding",
            )
            self._apply_changes(
                self._claim_binding_history,
                patch.claim_history_changes,
                undo,
                checkpoint,
                name="claim_history",
            )
            self._apply_changes(
                self._observations,
                patch.observation_changes,
                undo,
                checkpoint,
                name="requirement_observation",
            )
            self._apply_changes(
                self._observations_by_requirement,
                patch.observations_by_requirement_changes,
                undo,
                checkpoint,
                name="requirement_reverse",
            )
            self._apply_changes(
                self._observations_by_chunk,
                patch.observations_by_chunk_changes,
                undo,
                checkpoint,
                name="chunk_reverse",
            )

            previous_score_index = self._score_index
            self._score_index = patch.score_index_after

            def restore_score_index(
                value: _RequirementScoreIndex = previous_score_index,
            ) -> None:
                self._score_index = value

            undo.append(restore_score_index)
            checkpoint("overlay:score_index")

            previous_policy = self._policy
            self._policy = patch.policy

            def restore_policy(value: DecisionPolicy = previous_policy) -> None:
                self._policy = value

            undo.append(restore_policy)
            previous_point = self._point
            self._point = patch.point

            def restore_point(value: SnapshotPoint = previous_point) -> None:
                self._point = value

            undo.append(restore_point)
            previous_work = self.last_work
            self.last_work = patch.result.work

            def restore_work(value: M5OverlayWork = previous_work) -> None:
                self.last_work = value

            undo.append(restore_work)
            self._state_revision += 1

            def restore_revision(value: int = patch.expected_revision) -> None:
                self._state_revision = value

            undo.append(restore_revision)
            self._processed_events[patch.event_id] = (
                patch.payload_hash,
                patch.result,
            )

            def remove_event(key: str = patch.event_id) -> None:
                self._processed_events.pop(key)

            undo.append(remove_event)
            patch._state = "applied"
            checkpoint("before_direct_publish")

            direct_patch = patch.direct_patch
            assert direct_patch is not None
            self.direct_engine.apply_state_patch(
                direct_patch,
                failure_injector=(
                    None
                    if failure_injector is None
                    else lambda name: failure_injector(f"direct:{name}")
                ),
            )
            # Publication succeeded.  The immutable result was built during
            # prepare; there are deliberately no mutations or callbacks here.
            return patch.result
        except Exception:
            for rollback in reversed(undo):
                rollback()
            for index, token in reversed(matching_tokens):
                index.rollback_prepared_observation_deltas(token)
            patch._state = "failed"
            raise

    def apply_committed_event(
        self,
        event: CommittedOverlayEvent,
        before: M5Repository,
        after: M5Repository,
        *,
        failure_injector: OverlayFailureInjector | None = None,
    ) -> M5OverlayApplyResult:
        """Prepare and apply one already-committed repository event."""

        patch = self.prepare_committed_event_patch(event, before, after)
        return self.apply_prepared_event(patch, failure_injector=failure_injector)


__all__ = [
    "CommittedOverlayEvent",
    "M5IncrementalOverlay",
    "M5OverlayApplyResult",
    "M5OverlayWork",
    "OverlayFailureInjector",
    "PreparedM5OverlayPatch",
]
