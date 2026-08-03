"""Bounded exact matching and certificate maintenance for GroundLoop M5.

This module deliberately depends only on primitive identifiers, integer masks,
and the stable :class:`~groundloop.errors.ValidationError`.  It does not import
the independent Python full-state oracle or any PostgreSQL implementation.

The optimized kernel exploits the frozen M5 bound of at most eight left-side
requirements.  It maintains Hall-neighbour counts for all requirement subsets;
the deterministic augmenting-path implementation remains a separate
affected-group baseline and certificate constructor.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Protocol, TypeVar

from groundloop.errors import ValidationError
from groundloop.m5.domain import (
    EvidenceGroupVersion,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    RequirementWitness,
    SnapshotPoint,
)

MAX_REQUIREMENTS = 8
GROUP_CERTIFICATE_VERSION = "m5-group-certificate-v1"
_HEX = frozenset("0123456789abcdef")
_K = TypeVar("_K")
_V = TypeVar("_V")

# Backward-compatible Lane A name; the concrete record is coordinator-owned.
GroupMatchingCertificateRow = GroupCertificateRow


def _require_integer(
    name: str,
    value: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{name} must be at most {maximum}")


def _require_requirement_count(requirement_count: int) -> None:
    _require_integer(
        "requirement_count",
        requirement_count,
        minimum=1,
        maximum=MAX_REQUIREMENTS,
    )


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")


def _require_sha256(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_mask(name: str, mask: int, requirement_count: int) -> None:
    _require_integer(name, mask, minimum=0, maximum=(1 << requirement_count) - 1)


@dataclass(frozen=True, slots=True)
class MatchingWorkCounters:
    """Composable signed counters for the M5 matching overlay.

    Public operations in this module return non-negative counters.  Addition,
    subtraction, and negation intentionally permit signed values so callers can
    compute interval or before/after deltas without inventing a second record.
    ``assert_nonnegative`` is the release-boundary guard.
    """

    contribution_additions: int = 0
    contribution_removals: int = 0
    requirement_observation_changes_processed: int = 0
    policy_candidate_observations: int = 0
    ordered_policy_range_probes: int = 0
    ordered_index_operations: int = 0
    canonical_sort_items: int = 0
    edge_refcount_keys_updated: int = 0
    distinct_edge_crossings: int = 0
    hash_mask_transitions: int = 0
    hash_masks_initialized: int = 0
    hall_zeta_additions: int = 0
    hall_subset_entries_examined: int = 0
    hall_neighbor_entries_changed: int = 0
    hall_deficiency_entries_examined: int = 0
    certificate_repairs: int = 0
    certificate_reconstructions: int = 0
    policy_rebindings: int = 0
    representative_hashes_read: int = 0
    representative_observations_read: int = 0
    augmenting_searches: int = 0
    augmenting_requirement_visits: int = 0
    augmenting_edge_visits: int = 0
    certificate_digest_input_bytes: int = 0
    group_local_state_operations: int = 0
    groups_touched: int = 0
    claims_touched: int = 0
    answers_touched: int = 0
    claim_status_changes: int = 0
    answer_status_changes: int = 0
    output_bytes: int = 0

    def __post_init__(self) -> None:
        for name, value in self._named_values():
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValidationError(f"work counter {name} must be an integer")

    def _named_values(self) -> tuple[tuple[str, int], ...]:
        return (
            ("contribution_additions", self.contribution_additions),
            ("contribution_removals", self.contribution_removals),
            (
                "requirement_observation_changes_processed",
                self.requirement_observation_changes_processed,
            ),
            ("policy_candidate_observations", self.policy_candidate_observations),
            ("ordered_policy_range_probes", self.ordered_policy_range_probes),
            ("ordered_index_operations", self.ordered_index_operations),
            ("canonical_sort_items", self.canonical_sort_items),
            ("edge_refcount_keys_updated", self.edge_refcount_keys_updated),
            ("distinct_edge_crossings", self.distinct_edge_crossings),
            ("hash_mask_transitions", self.hash_mask_transitions),
            ("hash_masks_initialized", self.hash_masks_initialized),
            ("hall_zeta_additions", self.hall_zeta_additions),
            ("hall_subset_entries_examined", self.hall_subset_entries_examined),
            ("hall_neighbor_entries_changed", self.hall_neighbor_entries_changed),
            (
                "hall_deficiency_entries_examined",
                self.hall_deficiency_entries_examined,
            ),
            ("certificate_repairs", self.certificate_repairs),
            ("certificate_reconstructions", self.certificate_reconstructions),
            ("policy_rebindings", self.policy_rebindings),
            ("representative_hashes_read", self.representative_hashes_read),
            (
                "representative_observations_read",
                self.representative_observations_read,
            ),
            ("augmenting_searches", self.augmenting_searches),
            (
                "augmenting_requirement_visits",
                self.augmenting_requirement_visits,
            ),
            ("augmenting_edge_visits", self.augmenting_edge_visits),
            (
                "certificate_digest_input_bytes",
                self.certificate_digest_input_bytes,
            ),
            ("group_local_state_operations", self.group_local_state_operations),
            ("groups_touched", self.groups_touched),
            ("claims_touched", self.claims_touched),
            ("answers_touched", self.answers_touched),
            ("claim_status_changes", self.claim_status_changes),
            ("answer_status_changes", self.answer_status_changes),
            ("output_bytes", self.output_bytes),
        )

    def __add__(self, other: MatchingWorkCounters) -> MatchingWorkCounters:
        if not isinstance(other, MatchingWorkCounters):
            return NotImplemented
        return MatchingWorkCounters(
            contribution_additions=(
                self.contribution_additions + other.contribution_additions
            ),
            contribution_removals=(
                self.contribution_removals + other.contribution_removals
            ),
            requirement_observation_changes_processed=(
                self.requirement_observation_changes_processed
                + other.requirement_observation_changes_processed
            ),
            policy_candidate_observations=(
                self.policy_candidate_observations + other.policy_candidate_observations
            ),
            ordered_policy_range_probes=(
                self.ordered_policy_range_probes + other.ordered_policy_range_probes
            ),
            ordered_index_operations=(
                self.ordered_index_operations + other.ordered_index_operations
            ),
            canonical_sort_items=(
                self.canonical_sort_items + other.canonical_sort_items
            ),
            edge_refcount_keys_updated=(
                self.edge_refcount_keys_updated + other.edge_refcount_keys_updated
            ),
            distinct_edge_crossings=(
                self.distinct_edge_crossings + other.distinct_edge_crossings
            ),
            hash_mask_transitions=(
                self.hash_mask_transitions + other.hash_mask_transitions
            ),
            hash_masks_initialized=(
                self.hash_masks_initialized + other.hash_masks_initialized
            ),
            hall_zeta_additions=self.hall_zeta_additions + other.hall_zeta_additions,
            hall_subset_entries_examined=(
                self.hall_subset_entries_examined + other.hall_subset_entries_examined
            ),
            hall_neighbor_entries_changed=(
                self.hall_neighbor_entries_changed + other.hall_neighbor_entries_changed
            ),
            hall_deficiency_entries_examined=(
                self.hall_deficiency_entries_examined
                + other.hall_deficiency_entries_examined
            ),
            certificate_repairs=(self.certificate_repairs + other.certificate_repairs),
            certificate_reconstructions=(
                self.certificate_reconstructions + other.certificate_reconstructions
            ),
            policy_rebindings=self.policy_rebindings + other.policy_rebindings,
            representative_hashes_read=(
                self.representative_hashes_read + other.representative_hashes_read
            ),
            representative_observations_read=(
                self.representative_observations_read
                + other.representative_observations_read
            ),
            augmenting_searches=(self.augmenting_searches + other.augmenting_searches),
            augmenting_requirement_visits=(
                self.augmenting_requirement_visits + other.augmenting_requirement_visits
            ),
            augmenting_edge_visits=(
                self.augmenting_edge_visits + other.augmenting_edge_visits
            ),
            certificate_digest_input_bytes=(
                self.certificate_digest_input_bytes
                + other.certificate_digest_input_bytes
            ),
            group_local_state_operations=(
                self.group_local_state_operations + other.group_local_state_operations
            ),
            groups_touched=self.groups_touched + other.groups_touched,
            claims_touched=self.claims_touched + other.claims_touched,
            answers_touched=self.answers_touched + other.answers_touched,
            claim_status_changes=(
                self.claim_status_changes + other.claim_status_changes
            ),
            answer_status_changes=(
                self.answer_status_changes + other.answer_status_changes
            ),
            output_bytes=self.output_bytes + other.output_bytes,
        )

    def __neg__(self) -> MatchingWorkCounters:
        return MatchingWorkCounters() - self

    def __sub__(self, other: MatchingWorkCounters) -> MatchingWorkCounters:
        if not isinstance(other, MatchingWorkCounters):
            return NotImplemented
        return MatchingWorkCounters(
            contribution_additions=(
                self.contribution_additions - other.contribution_additions
            ),
            contribution_removals=(
                self.contribution_removals - other.contribution_removals
            ),
            requirement_observation_changes_processed=(
                self.requirement_observation_changes_processed
                - other.requirement_observation_changes_processed
            ),
            policy_candidate_observations=(
                self.policy_candidate_observations - other.policy_candidate_observations
            ),
            ordered_policy_range_probes=(
                self.ordered_policy_range_probes - other.ordered_policy_range_probes
            ),
            ordered_index_operations=(
                self.ordered_index_operations - other.ordered_index_operations
            ),
            canonical_sort_items=(
                self.canonical_sort_items - other.canonical_sort_items
            ),
            edge_refcount_keys_updated=(
                self.edge_refcount_keys_updated - other.edge_refcount_keys_updated
            ),
            distinct_edge_crossings=(
                self.distinct_edge_crossings - other.distinct_edge_crossings
            ),
            hash_mask_transitions=(
                self.hash_mask_transitions - other.hash_mask_transitions
            ),
            hash_masks_initialized=(
                self.hash_masks_initialized - other.hash_masks_initialized
            ),
            hall_zeta_additions=self.hall_zeta_additions - other.hall_zeta_additions,
            hall_subset_entries_examined=(
                self.hall_subset_entries_examined - other.hall_subset_entries_examined
            ),
            hall_neighbor_entries_changed=(
                self.hall_neighbor_entries_changed - other.hall_neighbor_entries_changed
            ),
            hall_deficiency_entries_examined=(
                self.hall_deficiency_entries_examined
                - other.hall_deficiency_entries_examined
            ),
            certificate_repairs=(self.certificate_repairs - other.certificate_repairs),
            certificate_reconstructions=(
                self.certificate_reconstructions - other.certificate_reconstructions
            ),
            policy_rebindings=self.policy_rebindings - other.policy_rebindings,
            representative_hashes_read=(
                self.representative_hashes_read - other.representative_hashes_read
            ),
            representative_observations_read=(
                self.representative_observations_read
                - other.representative_observations_read
            ),
            augmenting_searches=(self.augmenting_searches - other.augmenting_searches),
            augmenting_requirement_visits=(
                self.augmenting_requirement_visits - other.augmenting_requirement_visits
            ),
            augmenting_edge_visits=(
                self.augmenting_edge_visits - other.augmenting_edge_visits
            ),
            certificate_digest_input_bytes=(
                self.certificate_digest_input_bytes
                - other.certificate_digest_input_bytes
            ),
            group_local_state_operations=(
                self.group_local_state_operations - other.group_local_state_operations
            ),
            groups_touched=self.groups_touched - other.groups_touched,
            claims_touched=self.claims_touched - other.claims_touched,
            answers_touched=self.answers_touched - other.answers_touched,
            claim_status_changes=(
                self.claim_status_changes - other.claim_status_changes
            ),
            answer_status_changes=(
                self.answer_status_changes - other.answer_status_changes
            ),
            output_bytes=self.output_bytes - other.output_bytes,
        )

    def assert_nonnegative(self) -> None:
        negative = tuple(name for name, value in self._named_values() if value < 0)
        if negative:
            joined = ", ".join(negative)
            raise ValidationError(f"work counters must be nonnegative: {joined}")


def policy_range_probe_work(
    *,
    changed_threshold_dimensions: int,
    candidate_observations: int,
) -> MatchingWorkCounters:
    """Record frozen T2 policy-selection work, including an empty range.

    This is intentionally transaction-level rather than certificate-level: a
    caller adds it once to a policy-change microtransaction and then composes
    any number of group/claim rebind transitions without double charging ``P``.
    """

    _require_integer(
        "changed_threshold_dimensions",
        changed_threshold_dimensions,
        minimum=0,
        maximum=2,
    )
    _require_integer("candidate_observations", candidate_observations, minimum=0)
    return MatchingWorkCounters(
        policy_candidate_observations=candidate_observations,
        ordered_policy_range_probes=changed_threshold_dimensions,
        ordered_index_operations=changed_threshold_dimensions,
    )


def requirement_observation_work(
    *,
    changes_processed: int,
    ordered_index_operations: int = 0,
) -> MatchingWorkCounters:
    """Record the frozen T2 ``U`` term without inferring it from edge deltas.

    A decision flip can be semantically processed while producing no SUPPORT
    edge (for example REFUTE to NEUTRAL), so the exact ``U`` count belongs to
    the observation/currency caller and is not guessed by the mask kernel.
    """

    _require_integer("changes_processed", changes_processed, minimum=0)
    _require_integer(
        "ordered_index_operations",
        ordered_index_operations,
        minimum=0,
    )
    return MatchingWorkCounters(
        requirement_observation_changes_processed=changes_processed,
        ordered_index_operations=ordered_index_operations,
    )


def touched_state_work(
    *,
    groups_touched: int = 0,
    claims_touched: int = 0,
    answers_touched: int = 0,
    claim_status_changes: int = 0,
    answer_status_changes: int = 0,
    output_bytes: int = 0,
) -> MatchingWorkCounters:
    """Record deduplicated transaction-level state/output terms for M5-T2.

    Primitive group operations cannot know whether another operation in the
    same microtransaction touched the same group.  The coordinator therefore
    supplies these cardinalities once after deduplicating concrete IDs.
    """

    values = (
        ("groups_touched", groups_touched),
        ("claims_touched", claims_touched),
        ("answers_touched", answers_touched),
        ("claim_status_changes", claim_status_changes),
        ("answer_status_changes", answer_status_changes),
        ("output_bytes", output_bytes),
    )
    for name, value in values:
        _require_integer(name, value, minimum=0)
    return MatchingWorkCounters(
        groups_touched=groups_touched,
        claims_touched=claims_touched,
        answers_touched=answers_touched,
        claim_status_changes=claim_status_changes,
        answer_status_changes=answer_status_changes,
        output_bytes=output_bytes,
    )


@dataclass(frozen=True, slots=True, order=True)
class MatchingPair:
    requirement_ordinal: int
    text_hash: str


@dataclass(frozen=True, slots=True)
class AffectedMatchingResult:
    requirement_count: int
    pairs: tuple[MatchingPair, ...]
    work: MatchingWorkCounters

    @property
    def matching_size(self) -> int:
        return len(self.pairs)

    @property
    def complete(self) -> bool:
        return self.matching_size == self.requirement_count


def _hash_mask_items(
    hash_masks: Mapping[str, int] | Iterable[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    return (
        tuple(hash_masks.items())
        if isinstance(hash_masks, Mapping)
        else tuple(hash_masks)
    )


def _validated_hash_masks_in_input_order(
    requirement_count: int,
    hash_masks: Mapping[str, int] | Iterable[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    _require_requirement_count(requirement_count)
    items = _hash_mask_items(hash_masks)
    seen_hashes: set[str] = set()
    validated: list[tuple[str, int]] = []
    full_mask = (1 << requirement_count) - 1
    for text_hash, mask in items:
        _require_text("text_hash", text_hash)
        _require_integer("hash mask", mask, minimum=1, maximum=full_mask)
        if text_hash in seen_hashes:
            raise ValidationError(f"duplicate text hash: {text_hash}")
        seen_hashes.add(text_hash)
        validated.append((text_hash, mask))
    return tuple(validated)


def _canonical_hash_masks(
    requirement_count: int,
    hash_masks: Mapping[str, int] | Iterable[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    return tuple(
        sorted(_validated_hash_masks_in_input_order(requirement_count, hash_masks))
    )


def _affected_group_matching_from_canonical(
    requirement_count: int,
    canonical: Sequence[tuple[str, int]],
) -> AffectedMatchingResult:
    """Run the O(rE) kernel after canonical ordering has been established."""

    candidates: list[list[str]] = [[] for _ in range(requirement_count)]
    for text_hash, mask in canonical:
        for ordinal in range(requirement_count):
            if mask & (1 << ordinal):
                candidates[ordinal].append(text_hash)

    hash_owner: dict[str, int] = {}
    requirement_hash: dict[int, str] = {}
    requirement_visits = 0
    edge_visits = 0

    def augment(requirement_ordinal: int, seen_hashes: set[str]) -> bool:
        nonlocal requirement_visits, edge_visits
        requirement_visits += 1
        for text_hash in candidates[requirement_ordinal]:
            edge_visits += 1
            if text_hash in seen_hashes:
                continue
            seen_hashes.add(text_hash)
            owner = hash_owner.get(text_hash)
            if owner is None or augment(owner, seen_hashes):
                hash_owner[text_hash] = requirement_ordinal
                requirement_hash[requirement_ordinal] = text_hash
                return True
        return False

    for ordinal in range(requirement_count):
        augment(ordinal, set())

    pairs = tuple(
        MatchingPair(ordinal, requirement_hash[ordinal])
        for ordinal in range(requirement_count)
        if ordinal in requirement_hash
    )
    work = MatchingWorkCounters(
        augmenting_searches=requirement_count,
        augmenting_requirement_visits=requirement_visits,
        augmenting_edge_visits=edge_visits,
    )
    return AffectedMatchingResult(requirement_count, pairs, work)


def affected_group_matching_canonical(
    requirement_count: int,
    hash_masks: Sequence[tuple[str, int]],
) -> AffectedMatchingResult:
    """Run deterministic affected-group matching over preordered hash masks.

    ``hash_masks`` must be strictly increasing by text hash.  Validation and
    matching are linear in the supplied graph image, so the kernel has the
    frozen ``O(rE)`` cost under the ordered-index input contract.
    """

    validated = _validated_hash_masks_in_input_order(requirement_count, hash_masks)
    previous_hash: str | None = None
    for text_hash, _ in validated:
        if previous_hash is not None and text_hash <= previous_hash:
            raise ValidationError("canonical hash masks must be ordered by text hash")
        previous_hash = text_hash
    return _affected_group_matching_from_canonical(requirement_count, validated)


def affected_group_matching(
    requirement_count: int,
    hash_masks: Mapping[str, int] | Iterable[tuple[str, int]],
) -> AffectedMatchingResult:
    """Sort arbitrary input, then run the deterministic matching baseline.

    This convenience/build wrapper costs ``O(H log H + rE)``.  Measured callers
    that already maintain ordered hashes use :func:`affected_group_matching_canonical`
    and avoid the sort.
    """

    validated = _validated_hash_masks_in_input_order(requirement_count, hash_masks)
    canonical = tuple(sorted(validated))
    result = _affected_group_matching_from_canonical(requirement_count, canonical)
    return AffectedMatchingResult(
        result.requirement_count,
        result.pairs,
        result.work + MatchingWorkCounters(canonical_sort_items=len(validated)),
    )


@dataclass(frozen=True, slots=True)
class HallMaskState:
    """Immutable exact Hall state for one bounded group.

    Index zero is retained in every array for direct bit-mask indexing.
    ``mask_histogram[0]`` and ``neighbor_counts[0]`` are always zero.
    """

    requirement_count: int
    mask_histogram: tuple[int, ...]
    neighbor_counts: tuple[int, ...]
    deficiencies: tuple[int, ...]
    maximum_deficiency: int
    matching_size: int
    distinct_hash_count: int

    def __post_init__(self) -> None:
        _require_requirement_count(self.requirement_count)
        size = 1 << self.requirement_count
        for name, values in (
            ("mask_histogram", self.mask_histogram),
            ("neighbor_counts", self.neighbor_counts),
            ("deficiencies", self.deficiencies),
        ):
            if len(values) != size:
                raise ValidationError(f"{name} must contain exactly {size} entries")
            for value in values:
                _require_integer(f"{name} entry", value)
        if self.mask_histogram[0] != 0:
            raise ValidationError("mask_histogram[0] must be zero")
        if self.neighbor_counts[0] != 0 or self.deficiencies[0] != 0:
            raise ValidationError("empty-subset Hall entries must be zero")
        if any(value < 0 for value in self.mask_histogram):
            raise ValidationError("mask histogram cannot contain negative counts")
        if any(value < 0 for value in self.neighbor_counts):
            raise ValidationError("Hall neighbour counts cannot be negative")
        for subset in range(1, size):
            expected = subset.bit_count() - self.neighbor_counts[subset]
            if self.deficiencies[subset] != expected:
                raise ValidationError("deficiency array is inconsistent")
        expected_maximum = max(0, max(self.deficiencies[1:]))
        if self.maximum_deficiency != expected_maximum:
            raise ValidationError("maximum deficiency is inconsistent")
        if self.matching_size != self.requirement_count - expected_maximum:
            raise ValidationError("matching size is inconsistent with deficiency")
        if self.distinct_hash_count != sum(self.mask_histogram):
            raise ValidationError("distinct hash count is inconsistent")

    @property
    def complete(self) -> bool:
        return self.maximum_deficiency == 0


@dataclass(frozen=True, slots=True)
class HallKernelResult:
    state: HallMaskState
    work: MatchingWorkCounters


@dataclass(frozen=True, slots=True, order=True)
class HashMaskTransition:
    text_hash: str
    old_mask: int
    new_mask: int

    def __post_init__(self) -> None:
        _require_text("text_hash", self.text_hash)
        _require_integer("old_mask", self.old_mask, minimum=0)
        _require_integer("new_mask", self.new_mask, minimum=0)


def _histogram_from_masks(
    requirement_count: int,
    masks: Iterable[int],
) -> tuple[int, ...]:
    size = 1 << requirement_count
    histogram = [0] * size
    for mask in masks:
        _require_integer("hash mask", mask, minimum=1, maximum=size - 1)
        histogram[mask] += 1
    return tuple(histogram)


def _state_from_histogram_via_zeta(
    requirement_count: int,
    histogram: tuple[int, ...],
) -> tuple[HallMaskState, int]:
    size = 1 << requirement_count
    subset_sums = list(histogram)
    zeta_additions = 0
    for bit in range(requirement_count):
        bit_value = 1 << bit
        for mask in range(size):
            if mask & bit_value:
                subset_sums[mask] += subset_sums[mask ^ bit_value]
                zeta_additions += 1

    distinct_hash_count = sum(histogram)
    full_mask = size - 1
    neighbor_counts = [0] * size
    deficiencies = [0] * size
    for subset in range(1, size):
        neighbor_counts[subset] = distinct_hash_count - subset_sums[full_mask ^ subset]
        deficiencies[subset] = subset.bit_count() - neighbor_counts[subset]
    maximum_deficiency = max(0, max(deficiencies[1:]))
    state = HallMaskState(
        requirement_count=requirement_count,
        mask_histogram=histogram,
        neighbor_counts=tuple(neighbor_counts),
        deficiencies=tuple(deficiencies),
        maximum_deficiency=maximum_deficiency,
        matching_size=requirement_count - maximum_deficiency,
        distinct_hash_count=distinct_hash_count,
    )
    return state, zeta_additions


def initialize_hall_mask_state(
    requirement_count: int,
    hash_masks: Mapping[str, int] | Iterable[int],
) -> HallKernelResult:
    """Initialize exact ``C``, ``N``, and deficiency arrays via subset zeta."""

    _require_requirement_count(requirement_count)
    if isinstance(hash_masks, Mapping):
        validated = _validated_hash_masks_in_input_order(
            requirement_count,
            hash_masks,
        )
        masks = tuple(mask for _, mask in validated)
    else:
        masks = tuple(hash_masks)
    histogram = _histogram_from_masks(requirement_count, masks)
    state, zeta_additions = _state_from_histogram_via_zeta(
        requirement_count,
        histogram,
    )
    subset_count = (1 << requirement_count) - 1
    return HallKernelResult(
        state=state,
        work=MatchingWorkCounters(
            hash_masks_initialized=len(masks),
            hall_zeta_additions=zeta_additions,
            hall_subset_entries_examined=subset_count,
            hall_deficiency_entries_examined=subset_count,
        ),
    )


def validate_hall_mask_state(state: HallMaskState) -> tuple[str, ...]:
    """Independently validate histogram-to-neighbour equality for audit mode."""

    issues: list[str] = []
    expected, _ = _state_from_histogram_via_zeta(
        state.requirement_count,
        state.mask_histogram,
    )
    if state.neighbor_counts != expected.neighbor_counts:
        issues.append("neighbor_counts_do_not_match_histogram")
    if state.deficiencies != expected.deficiencies:
        issues.append("deficiencies_do_not_match_histogram")
    if state.maximum_deficiency != expected.maximum_deficiency:
        issues.append("maximum_deficiency_does_not_match_histogram")
    if state.matching_size != expected.matching_size:
        issues.append("matching_size_does_not_match_histogram")
    if state.distinct_hash_count != expected.distinct_hash_count:
        issues.append("distinct_hash_count_does_not_match_histogram")
    return tuple(issues)


def apply_hash_mask_transition(
    state: HallMaskState,
    *,
    old_mask: int,
    new_mask: int,
) -> HallKernelResult:
    """Apply one coalesced old/new hash-mask transition in ``O(2^r)``.

    A net-equal transition returns the identical state object and zero Hall
    work.  Group-key touch accounting is intentionally performed by the batch
    wrapper so multiple hash transitions do not overcount one affected group.
    """

    requirement_count = state.requirement_count
    full_mask = (1 << requirement_count) - 1
    _require_integer("old_mask", old_mask, minimum=0, maximum=full_mask)
    _require_integer("new_mask", new_mask, minimum=0, maximum=full_mask)
    if old_mask == new_mask:
        return HallKernelResult(state, MatchingWorkCounters())
    if old_mask != 0 and state.mask_histogram[old_mask] <= 0:
        raise ValidationError("cannot remove a hash from an empty mask bucket")

    histogram = list(state.mask_histogram)
    if old_mask != 0:
        histogram[old_mask] -= 1
    if new_mask != 0:
        histogram[new_mask] += 1

    neighbor_counts = list(state.neighbor_counts)
    deficiencies = list(state.deficiencies)
    changed_neighbors = 0
    for subset in range(1, full_mask + 1):
        old_adjacent = old_mask != 0 and bool(subset & old_mask)
        new_adjacent = new_mask != 0 and bool(subset & new_mask)
        if old_adjacent == new_adjacent:
            continue
        neighbor_counts[subset] += 1 if new_adjacent else -1
        deficiencies[subset] = subset.bit_count() - neighbor_counts[subset]
        changed_neighbors += 1

    maximum_deficiency = max(0, max(deficiencies[1:]))
    new_state = HallMaskState(
        requirement_count=requirement_count,
        mask_histogram=tuple(histogram),
        neighbor_counts=tuple(neighbor_counts),
        deficiencies=tuple(deficiencies),
        maximum_deficiency=maximum_deficiency,
        matching_size=requirement_count - maximum_deficiency,
        distinct_hash_count=sum(histogram),
    )
    work = MatchingWorkCounters(
        distinct_edge_crossings=(old_mask ^ new_mask).bit_count(),
        hash_mask_transitions=1,
        hall_subset_entries_examined=full_mask,
        hall_neighbor_entries_changed=changed_neighbors,
        hall_deficiency_entries_examined=full_mask,
    )
    return HallKernelResult(new_state, work)


def apply_hash_mask_transitions(
    state: HallMaskState,
    transitions: Iterable[HashMaskTransition],
) -> HallKernelResult:
    """Failure-atomically apply at most one net transition per text hash."""

    ordered = tuple(transitions)
    seen: set[str] = set()
    current = state
    work = MatchingWorkCounters()
    changed = False
    full_mask = (1 << state.requirement_count) - 1
    for transition in ordered:
        if transition.text_hash in seen:
            raise ValidationError(
                f"duplicate coalesced transition for {transition.text_hash}"
            )
        seen.add(transition.text_hash)
        _require_integer(
            "old_mask",
            transition.old_mask,
            minimum=0,
            maximum=full_mask,
        )
        _require_integer(
            "new_mask",
            transition.new_mask,
            minimum=0,
            maximum=full_mask,
        )
        result = apply_hash_mask_transition(
            current,
            old_mask=transition.old_mask,
            new_mask=transition.new_mask,
        )
        current = result.state
        work += result.work
        changed = changed or transition.old_mask != transition.new_mask
    if changed:
        work += MatchingWorkCounters(group_local_state_operations=1)
    work.assert_nonnegative()
    return HallKernelResult(current, work)


@dataclass(frozen=True, slots=True, order=True)
class EdgeRefcount:
    requirement_ordinal: int
    text_hash: str
    count: int

    def __post_init__(self) -> None:
        _require_integer("requirement_ordinal", self.requirement_ordinal, minimum=0)
        _require_text("text_hash", self.text_hash)
        _require_integer("edge refcount", self.count, minimum=1)


@dataclass(frozen=True, slots=True, order=True)
class EdgeMultiplicityDelta:
    requirement_ordinal: int
    text_hash: str
    delta: int

    def __post_init__(self) -> None:
        _require_integer("requirement_ordinal", self.requirement_ordinal, minimum=0)
        _require_text("text_hash", self.text_hash)
        _require_integer("edge multiplicity delta", self.delta)
        if self.delta == 0:
            raise ValidationError("edge multiplicity delta must be nonzero")


@dataclass(frozen=True, slots=True)
class EdgeCoalescingResult:
    refcounts: tuple[EdgeRefcount, ...]
    transitions: tuple[HashMaskTransition, ...]
    work: MatchingWorkCounters


@dataclass(frozen=True, slots=True)
class EdgeDeltaApplication:
    state: HallMaskState
    refcounts: tuple[EdgeRefcount, ...]
    transitions: tuple[HashMaskTransition, ...]
    work: MatchingWorkCounters


def _canonical_refcount_map(
    requirement_count: int,
    refcounts: Mapping[tuple[int, str], int] | Iterable[EdgeRefcount],
) -> dict[tuple[int, str], int]:
    _require_requirement_count(requirement_count)
    if isinstance(refcounts, Mapping):
        items = tuple(
            EdgeRefcount(ordinal, text_hash, count)
            for (ordinal, text_hash), count in refcounts.items()
        )
    else:
        items = tuple(refcounts)
    result: dict[tuple[int, str], int] = {}
    for item in items:
        _require_integer(
            "requirement_ordinal",
            item.requirement_ordinal,
            minimum=0,
            maximum=requirement_count - 1,
        )
        key = (item.requirement_ordinal, item.text_hash)
        if key in result:
            raise ValidationError(f"duplicate edge refcount key: {key!r}")
        result[key] = item.count
    return result


def _masks_from_refcounts(
    refcounts: Mapping[tuple[int, str], int],
) -> dict[str, int]:
    masks: dict[str, int] = {}
    for (ordinal, text_hash), count in refcounts.items():
        if count > 0:
            masks[text_hash] = masks.get(text_hash, 0) | (1 << ordinal)
    return masks


def coalesce_edge_multiplicity_deltas(
    requirement_count: int,
    refcounts: Mapping[tuple[int, str], int] | Iterable[EdgeRefcount],
    deltas: Iterable[EdgeMultiplicityDelta],
) -> EdgeCoalescingResult:
    """Purely coalesce signed edge-count deltas before Hall maintenance."""

    current = _canonical_refcount_map(requirement_count, refcounts)
    delta_items = tuple(deltas)
    additions = sum(max(item.delta, 0) for item in delta_items)
    removals = sum(max(-item.delta, 0) for item in delta_items)
    totals: dict[tuple[int, str], int] = {}
    for item in delta_items:
        _require_integer(
            "requirement_ordinal",
            item.requirement_ordinal,
            minimum=0,
            maximum=requirement_count - 1,
        )
        key = (item.requirement_ordinal, item.text_hash)
        totals[key] = totals.get(key, 0) + item.delta

    old_masks = _masks_from_refcounts(current)
    changed_hashes: set[str] = set()
    updated_keys = 0
    next_counts = dict(current)
    for key, delta in sorted(totals.items()):
        if delta == 0:
            continue
        updated = next_counts.get(key, 0) + delta
        if updated < 0:
            raise ValidationError(f"edge refcount underflow for {key!r}")
        updated_keys += 1
        changed_hashes.add(key[1])
        if updated == 0:
            next_counts.pop(key, None)
        else:
            next_counts[key] = updated

    new_masks = _masks_from_refcounts(next_counts)
    transitions = tuple(
        HashMaskTransition(
            text_hash,
            old_masks.get(text_hash, 0),
            new_masks.get(text_hash, 0),
        )
        for text_hash in sorted(changed_hashes)
        if old_masks.get(text_hash, 0) != new_masks.get(text_hash, 0)
    )
    canonical_refcounts = tuple(
        EdgeRefcount(ordinal, text_hash, count)
        for (ordinal, text_hash), count in sorted(next_counts.items())
    )
    return EdgeCoalescingResult(
        refcounts=canonical_refcounts,
        transitions=transitions,
        work=MatchingWorkCounters(
            contribution_additions=additions,
            contribution_removals=removals,
            edge_refcount_keys_updated=updated_keys,
        ),
    )


def apply_edge_multiplicity_deltas(
    state: HallMaskState,
    refcounts: Mapping[tuple[int, str], int] | Iterable[EdgeRefcount],
    deltas: Iterable[EdgeMultiplicityDelta],
) -> EdgeDeltaApplication:
    """Coalesce and failure-atomically apply one group's multiplicity batch.

    This defensive convenience path validates the supplied full refcount image
    against the Hall state.  The measured overlay instead retains point indexes
    and invokes the coalescer/kernel on already validated affected keys.
    """

    current = _canonical_refcount_map(state.requirement_count, refcounts)
    initial_masks = _masks_from_refcounts(current)
    expected_histogram = _histogram_from_masks(
        state.requirement_count,
        initial_masks.values(),
    )
    if state.mask_histogram != expected_histogram:
        raise ValidationError("edge refcounts do not match the Hall histogram")
    hall_issues = validate_hall_mask_state(state)
    if hall_issues:
        raise ValidationError("invalid Hall state: " + ", ".join(hall_issues))
    coalesced = coalesce_edge_multiplicity_deltas(
        state.requirement_count,
        current,
        deltas,
    )
    transitioned = apply_hash_mask_transitions(state, coalesced.transitions)
    work = coalesced.work + transitioned.work
    work.assert_nonnegative()
    return EdgeDeltaApplication(
        state=transitioned.state,
        refcounts=coalesced.refcounts,
        transitions=coalesced.transitions,
        work=work,
    )


class CertificateEvidenceView(Protocol):
    """Bounded lookup contract consumed by measured certificate operations.

    Implementations may be a full immutable audit snapshot or a current view
    over maintained ordered indexes.  Measured paths use the latter and never
    construct a full witness image merely to validate or repair a certificate.
    """

    @property
    def epoch_id(self) -> int: ...

    @property
    def revision(self) -> int: ...

    @property
    def decision_policy_version(self) -> str: ...

    @property
    def group_version_id(self) -> str: ...

    @property
    def requirement_version_ids(self) -> tuple[str, ...]: ...

    @property
    def requirement_count(self) -> int: ...

    def representative_hash_masks(self) -> tuple[tuple[str, int], ...]: ...

    def edge_active(self, requirement_ordinal: int, text_hash: str) -> bool: ...

    def least_observation_id(
        self,
        requirement_ordinal: int,
        text_hash: str,
    ) -> str | None: ...

    def observation_active(
        self,
        requirement_ordinal: int,
        text_hash: str,
        observation_id: str,
    ) -> bool: ...

    def hall_histogram(self) -> tuple[int, ...]: ...


@dataclass(frozen=True, slots=True)
class _AvlNode:
    key: str
    left: _AvlNode | None
    right: _AvlNode | None
    height: int
    size: int


def _avl_height(node: _AvlNode | None) -> int:
    return 0 if node is None else node.height


def _avl_size(node: _AvlNode | None) -> int:
    return 0 if node is None else node.size


def _avl_node(
    key: str,
    left: _AvlNode | None,
    right: _AvlNode | None,
) -> _AvlNode:
    return _AvlNode(
        key=key,
        left=left,
        right=right,
        height=1 + max(_avl_height(left), _avl_height(right)),
        size=1 + _avl_size(left) + _avl_size(right),
    )


def _avl_rotate_left(node: _AvlNode) -> _AvlNode:
    pivot = node.right
    if pivot is None:
        raise AssertionError("left rotation requires a right child")
    moved = _avl_node(node.key, node.left, pivot.left)
    return _avl_node(pivot.key, moved, pivot.right)


def _avl_rotate_right(node: _AvlNode) -> _AvlNode:
    pivot = node.left
    if pivot is None:
        raise AssertionError("right rotation requires a left child")
    moved = _avl_node(node.key, pivot.right, node.right)
    return _avl_node(pivot.key, pivot.left, moved)


def _avl_balance(node: _AvlNode) -> _AvlNode:
    balance = _avl_height(node.left) - _avl_height(node.right)
    if balance > 1:
        left = node.left
        if left is None:
            raise AssertionError("invalid AVL left-heavy node")
        if _avl_height(left.left) < _avl_height(left.right):
            left = _avl_rotate_left(left)
            node = _avl_node(node.key, left, node.right)
        return _avl_rotate_right(node)
    if balance < -1:
        right = node.right
        if right is None:
            raise AssertionError("invalid AVL right-heavy node")
        if _avl_height(right.right) < _avl_height(right.left):
            right = _avl_rotate_right(right)
            node = _avl_node(node.key, node.left, right)
        return _avl_rotate_left(node)
    return node


def _avl_add(node: _AvlNode | None, key: str) -> tuple[_AvlNode, bool]:
    if node is None:
        return _avl_node(key, None, None), True
    if key == node.key:
        return node, False
    if key < node.key:
        left, changed = _avl_add(node.left, key)
        if not changed:
            return node, False
        return _avl_balance(_avl_node(node.key, left, node.right)), True
    right, changed = _avl_add(node.right, key)
    if not changed:
        return node, False
    return _avl_balance(_avl_node(node.key, node.left, right)), True


def _avl_least_node(node: _AvlNode) -> _AvlNode:
    current = node
    while current.left is not None:
        current = current.left
    return current


def _avl_remove(
    node: _AvlNode | None,
    key: str,
) -> tuple[_AvlNode | None, bool]:
    if node is None:
        return None, False
    if key < node.key:
        left, changed = _avl_remove(node.left, key)
        if not changed:
            return node, False
        return _avl_balance(_avl_node(node.key, left, node.right)), True
    if key > node.key:
        right, changed = _avl_remove(node.right, key)
        if not changed:
            return node, False
        return _avl_balance(_avl_node(node.key, node.left, right)), True
    if node.left is None:
        return node.right, True
    if node.right is None:
        return node.left, True
    successor = _avl_least_node(node.right)
    right, removed = _avl_remove(node.right, successor.key)
    if not removed:
        raise AssertionError("AVL successor must be removable")
    return _avl_balance(_avl_node(successor.key, node.left, right)), True


def _avl_contains(node: _AvlNode | None, key: str) -> bool:
    current = node
    while current is not None:
        if key == current.key:
            return True
        current = current.left if key < current.key else current.right
    return False


def _avl_first(node: _AvlNode | None, limit: int) -> tuple[str, ...]:
    if node is None or limit <= 0:
        return ()
    result: list[str] = []
    stack: list[_AvlNode] = []
    current: _AvlNode | None = node
    while (current is not None or stack) and len(result) < limit:
        while current is not None:
            stack.append(current)
            current = current.left
        current = stack.pop()
        result.append(current.key)
        current = current.right
    return tuple(result)


def _avl_items(node: _AvlNode | None) -> tuple[str, ...]:
    return _avl_first(node, _avl_size(node))


def _avl_audit(
    node: _AvlNode | None,
    lower: str | None = None,
    upper: str | None = None,
) -> tuple[int, int, tuple[str, ...]]:
    if node is None:
        return 0, 0, ()
    issues: list[str] = []
    if lower is not None and node.key <= lower:
        issues.append("avl_key_not_above_lower_bound")
    if upper is not None and node.key >= upper:
        issues.append("avl_key_not_below_upper_bound")
    left_height, left_size, left_issues = _avl_audit(node.left, lower, node.key)
    right_height, right_size, right_issues = _avl_audit(node.right, node.key, upper)
    issues.extend(left_issues)
    issues.extend(right_issues)
    expected_height = 1 + max(left_height, right_height)
    expected_size = 1 + left_size + right_size
    if node.height != expected_height:
        issues.append("avl_height_mismatch")
    if node.size != expected_size:
        issues.append("avl_size_mismatch")
    if abs(left_height - right_height) > 1:
        issues.append("avl_balance_violation")
    return expected_height, expected_size, tuple(issues)


@dataclass(frozen=True, slots=True)
class PersistentStringSet:
    """Immutable path-copy AVL set with worst-case logarithmic updates."""

    root: _AvlNode | None = None

    def __len__(self) -> int:
        return _avl_size(self.root)

    def add(self, key: str) -> tuple[PersistentStringSet, bool]:
        _require_text("ordered string-set key", key)
        root, changed = _avl_add(self.root, key)
        return PersistentStringSet(root), changed

    def remove(self, key: str) -> tuple[PersistentStringSet, bool]:
        _require_text("ordered string-set key", key)
        root, changed = _avl_remove(self.root, key)
        return PersistentStringSet(root), changed

    def contains(self, key: str) -> bool:
        _require_text("ordered string-set key", key)
        return _avl_contains(self.root, key)

    def least(self) -> str | None:
        return None if self.root is None else _avl_least_node(self.root).key

    def first(self, limit: int) -> tuple[str, ...]:
        _require_integer("ordered string-set limit", limit, minimum=0)
        return _avl_first(self.root, limit)

    def items(self) -> tuple[str, ...]:
        return _avl_items(self.root)

    def audit_issues(self) -> tuple[str, ...]:
        return _avl_audit(self.root)[2]


@dataclass(frozen=True, slots=True, order=True)
class ObservationMembershipDelta:
    requirement_ordinal: int
    text_hash: str
    observation_id: str
    delta: int

    def __post_init__(self) -> None:
        _require_integer("requirement_ordinal", self.requirement_ordinal, minimum=0)
        _require_sha256("text_hash", self.text_hash)
        _require_text("observation_id", self.observation_id)
        _require_integer("observation membership delta", self.delta)
        if self.delta == 0:
            raise ValidationError("observation membership delta must be nonzero")


@dataclass(frozen=True, slots=True)
class MaintainedIndexUpdate:
    transitions: tuple[HashMaskTransition, ...]
    work: MatchingWorkCounters


@dataclass(frozen=True, slots=True)
class _EdgeObservationChange:
    key: tuple[int, str]
    before: PersistentStringSet | None
    after: PersistentStringSet | None


@dataclass(frozen=True, slots=True)
class _ObservationEdgeChange:
    key: str
    before: tuple[int, str] | None
    after: tuple[int, str] | None


@dataclass(frozen=True, slots=True)
class _HashMaskChange:
    key: str
    before: int | None
    after: int | None


@dataclass(frozen=True, slots=True)
class _MaskBucketChange:
    key: int
    before: PersistentStringSet | None
    after: PersistentStringSet | None


@dataclass(slots=True)
class PreparedObservationIndexPatch:
    """Opaque point-granular update prepared against one index generation."""

    expected_generation: int
    transitions: tuple[HashMaskTransition, ...]
    work: MatchingWorkCounters
    _owner: MaintainedCertificateIndex
    _edge_changes: tuple[_EdgeObservationChange, ...]
    _observation_changes: tuple[_ObservationEdgeChange, ...]
    _mask_changes: tuple[_HashMaskChange, ...]
    _bucket_changes: tuple[_MaskBucketChange, ...]
    _state: str = "prepared"

    @property
    def mutates(self) -> bool:
        return bool(
            self._edge_changes
            or self._observation_changes
            or self._mask_changes
            or self._bucket_changes
        )

    def preview_view(
        self,
        *,
        point: SnapshotPoint,
        decision_policy_version: str,
    ) -> PreparedCertificateView:
        """Expose the proposed post-patch certificate view without mutation."""

        return PreparedCertificateView(
            patch=self,
            point=point,
            decision_policy_version=decision_policy_version,
        )


@dataclass(slots=True)
class MaintainedIndexRollbackToken:
    """Single-use rollback capability returned after a prepared patch applies."""

    _owner: MaintainedCertificateIndex
    _patch: PreparedObservationIndexPatch
    _applied_generation: int
    _active: bool = True


@dataclass(frozen=True, slots=True)
class CertificateIndexBuildResult:
    index: MaintainedCertificateIndex
    work: MatchingWorkCounters


class MaintainedCertificateIndex:
    """Maintained expected-O(1) maps with persistent worst-case-O(log N) sets.

    Updates path-copy only touched AVL roots and commit dictionary assignments
    after every logical validation succeeds.  Full scans are confined to the
    explicit ``audit_*`` methods and the bootstrap witness adapter.
    """

    __slots__ = (
        "group_version_id",
        "requirement_version_ids",
        "_edge_observations",
        "_generation",
        "_hash_masks",
        "_hashes_by_mask",
        "_observation_edge",
    )

    def __init__(
        self,
        *,
        group_version_id: str,
        requirement_version_ids: Sequence[str],
    ) -> None:
        _require_text("group_version_id", group_version_id)
        requirement_ids = tuple(requirement_version_ids)
        _require_requirement_count(len(requirement_ids))
        if len(set(requirement_ids)) != len(requirement_ids):
            raise ValidationError("requirement version IDs must be unique")
        for requirement_id in requirement_ids:
            _require_text("requirement_version_id", requirement_id)
        self.group_version_id = group_version_id
        self.requirement_version_ids = requirement_ids
        self._edge_observations: dict[tuple[int, str], PersistentStringSet] = {}
        self._hash_masks: dict[str, int] = {}
        self._hashes_by_mask: dict[int, PersistentStringSet] = {}
        self._observation_edge: dict[str, tuple[int, str]] = {}
        self._generation = 0

    @property
    def requirement_count(self) -> int:
        return len(self.requirement_version_ids)

    @property
    def generation(self) -> int:
        return self._generation

    @classmethod
    def from_requirement_witnesses(
        cls,
        group: EvidenceGroupVersion,
        witnesses: Iterable[RequirementWitness],
    ) -> CertificateIndexBuildResult:
        """Full bootstrap/audit adapter from the shared M5.1 witness contract."""

        index = cls(
            group_version_id=group.group_version_id,
            requirement_version_ids=tuple(
                requirement.requirement_version_id for requirement in group.requirements
            ),
        )
        deltas: list[ObservationMembershipDelta] = []
        seen_edges: set[tuple[int, str]] = set()
        for witness in witnesses:
            ordinal = witness.requirement_ordinal
            _require_integer(
                "requirement_ordinal",
                ordinal,
                minimum=0,
                maximum=index.requirement_count - 1,
            )
            expected_id = index.requirement_version_ids[ordinal]
            if witness.requirement_version_id != expected_id:
                raise ValidationError(
                    "witness requirement ID does not match its group ordinal"
                )
            edge = (ordinal, witness.text_hash)
            if edge in seen_edges:
                raise ValidationError("duplicate RequirementWitness edge")
            seen_edges.add(edge)
            for observation_id in witness.active_observation_ids:
                deltas.append(
                    ObservationMembershipDelta(
                        ordinal,
                        witness.text_hash,
                        observation_id,
                        1,
                    )
                )
        updated = index.apply_observation_deltas(deltas)
        work = updated.work + MatchingWorkCounters(
            hash_masks_initialized=len(index._hash_masks),
        )
        work.assert_nonnegative()
        return CertificateIndexBuildResult(index, work)

    def apply_observation_deltas(
        self,
        deltas: Iterable[ObservationMembershipDelta],
    ) -> MaintainedIndexUpdate:
        """Prepare and apply one update while preserving the original API."""

        patch = self.prepare_observation_deltas(deltas)
        self.apply_prepared_observation_deltas(patch)
        return MaintainedIndexUpdate(patch.transitions, patch.work)

    def prepare_observation_deltas(
        self,
        deltas: Iterable[ObservationMembershipDelta],
    ) -> PreparedObservationIndexPatch:
        """Prepare touched-key persistent-root changes without mutating state.

        The method allocates maps proportional only to the supplied deltas and
        their affected edges, hashes, and mask buckets.  It neither copies an
        index-wide dictionary nor materializes a full certificate snapshot.
        """

        items = tuple(deltas)
        additions = sum(max(item.delta, 0) for item in items)
        removals = sum(max(-item.delta, 0) for item in items)
        totals: dict[tuple[int, str, str], int] = {}
        for item in items:
            _require_integer(
                "requirement_ordinal",
                item.requirement_ordinal,
                minimum=0,
                maximum=self.requirement_count - 1,
            )
            key = (
                item.requirement_ordinal,
                item.text_hash,
                item.observation_id,
            )
            totals[key] = totals.get(key, 0) + item.delta

        observation_targets: dict[str, tuple[int, str]] = {}
        for (ordinal, text_hash, observation_id), delta in totals.items():
            if delta == 0:
                continue
            if delta not in {-1, 1}:
                raise ValidationError(
                    "coalesced observation membership must cross at most once"
                )
            edge = (ordinal, text_hash)
            previous_target = observation_targets.get(observation_id)
            if previous_target is not None and previous_target != edge:
                raise ValidationError(
                    "one observation ID cannot change membership on two edges"
                )
            observation_targets[observation_id] = edge

        empty_set = PersistentStringSet()
        changed_edge_sets: dict[tuple[int, str], PersistentStringSet] = {}
        observation_changes: dict[str, tuple[int, str] | None] = {}
        ordered_operations = 0
        for (ordinal, text_hash, observation_id), delta in totals.items():
            if delta == 0:
                continue
            edge = (ordinal, text_hash)
            current_set = changed_edge_sets.get(
                edge,
                self._edge_observations.get(edge, empty_set),
            )
            current_edge = self._observation_edge.get(observation_id)
            if delta > 0:
                if current_edge is not None:
                    raise ValidationError("observation membership is already active")
                next_set, changed = current_set.add(observation_id)
                if not changed:
                    raise ValidationError("observation membership is already active")
                observation_changes[observation_id] = edge
            else:
                if current_edge != edge:
                    raise ValidationError("observation membership is not active")
                next_set, changed = current_set.remove(observation_id)
                if not changed:
                    raise AssertionError(
                        "active observation must occur in its edge set"
                    )
                observation_changes[observation_id] = None
            changed_edge_sets[edge] = next_set
            ordered_operations += 1

        next_masks: dict[str, int] = {}
        touched_edge_count = 0
        for edge, next_set in changed_edge_sets.items():
            original = self._edge_observations.get(edge, empty_set)
            if original.root is next_set.root:
                continue
            touched_edge_count += 1
            ordinal, text_hash = edge
            old_present = len(original) > 0
            new_present = len(next_set) > 0
            if old_present == new_present:
                continue
            mask = next_masks.get(text_hash, self._hash_masks.get(text_hash, 0))
            bit = 1 << ordinal
            if new_present:
                if mask & bit:
                    raise AssertionError("active edge bit already set")
                mask |= bit
            else:
                if not mask & bit:
                    raise AssertionError("removed edge bit was absent")
                mask &= ~bit
            next_masks[text_hash] = mask

        transitions: list[HashMaskTransition] = []
        changed_buckets: dict[int, PersistentStringSet] = {}
        for text_hash, new_mask in next_masks.items():
            old_mask = self._hash_masks.get(text_hash, 0)
            if old_mask == new_mask:
                continue
            if old_mask:
                old_bucket = changed_buckets.get(
                    old_mask,
                    self._hashes_by_mask.get(old_mask, empty_set),
                )
                next_bucket, removed = old_bucket.remove(text_hash)
                if not removed:
                    raise AssertionError("hash must occur in its old mask bucket")
                changed_buckets[old_mask] = next_bucket
                ordered_operations += 1
            if new_mask:
                new_bucket = changed_buckets.get(
                    new_mask,
                    self._hashes_by_mask.get(new_mask, empty_set),
                )
                next_bucket, added = new_bucket.add(text_hash)
                if not added:
                    raise AssertionError("hash already occurs in its new mask bucket")
                changed_buckets[new_mask] = next_bucket
                ordered_operations += 1
            transitions.append(HashMaskTransition(text_hash, old_mask, new_mask))

        work = MatchingWorkCounters(
            contribution_additions=additions,
            contribution_removals=removals,
            ordered_index_operations=ordered_operations,
            edge_refcount_keys_updated=touched_edge_count,
        )
        work.assert_nonnegative()
        return PreparedObservationIndexPatch(
            expected_generation=self._generation,
            transitions=tuple(transitions),
            work=work,
            _owner=self,
            _edge_changes=tuple(
                _EdgeObservationChange(
                    edge,
                    self._edge_observations.get(edge),
                    None if next_set.root is None else next_set,
                )
                for edge, next_set in changed_edge_sets.items()
            ),
            _observation_changes=tuple(
                _ObservationEdgeChange(
                    observation_id,
                    self._observation_edge.get(observation_id),
                    target_edge,
                )
                for observation_id, target_edge in observation_changes.items()
            ),
            _mask_changes=tuple(
                _HashMaskChange(
                    text_hash,
                    self._hash_masks.get(text_hash),
                    mask or None,
                )
                for text_hash, mask in next_masks.items()
            ),
            _bucket_changes=tuple(
                _MaskBucketChange(
                    mask,
                    self._hashes_by_mask.get(mask),
                    None if bucket.root is None else bucket,
                )
                for mask, bucket in changed_buckets.items()
            ),
        )

    @staticmethod
    def _set_value_matches(
        current: PersistentStringSet | None,
        expected: PersistentStringSet | None,
    ) -> bool:
        return current is expected

    def _validate_prepared_patch(
        self,
        patch: PreparedObservationIndexPatch,
        *,
        use_after: bool,
        expected_generation: int,
    ) -> None:
        if patch._owner is not self:
            raise ValidationError("prepared observation patch belongs to another index")
        if self._generation != expected_generation:
            raise ValidationError("prepared observation patch is stale")
        expected_state = "applied" if use_after else "prepared"
        if patch._state != expected_state:
            raise ValidationError(
                f"prepared observation patch is already {patch._state}"
            )
        for edge_change in patch._edge_changes:
            expected_edge_values = (
                edge_change.after if use_after else edge_change.before
            )
            current_edge_values = self._edge_observations.get(edge_change.key)
            if not self._set_value_matches(current_edge_values, expected_edge_values):
                raise ValidationError("prepared edge-observation precondition failed")
        for observation_change in patch._observation_changes:
            expected_observation_edge = (
                observation_change.after if use_after else observation_change.before
            )
            if (
                self._observation_edge.get(observation_change.key)
                != expected_observation_edge
            ):
                raise ValidationError("prepared observation-edge precondition failed")
        for mask_change in patch._mask_changes:
            expected_mask = mask_change.after if use_after else mask_change.before
            if self._hash_masks.get(mask_change.key) != expected_mask:
                raise ValidationError("prepared hash-mask precondition failed")
        for bucket_change in patch._bucket_changes:
            expected_bucket = bucket_change.after if use_after else bucket_change.before
            current_bucket = self._hashes_by_mask.get(bucket_change.key)
            if not self._set_value_matches(current_bucket, expected_bucket):
                raise ValidationError("prepared mask-bucket precondition failed")

    @staticmethod
    def _assign_or_remove(mapping: dict[_K, _V], key: _K, value: _V | None) -> None:
        if value is None:
            mapping.pop(key, None)
        else:
            mapping[key] = value

    def apply_prepared_observation_deltas(
        self,
        patch: PreparedObservationIndexPatch,
    ) -> MaintainedIndexRollbackToken:
        """Apply one prepared patch and return a single-use rollback token."""

        self._validate_prepared_patch(
            patch,
            use_after=False,
            expected_generation=patch.expected_generation,
        )
        for edge_change in patch._edge_changes:
            self._assign_or_remove(
                self._edge_observations, edge_change.key, edge_change.after
            )
        for observation_change in patch._observation_changes:
            self._assign_or_remove(
                self._observation_edge,
                observation_change.key,
                observation_change.after,
            )
        for mask_change in patch._mask_changes:
            self._assign_or_remove(self._hash_masks, mask_change.key, mask_change.after)
        for bucket_change in patch._bucket_changes:
            self._assign_or_remove(
                self._hashes_by_mask, bucket_change.key, bucket_change.after
            )
        applied_generation = patch.expected_generation + int(patch.mutates)
        self._generation = applied_generation
        patch._state = "applied"
        return MaintainedIndexRollbackToken(
            _owner=self,
            _patch=patch,
            _applied_generation=applied_generation,
        )

    def rollback_prepared_observation_deltas(
        self,
        token: MaintainedIndexRollbackToken,
    ) -> None:
        """Restore every touched value/root and the exact prior generation."""

        if token._owner is not self:
            raise ValidationError("rollback token belongs to another index")
        if not token._active:
            raise ValidationError("rollback token is already consumed")
        patch = token._patch
        self._validate_prepared_patch(
            patch,
            use_after=True,
            expected_generation=token._applied_generation,
        )
        for edge_change in patch._edge_changes:
            self._assign_or_remove(
                self._edge_observations, edge_change.key, edge_change.before
            )
        for observation_change in patch._observation_changes:
            self._assign_or_remove(
                self._observation_edge,
                observation_change.key,
                observation_change.before,
            )
        for mask_change in patch._mask_changes:
            self._assign_or_remove(
                self._hash_masks, mask_change.key, mask_change.before
            )
        for bucket_change in patch._bucket_changes:
            self._assign_or_remove(
                self._hashes_by_mask, bucket_change.key, bucket_change.before
            )
        self._generation = patch.expected_generation
        patch._state = "rolled_back"
        token._active = False

    def current_view(
        self,
        *,
        point: SnapshotPoint,
        decision_policy_version: str,
    ) -> MaintainedCertificateView:
        return MaintainedCertificateView(
            epoch_id=point.epoch_id,
            revision=point.revision,
            decision_policy_version=decision_policy_version,
            group_version_id=self.group_version_id,
            requirement_version_ids=self.requirement_version_ids,
            index=self,
            index_generation=self._generation,
        )

    def audit_snapshot(
        self,
        *,
        point: SnapshotPoint,
        decision_policy_version: str,
    ) -> CertificateSnapshot:
        """Materialize the full image only for bootstrap/audit code."""

        buckets = tuple(
            MaskHashBucket(mask, values.items())
            for mask, values in sorted(self._hashes_by_mask.items())
        )
        edges = tuple(
            ActiveEdgeObservations(ordinal, text_hash, values.items())
            for (ordinal, text_hash), values in sorted(self._edge_observations.items())
        )
        return CertificateSnapshot(
            epoch_id=point.epoch_id,
            revision=point.revision,
            decision_policy_version=decision_policy_version,
            group_version_id=self.group_version_id,
            requirement_version_ids=self.requirement_version_ids,
            mask_hash_buckets=buckets,
            edge_observations=edges,
        )

    def audit_issues(self) -> tuple[str, ...]:
        """Full out-of-band invariant scan; never part of measured latency."""

        issues: list[str] = []
        for values in self._hashes_by_mask.values():
            issues.extend(values.audit_issues())
        for values in self._edge_observations.values():
            issues.extend(values.audit_issues())
        derived_masks: dict[str, int] = {}
        derived_observations: dict[str, tuple[int, str]] = {}
        for (ordinal, text_hash), values in self._edge_observations.items():
            if not len(values):
                issues.append("empty_edge_bucket")
            derived_masks[text_hash] = derived_masks.get(text_hash, 0) | (1 << ordinal)
            for observation_id in values.items():
                if observation_id in derived_observations:
                    issues.append("duplicate_observation_membership")
                derived_observations[observation_id] = (ordinal, text_hash)
        if derived_masks != self._hash_masks:
            issues.append("hash_mask_index_mismatch")
        if derived_observations != self._observation_edge:
            issues.append("observation_reverse_index_mismatch")
        bucket_masks = {
            text_hash: mask
            for mask, values in self._hashes_by_mask.items()
            for text_hash in values.items()
        }
        if bucket_masks != self._hash_masks:
            issues.append("mask_bucket_index_mismatch")
        return tuple(dict.fromkeys(issues))


@dataclass(frozen=True, slots=True)
class MaintainedCertificateView:
    """O(1)-capture current view over the maintained index generation."""

    epoch_id: int
    revision: int
    decision_policy_version: str
    group_version_id: str
    requirement_version_ids: tuple[str, ...]
    index: MaintainedCertificateIndex
    index_generation: int

    def __post_init__(self) -> None:
        _require_integer("epoch_id", self.epoch_id, minimum=0)
        _require_integer("revision", self.revision, minimum=0)
        _require_text("decision_policy_version", self.decision_policy_version)
        if self.group_version_id != self.index.group_version_id:
            raise ValidationError("maintained view group does not match its index")
        if self.requirement_version_ids != self.index.requirement_version_ids:
            raise ValidationError(
                "maintained view requirements do not match their index"
            )
        if self.index_generation != self.index.generation:
            raise ValidationError("maintained view generation is already stale")

    @property
    def requirement_count(self) -> int:
        return len(self.requirement_version_ids)

    def _require_current(self) -> None:
        if self.index_generation != self.index.generation:
            raise ValidationError("maintained certificate view is stale")

    def representative_hash_masks(self) -> tuple[tuple[str, int], ...]:
        self._require_current()
        candidates: list[tuple[str, int]] = []
        for mask in range(1, 1 << self.requirement_count):
            bucket = self.index._hashes_by_mask.get(mask)
            if bucket is None:
                continue
            candidates.extend(
                (text_hash, mask) for text_hash in bucket.first(self.requirement_count)
            )
        return tuple(candidates)

    def edge_active(self, requirement_ordinal: int, text_hash: str) -> bool:
        self._require_current()
        return (requirement_ordinal, text_hash) in self.index._edge_observations

    def least_observation_id(
        self,
        requirement_ordinal: int,
        text_hash: str,
    ) -> str | None:
        self._require_current()
        values = self.index._edge_observations.get((requirement_ordinal, text_hash))
        return None if values is None else values.least()

    def observation_active(
        self,
        requirement_ordinal: int,
        text_hash: str,
        observation_id: str,
    ) -> bool:
        self._require_current()
        return self.index._observation_edge.get(observation_id) == (
            requirement_ordinal,
            text_hash,
        )

    def hall_histogram(self) -> tuple[int, ...]:
        self._require_current()
        histogram = [0] * (1 << self.requirement_count)
        for mask, values in self.index._hashes_by_mask.items():
            histogram[mask] = len(values)
        return tuple(histogram)


class PreparedCertificateView:
    """Certificate evidence over one unapplied touched-key index patch."""

    __slots__ = (
        "_bucket_overrides",
        "_edge_overrides",
        "_observation_overrides",
        "_patch",
        "decision_policy_version",
        "epoch_id",
        "group_version_id",
        "requirement_version_ids",
        "revision",
    )

    def __init__(
        self,
        *,
        patch: PreparedObservationIndexPatch,
        point: SnapshotPoint,
        decision_policy_version: str,
    ) -> None:
        owner = patch._owner
        owner._validate_prepared_patch(
            patch,
            use_after=False,
            expected_generation=patch.expected_generation,
        )
        _require_integer("epoch_id", point.epoch_id, minimum=0)
        _require_integer("revision", point.revision, minimum=0)
        _require_text("decision_policy_version", decision_policy_version)
        self.epoch_id = point.epoch_id
        self.revision = point.revision
        self.decision_policy_version = decision_policy_version
        self.group_version_id = owner.group_version_id
        self.requirement_version_ids = owner.requirement_version_ids
        self._patch = patch
        self._edge_overrides = {
            change.key: change.after for change in patch._edge_changes
        }
        self._observation_overrides = {
            change.key: change.after for change in patch._observation_changes
        }
        self._bucket_overrides = {
            change.key: change.after for change in patch._bucket_changes
        }

    @property
    def requirement_count(self) -> int:
        return len(self.requirement_version_ids)

    def _require_current(self) -> None:
        patch = self._patch
        if patch._state != "prepared":
            raise ValidationError("prepared certificate view is stale")
        if patch._owner.generation != patch.expected_generation:
            raise ValidationError("prepared certificate view is stale")

    def _edge_values(
        self,
        requirement_ordinal: int,
        text_hash: str,
    ) -> PersistentStringSet | None:
        key = (requirement_ordinal, text_hash)
        if key in self._edge_overrides:
            return self._edge_overrides[key]
        return self._patch._owner._edge_observations.get(key)

    def _bucket_values(self, mask: int) -> PersistentStringSet | None:
        if mask in self._bucket_overrides:
            return self._bucket_overrides[mask]
        return self._patch._owner._hashes_by_mask.get(mask)

    def representative_hash_masks(self) -> tuple[tuple[str, int], ...]:
        self._require_current()
        candidates: list[tuple[str, int]] = []
        for mask in range(1, 1 << self.requirement_count):
            bucket = self._bucket_values(mask)
            if bucket is None:
                continue
            candidates.extend(
                (text_hash, mask) for text_hash in bucket.first(self.requirement_count)
            )
        return tuple(candidates)

    def edge_active(self, requirement_ordinal: int, text_hash: str) -> bool:
        self._require_current()
        values = self._edge_values(requirement_ordinal, text_hash)
        return values is not None and len(values) > 0

    def least_observation_id(
        self,
        requirement_ordinal: int,
        text_hash: str,
    ) -> str | None:
        self._require_current()
        values = self._edge_values(requirement_ordinal, text_hash)
        return None if values is None else values.least()

    def observation_active(
        self,
        requirement_ordinal: int,
        text_hash: str,
        observation_id: str,
    ) -> bool:
        self._require_current()
        if observation_id in self._observation_overrides:
            edge = self._observation_overrides[observation_id]
        else:
            edge = self._patch._owner._observation_edge.get(observation_id)
        return edge == (requirement_ordinal, text_hash)

    def hall_histogram(self) -> tuple[int, ...]:
        self._require_current()
        histogram = [0] * (1 << self.requirement_count)
        for mask in range(1, 1 << self.requirement_count):
            values = self._bucket_values(mask)
            if values is not None:
                histogram[mask] = len(values)
        return tuple(histogram)


@dataclass(frozen=True, slots=True, order=True)
class MaskHashBucket:
    mask: int
    text_hashes: tuple[str, ...]


@dataclass(frozen=True, slots=True, order=True)
class ActiveEdgeObservations:
    requirement_ordinal: int
    text_hash: str
    observation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CertificateSnapshot:
    """Exact immutable graph/provenance input at one epoch revision."""

    epoch_id: int
    revision: int
    decision_policy_version: str
    group_version_id: str
    requirement_version_ids: tuple[str, ...]
    mask_hash_buckets: tuple[MaskHashBucket, ...]
    edge_observations: tuple[ActiveEdgeObservations, ...]

    def __post_init__(self) -> None:
        _require_integer("epoch_id", self.epoch_id, minimum=1)
        _require_integer("revision", self.revision, minimum=0)
        _require_text("decision_policy_version", self.decision_policy_version)
        _require_text("group_version_id", self.group_version_id)
        _require_requirement_count(len(self.requirement_version_ids))
        if len(set(self.requirement_version_ids)) != len(self.requirement_version_ids):
            raise ValidationError("requirement version IDs must be unique")
        for requirement_id in self.requirement_version_ids:
            _require_text("requirement_version_id", requirement_id)

        requirement_count = len(self.requirement_version_ids)
        full_mask = (1 << requirement_count) - 1
        masks = tuple(bucket.mask for bucket in self.mask_hash_buckets)
        if masks != tuple(sorted(set(masks))):
            raise ValidationError("mask hash buckets must be ordered and unique")
        hash_to_mask: dict[str, int] = {}
        for bucket in self.mask_hash_buckets:
            _require_integer("mask bucket", bucket.mask, minimum=1, maximum=full_mask)
            if not bucket.text_hashes:
                raise ValidationError("mask hash bucket cannot be empty")
            if bucket.text_hashes != tuple(sorted(set(bucket.text_hashes))):
                raise ValidationError("bucket hashes must be sorted and unique")
            for text_hash in bucket.text_hashes:
                _require_sha256("text_hash", text_hash)
                if text_hash in hash_to_mask:
                    raise ValidationError("a text hash cannot occupy two mask buckets")
                hash_to_mask[text_hash] = bucket.mask

        edge_keys = tuple(
            (edge.requirement_ordinal, edge.text_hash)
            for edge in self.edge_observations
        )
        if edge_keys != tuple(sorted(set(edge_keys))):
            raise ValidationError("edge observations must be ordered and unique")
        derived_masks: dict[str, int] = {}
        seen_observations: set[str] = set()
        for edge in self.edge_observations:
            _require_integer(
                "requirement_ordinal",
                edge.requirement_ordinal,
                minimum=0,
                maximum=requirement_count - 1,
            )
            _require_sha256("text_hash", edge.text_hash)
            if not edge.observation_ids:
                raise ValidationError("an active edge must have an observation")
            if edge.observation_ids != tuple(sorted(set(edge.observation_ids))):
                raise ValidationError("observation IDs must be sorted and unique")
            for observation_id in edge.observation_ids:
                _require_text("observation_id", observation_id)
                if observation_id in seen_observations:
                    raise ValidationError(
                        "one observation ID cannot realize multiple active edges"
                    )
                seen_observations.add(observation_id)
            derived_masks[edge.text_hash] = derived_masks.get(edge.text_hash, 0) | (
                1 << edge.requirement_ordinal
            )
        if derived_masks != hash_to_mask:
            raise ValidationError(
                "mask buckets must exactly equal active edge-observation indexes"
            )

    @property
    def requirement_count(self) -> int:
        return len(self.requirement_version_ids)

    @property
    def hash_masks(self) -> tuple[tuple[str, int], ...]:
        return tuple(
            sorted(
                (text_hash, bucket.mask)
                for bucket in self.mask_hash_buckets
                for text_hash in bucket.text_hashes
            )
        )

    def observations_for(
        self,
        requirement_ordinal: int,
        text_hash: str,
    ) -> tuple[str, ...]:
        """Find one edge's ordered observation IDs by binary search."""

        target = (requirement_ordinal, text_hash)
        lower = 0
        upper = len(self.edge_observations)
        while lower < upper:
            middle = (lower + upper) // 2
            edge = self.edge_observations[middle]
            key = (edge.requirement_ordinal, edge.text_hash)
            if key < target:
                lower = middle + 1
            else:
                upper = middle
        if lower >= len(self.edge_observations):
            return ()
        edge = self.edge_observations[lower]
        if (edge.requirement_ordinal, edge.text_hash) != target:
            return ()
        return edge.observation_ids

    def has_observation(
        self,
        requirement_ordinal: int,
        text_hash: str,
        observation_id: str,
    ) -> bool:
        """Test active edge provenance with two ordered binary searches."""

        observations = self.observations_for(requirement_ordinal, text_hash)
        lower = 0
        upper = len(observations)
        while lower < upper:
            middle = (lower + upper) // 2
            if observations[middle] < observation_id:
                lower = middle + 1
            else:
                upper = middle
        return lower < len(observations) and observations[lower] == observation_id

    def representative_hash_masks(self) -> tuple[tuple[str, int], ...]:
        candidates: list[tuple[str, int]] = []
        for bucket in self.mask_hash_buckets:
            candidates.extend(
                (text_hash, bucket.mask)
                for text_hash in bucket.text_hashes[: self.requirement_count]
            )
        return tuple(candidates)

    def edge_active(self, requirement_ordinal: int, text_hash: str) -> bool:
        return bool(self.observations_for(requirement_ordinal, text_hash))

    def least_observation_id(
        self,
        requirement_ordinal: int,
        text_hash: str,
    ) -> str | None:
        observations = self.observations_for(requirement_ordinal, text_hash)
        return None if not observations else observations[0]

    def observation_active(
        self,
        requirement_ordinal: int,
        text_hash: str,
        observation_id: str,
    ) -> bool:
        return self.has_observation(
            requirement_ordinal,
            text_hash,
            observation_id,
        )

    def hall_histogram(self) -> tuple[int, ...]:
        histogram = [0] * (1 << self.requirement_count)
        for bucket in self.mask_hash_buckets:
            histogram[bucket.mask] = len(bucket.text_hashes)
        return tuple(histogram)

    @classmethod
    def from_primitives(
        cls,
        *,
        epoch_id: int,
        revision: int,
        decision_policy_version: str,
        group_version_id: str,
        requirement_version_ids: Sequence[str],
        hash_masks: Mapping[str, int],
        observation_ids_by_edge: Mapping[tuple[int, str], Iterable[str]],
    ) -> CertificateSnapshot:
        requirement_ids = tuple(requirement_version_ids)
        canonical_masks = _canonical_hash_masks(len(requirement_ids), hash_masks)
        hashes_by_mask: dict[int, list[str]] = {}
        for text_hash, mask in canonical_masks:
            _require_sha256("text_hash", text_hash)
            hashes_by_mask.setdefault(mask, []).append(text_hash)
        buckets = tuple(
            MaskHashBucket(mask, tuple(sorted(text_hashes)))
            for mask, text_hashes in sorted(hashes_by_mask.items())
        )
        edge_observations = tuple(
            ActiveEdgeObservations(
                ordinal,
                text_hash,
                tuple(sorted(observation_ids)),
            )
            for (ordinal, text_hash), observation_ids in sorted(
                observation_ids_by_edge.items()
            )
        )
        return cls(
            epoch_id=epoch_id,
            revision=revision,
            decision_policy_version=decision_policy_version,
            group_version_id=group_version_id,
            requirement_version_ids=requirement_ids,
            mask_hash_buckets=buckets,
            edge_observations=edge_observations,
        )


@dataclass(frozen=True, slots=True)
class WorkingGroupCertificateBinding:
    epoch_id: int
    group_version_id: str
    valid_from_revision: int
    valid_to_revision: int | None
    certificate_digest: str

    def __post_init__(self) -> None:
        _require_integer("epoch_id", self.epoch_id, minimum=1)
        _require_text("group_version_id", self.group_version_id)
        _require_integer("valid_from_revision", self.valid_from_revision, minimum=0)
        if self.valid_to_revision is not None:
            _require_integer(
                "valid_to_revision",
                self.valid_to_revision,
                minimum=self.valid_from_revision + 1,
            )
        _require_sha256("certificate_digest", self.certificate_digest)

    @property
    def open(self) -> bool:
        return self.valid_to_revision is None

    def covers(self, epoch_id: int, revision: int) -> bool:
        if epoch_id != self.epoch_id or revision < self.valid_from_revision:
            return False
        return self.valid_to_revision is None or revision < self.valid_to_revision


@dataclass(frozen=True, slots=True)
class CertificateValidation:
    valid: bool
    issues: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CertificateReconstruction:
    artifact: GroupMatchingCertificateArtifact | None
    matching: AffectedMatchingResult
    work: MatchingWorkCounters


class CertificateTransitionKind(StrEnum):
    RETAIN = "retain"
    BUILD = "build"
    REPAIR = "repair"
    REBUILD = "rebuild"
    REBIND = "rebind"
    REBIND_REPAIR = "rebind_repair"
    REBIND_REBUILD = "rebind_rebuild"
    CLOSE = "close"
    EPOCH_RETAIN = "epoch_retain"
    EPOCH_REPAIR = "epoch_repair"
    EPOCH_REBUILD = "epoch_rebuild"
    EPOCH_REBIND = "epoch_rebind"
    EPOCH_REBIND_REPAIR = "epoch_rebind_repair"
    EPOCH_REBIND_REBUILD = "epoch_rebind_rebuild"
    EPOCH_INCOMPLETE = "epoch_incomplete"


@dataclass(frozen=True, slots=True)
class CertificateTransitionResult:
    kind: CertificateTransitionKind
    artifact: GroupMatchingCertificateArtifact | None
    closed_binding: WorkingGroupCertificateBinding | None
    open_binding: WorkingGroupCertificateBinding | None
    work: MatchingWorkCounters


class CertificateRebuildRequired(ValidationError):
    """A selected edge vanished, so local provenance repair is insufficient."""


def _typed_int(value: int) -> tuple[str, str]:
    _require_integer("typed integer", value)
    return "int", str(value)


def _typed_text(value: str) -> tuple[str, str]:
    _require_text("typed text", value)
    return "text", value


def _typed_hash(value: str) -> tuple[str, str]:
    _require_sha256("typed hash", value)
    return "sha256", value


def _typed_sequence(items: Sequence[Sequence[str]]) -> tuple[str, ...]:
    fields: list[str] = ["sequence", *_typed_int(len(items))]
    for item in items:
        fields.extend(item)
    return tuple(fields)


def _stable_digest_with_size(*parts: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    for part in parts:
        encoded = part.encode("utf-8")
        length = len(encoded).to_bytes(8, "big")
        digest.update(length)
        digest.update(encoded)
        byte_count += len(length) + len(encoded)
    return digest.hexdigest(), byte_count


def _group_certificate_digest_parts(
    *,
    decision_policy_version: str,
    group_version_id: str,
    requirement_count: int,
    rows: Sequence[GroupMatchingCertificateRow],
) -> tuple[str, ...]:
    """Return the exact typed fields in the frozen certificate preimage."""

    _require_text("decision_policy_version", decision_policy_version)
    _require_text("group_version_id", group_version_id)
    _require_requirement_count(requirement_count)
    row_fields: list[tuple[str, ...]] = []
    for row in rows:
        row_fields.append(
            _typed_sequence(
                (
                    _typed_int(row.requirement_ordinal),
                    _typed_text(row.requirement_version_id),
                    _typed_hash(row.text_hash),
                    _typed_text(row.selected_observation_id),
                )
            )
        )
    return (
        GROUP_CERTIFICATE_VERSION,
        *_typed_text(decision_policy_version),
        *_typed_text(group_version_id),
        *_typed_int(requirement_count),
        *_typed_sequence(row_fields),
    )


def _framed_input_size(parts: Sequence[str]) -> int:
    """Return framed UTF-8 input bytes without performing a digest pass."""

    return sum(8 + len(part.encode("utf-8")) for part in parts)


def compute_group_certificate_digest(
    *,
    decision_policy_version: str,
    group_version_id: str,
    requirement_count: int,
    rows: Sequence[GroupMatchingCertificateRow],
) -> tuple[str, int]:
    """Return the exact frozen digest and length-framed preimage byte count."""

    parts = _group_certificate_digest_parts(
        decision_policy_version=decision_policy_version,
        group_version_id=group_version_id,
        requirement_count=requirement_count,
        rows=rows,
    )
    return _stable_digest_with_size(*parts)


def _artifact_from_rows(
    snapshot: CertificateEvidenceView,
    rows: Sequence[GroupMatchingCertificateRow],
) -> tuple[GroupMatchingCertificateArtifact, int]:
    canonical_rows = tuple(rows)
    digest_parts = _group_certificate_digest_parts(
        decision_policy_version=snapshot.decision_policy_version,
        group_version_id=snapshot.group_version_id,
        requirement_count=snapshot.requirement_count,
        rows=canonical_rows,
    )
    digest_input_bytes = _framed_input_size(digest_parts)
    artifact = GroupMatchingCertificateArtifact(
        decision_policy_version=snapshot.decision_policy_version,
        group_version_id=snapshot.group_version_id,
        rows=canonical_rows,
        certificate_version=GROUP_CERTIFICATE_VERSION,
    )
    issues = _artifact_shape_issues(
        artifact,
        snapshot,
        require_policy_match=True,
        require_active_observations=True,
        verify_digest=False,
    )
    if issues:
        raise AssertionError("constructed certificate is invalid: " + ", ".join(issues))
    return artifact, digest_input_bytes


def _artifact_shape_issues(
    artifact: GroupMatchingCertificateArtifact,
    snapshot: CertificateEvidenceView,
    *,
    require_policy_match: bool,
    require_active_observations: bool,
    verify_digest: bool = True,
) -> tuple[str, ...]:
    issues: list[str] = []
    if artifact.certificate_version != GROUP_CERTIFICATE_VERSION:
        issues.append("certificate_version_mismatch")
    if artifact.group_version_id != snapshot.group_version_id:
        issues.append("group_version_mismatch")
    if artifact.requirement_count != snapshot.requirement_count:
        issues.append("requirement_count_mismatch")
    if require_policy_match and (
        artifact.decision_policy_version != snapshot.decision_policy_version
    ):
        issues.append("decision_policy_version_mismatch")
    if len(artifact.rows) != snapshot.requirement_count:
        issues.append("row_count_mismatch")

    ordinals = tuple(row.requirement_ordinal for row in artifact.rows)
    if ordinals != tuple(range(snapshot.requirement_count)):
        issues.append("requirement_ordinals_not_dense")
    hashes = tuple(row.text_hash for row in artifact.rows)
    if len(set(hashes)) != len(hashes):
        issues.append("selected_hashes_not_distinct")
    for row in artifact.rows:
        ordinal = row.requirement_ordinal
        if not 0 <= ordinal < snapshot.requirement_count:
            issues.append("requirement_ordinal_out_of_range")
            continue
        if row.requirement_version_id != snapshot.requirement_version_ids[ordinal]:
            issues.append("requirement_version_mismatch")
        try:
            _require_sha256("text_hash", row.text_hash)
            _require_text("selected_observation_id", row.selected_observation_id)
        except ValidationError:
            issues.append("certificate_row_shape_invalid")
            continue
        if not snapshot.edge_active(ordinal, row.text_hash):
            issues.append("selected_edge_inactive")
        elif require_active_observations and not snapshot.observation_active(
            ordinal,
            row.text_hash,
            row.selected_observation_id,
        ):
            issues.append("selected_observation_inactive")

    if verify_digest:
        try:
            expected_digest, _ = compute_group_certificate_digest(
                decision_policy_version=artifact.decision_policy_version,
                group_version_id=artifact.group_version_id,
                requirement_count=artifact.requirement_count,
                rows=artifact.rows,
            )
        except ValidationError:
            issues.append("certificate_digest_input_invalid")
        else:
            if artifact.certificate_digest != expected_digest:
                issues.append("certificate_digest_mismatch")
    return tuple(dict.fromkeys(issues))


def validate_certificate_artifact(
    artifact: GroupMatchingCertificateArtifact,
    snapshot: CertificateEvidenceView,
) -> CertificateValidation:
    """Validate exact policy/group/edge/observation semantics at a snapshot."""

    issues = _artifact_shape_issues(
        artifact,
        snapshot,
        require_policy_match=True,
        require_active_observations=True,
    )
    return CertificateValidation(not issues, issues)


def validate_bound_certificate(
    artifact: GroupMatchingCertificateArtifact,
    binding: WorkingGroupCertificateBinding,
    snapshot: CertificateEvidenceView,
) -> CertificateValidation:
    """Validate an immutable artifact and its exact epoch/revision binding."""

    issues = list(validate_certificate_artifact(artifact, snapshot).issues)
    if binding.group_version_id != snapshot.group_version_id:
        issues.append("binding_group_version_mismatch")
    if binding.certificate_digest != artifact.certificate_digest:
        issues.append("binding_certificate_digest_mismatch")
    if not binding.covers(snapshot.epoch_id, snapshot.revision):
        issues.append("binding_does_not_cover_snapshot")
    unique_issues = tuple(dict.fromkeys(issues))
    return CertificateValidation(not unique_issues, unique_issues)


def _certificate_candidate_masks(
    snapshot: CertificateEvidenceView,
) -> tuple[tuple[str, int], ...]:
    return snapshot.representative_hash_masks()


def reconstruct_certificate(
    snapshot: CertificateEvidenceView,
) -> CertificateReconstruction:
    """Deterministically build a certificate from bounded mask representatives."""

    candidates = _certificate_candidate_masks(snapshot)
    matching = affected_group_matching(snapshot.requirement_count, candidates)
    representative_observations = 0
    artifact: GroupMatchingCertificateArtifact | None = None
    digest_input_bytes = 0
    if matching.complete:
        rows: list[GroupMatchingCertificateRow] = []
        for pair in matching.pairs:
            observation_id = snapshot.least_observation_id(
                pair.requirement_ordinal,
                pair.text_hash,
            )
            if observation_id is None:
                raise AssertionError("candidate matching selected an absent edge")
            representative_observations += 1
            rows.append(
                GroupMatchingCertificateRow(
                    requirement_ordinal=pair.requirement_ordinal,
                    requirement_version_id=snapshot.requirement_version_ids[
                        pair.requirement_ordinal
                    ],
                    text_hash=pair.text_hash,
                    selected_observation_id=observation_id,
                )
            )
        artifact, digest_input_bytes = _artifact_from_rows(snapshot, rows)
    work = matching.work + MatchingWorkCounters(
        certificate_reconstructions=1,
        ordered_index_operations=(len(candidates) + representative_observations),
        representative_hashes_read=len(candidates),
        representative_observations_read=representative_observations,
        certificate_digest_input_bytes=digest_input_bytes,
    )
    return CertificateReconstruction(artifact, matching, work)


def open_certificate_binding(
    snapshot: CertificateEvidenceView,
    artifact: GroupMatchingCertificateArtifact,
) -> WorkingGroupCertificateBinding:
    validation = validate_certificate_artifact(artifact, snapshot)
    if not validation.valid:
        raise ValidationError(
            "cannot bind invalid certificate: " + ", ".join(validation.issues)
        )
    return _open_validated_certificate_binding(snapshot, artifact)


def _open_validated_certificate_binding(
    snapshot: CertificateEvidenceView,
    artifact: GroupMatchingCertificateArtifact,
) -> WorkingGroupCertificateBinding:
    """Open a binding after the caller validated this frozen artifact once."""

    return WorkingGroupCertificateBinding(
        epoch_id=snapshot.epoch_id,
        group_version_id=snapshot.group_version_id,
        valid_from_revision=snapshot.revision,
        valid_to_revision=None,
        certificate_digest=artifact.certificate_digest,
    )


def close_certificate_binding(
    binding: WorkingGroupCertificateBinding,
    *,
    closing_revision: int,
) -> WorkingGroupCertificateBinding:
    if not binding.open:
        raise ValidationError("certificate binding is already closed")
    _require_integer(
        "closing_revision",
        closing_revision,
        minimum=binding.valid_from_revision + 1,
    )
    return replace(binding, valid_to_revision=closing_revision)


def _require_prior_binding(
    artifact: GroupMatchingCertificateArtifact,
    binding: WorkingGroupCertificateBinding,
    snapshot: CertificateEvidenceView,
) -> None:
    if not binding.open:
        raise ValidationError("prior certificate binding must be open")
    if binding.epoch_id != snapshot.epoch_id:
        raise ValidationError("prior binding belongs to another epoch")
    if binding.group_version_id != snapshot.group_version_id:
        raise ValidationError("prior binding belongs to another group")
    if binding.certificate_digest != artifact.certificate_digest:
        raise ValidationError("prior binding does not name the prior artifact")
    if binding.valid_from_revision >= snapshot.revision:
        raise ValidationError("certificate transition revision must advance")


def _replace_open_binding(
    artifact: GroupMatchingCertificateArtifact,
    binding: WorkingGroupCertificateBinding,
    snapshot: CertificateEvidenceView,
) -> tuple[WorkingGroupCertificateBinding, WorkingGroupCertificateBinding]:
    closed = close_certificate_binding(
        binding,
        closing_revision=snapshot.revision,
    )
    opened = _open_validated_certificate_binding(snapshot, artifact)
    return closed, opened


def build_or_rebuild_certificate(
    snapshot: CertificateEvidenceView,
    *,
    prior_artifact: GroupMatchingCertificateArtifact | None = None,
    prior_binding: WorkingGroupCertificateBinding | None = None,
) -> CertificateTransitionResult:
    """Build/rebuild deterministically, or close when the graph is incomplete."""

    if (prior_artifact is None) != (prior_binding is None):
        raise ValidationError("prior artifact and binding must be supplied together")
    if prior_artifact is not None and prior_binding is not None:
        _require_prior_binding(prior_artifact, prior_binding, snapshot)
        prior_issues = _artifact_shape_issues(
            prior_artifact,
            snapshot,
            require_policy_match=True,
            require_active_observations=True,
            verify_digest=False,
        )
        blocking_issues = tuple(
            issue
            for issue in prior_issues
            if issue not in {"selected_edge_inactive", "selected_observation_inactive"}
        )
        if blocking_issues:
            raise ValidationError(
                "prior certificate is malformed: " + ", ".join(blocking_issues)
            )

    reconstruction = reconstruct_certificate(snapshot)
    work = reconstruction.work
    if reconstruction.artifact is None:
        if prior_binding is None:
            return CertificateTransitionResult(
                CertificateTransitionKind.CLOSE,
                None,
                None,
                None,
                work,
            )
        closed = close_certificate_binding(
            prior_binding,
            closing_revision=snapshot.revision,
        )
        return CertificateTransitionResult(
            CertificateTransitionKind.CLOSE,
            None,
            closed,
            None,
            work + MatchingWorkCounters(group_local_state_operations=1),
        )

    artifact = reconstruction.artifact
    if prior_artifact is None or prior_binding is None:
        opened = _open_validated_certificate_binding(snapshot, artifact)
        return CertificateTransitionResult(
            CertificateTransitionKind.BUILD,
            artifact,
            None,
            opened,
            work + MatchingWorkCounters(group_local_state_operations=1),
        )
    if artifact.certificate_digest == prior_artifact.certificate_digest:
        return CertificateTransitionResult(
            CertificateTransitionKind.RETAIN,
            prior_artifact,
            None,
            prior_binding,
            work,
        )
    closed, opened = _replace_open_binding(
        artifact,
        prior_binding,
        snapshot,
    )
    return CertificateTransitionResult(
        CertificateTransitionKind.REBUILD,
        artifact,
        closed,
        opened,
        work + MatchingWorkCounters(group_local_state_operations=1),
    )


def repair_selected_observations(
    snapshot: CertificateEvidenceView,
    *,
    prior_artifact: GroupMatchingCertificateArtifact,
    prior_binding: WorkingGroupCertificateBinding,
) -> CertificateTransitionResult:
    """Repair vanished selected provenance while retaining every selected edge."""

    _require_prior_binding(prior_artifact, prior_binding, snapshot)
    issues = _artifact_shape_issues(
        prior_artifact,
        snapshot,
        require_policy_match=True,
        require_active_observations=False,
        verify_digest=False,
    )
    blocking = tuple(
        issue for issue in issues if issue != "selected_observation_inactive"
    )
    if blocking:
        if "selected_edge_inactive" in blocking:
            raise CertificateRebuildRequired("a selected certificate edge disappeared")
        raise ValidationError("prior certificate is malformed: " + ", ".join(blocking))

    repaired_rows: list[GroupMatchingCertificateRow] = []
    repairs = 0
    representative_reads = 0
    for row in prior_artifact.rows:
        if snapshot.observation_active(
            row.requirement_ordinal,
            row.text_hash,
            row.selected_observation_id,
        ):
            repaired_rows.append(row)
            continue
        observation_id = snapshot.least_observation_id(
            row.requirement_ordinal,
            row.text_hash,
        )
        if observation_id is None:
            raise CertificateRebuildRequired("a selected certificate edge disappeared")
        repairs += 1
        representative_reads += 1
        repaired_rows.append(replace(row, selected_observation_id=observation_id))

    if repairs == 0:
        return CertificateTransitionResult(
            CertificateTransitionKind.RETAIN,
            prior_artifact,
            None,
            prior_binding,
            MatchingWorkCounters(),
        )

    artifact, digest_input_bytes = _artifact_from_rows(snapshot, repaired_rows)
    closed, opened = _replace_open_binding(artifact, prior_binding, snapshot)
    return CertificateTransitionResult(
        CertificateTransitionKind.REPAIR,
        artifact,
        closed,
        opened,
        MatchingWorkCounters(
            certificate_repairs=repairs,
            ordered_index_operations=representative_reads,
            representative_observations_read=representative_reads,
            certificate_digest_input_bytes=digest_input_bytes,
            group_local_state_operations=1,
        ),
    )


def rebind_certificate_policy(
    snapshot: CertificateEvidenceView,
    *,
    prior_artifact: GroupMatchingCertificateArtifact,
    prior_binding: WorkingGroupCertificateBinding,
) -> CertificateTransitionResult:
    """Rebind a complete group to a new policy, even when no edge flips.

    Ordered score-range probes are transaction-global and are therefore
    charged separately through :func:`policy_range_probe_work`.
    """

    _require_prior_binding(prior_artifact, prior_binding, snapshot)
    if prior_artifact.decision_policy_version == snapshot.decision_policy_version:
        raise ValidationError("policy rebind requires a new policy version")

    structural_issues = _artifact_shape_issues(
        prior_artifact,
        snapshot,
        require_policy_match=False,
        require_active_observations=True,
        verify_digest=False,
    )
    blocking_issues = tuple(
        issue
        for issue in structural_issues
        if issue not in {"selected_edge_inactive", "selected_observation_inactive"}
    )
    if blocking_issues:
        raise ValidationError(
            "prior certificate is malformed: " + ", ".join(blocking_issues)
        )
    if "selected_edge_inactive" not in structural_issues:
        rebound_rows: list[GroupMatchingCertificateRow] = []
        repairs = 0
        representative_reads = 0
        for row in prior_artifact.rows:
            if snapshot.observation_active(
                row.requirement_ordinal,
                row.text_hash,
                row.selected_observation_id,
            ):
                rebound_rows.append(row)
                continue
            observation_id = snapshot.least_observation_id(
                row.requirement_ordinal,
                row.text_hash,
            )
            if observation_id is None:
                raise AssertionError("active selected edge has no observation")
            repairs += 1
            representative_reads += 1
            rebound_rows.append(replace(row, selected_observation_id=observation_id))
        artifact, digest_input_bytes = _artifact_from_rows(snapshot, rebound_rows)
        closed, opened = _replace_open_binding(artifact, prior_binding, snapshot)
        return CertificateTransitionResult(
            (
                CertificateTransitionKind.REBIND_REPAIR
                if repairs
                else CertificateTransitionKind.REBIND
            ),
            artifact,
            closed,
            opened,
            MatchingWorkCounters(
                certificate_repairs=repairs,
                policy_rebindings=1,
                ordered_index_operations=representative_reads,
                representative_observations_read=representative_reads,
                certificate_digest_input_bytes=digest_input_bytes,
                group_local_state_operations=1,
            ),
        )

    reconstruction = reconstruct_certificate(snapshot)
    if reconstruction.artifact is None:
        closed = close_certificate_binding(
            prior_binding,
            closing_revision=snapshot.revision,
        )
        return CertificateTransitionResult(
            CertificateTransitionKind.CLOSE,
            None,
            closed,
            None,
            reconstruction.work
            + MatchingWorkCounters(
                policy_rebindings=1,
                group_local_state_operations=1,
            ),
        )
    artifact = reconstruction.artifact
    closed, opened = _replace_open_binding(artifact, prior_binding, snapshot)
    return CertificateTransitionResult(
        CertificateTransitionKind.REBIND_REBUILD,
        artifact,
        closed,
        opened,
        reconstruction.work
        + MatchingWorkCounters(
            policy_rebindings=1,
            group_local_state_operations=1,
        ),
    )


def carry_forward_certificate_epoch(
    snapshot: CertificateEvidenceView,
    *,
    prior_artifact: GroupMatchingCertificateArtifact,
    prior_binding: WorkingGroupCertificateBinding,
) -> CertificateTransitionResult:
    """Carry a published certificate into a later epoch at any revision.

    The previous epoch's binding is immutable history and is never closed by
    this transition.  The resulting current-epoch binding starts directly at
    the supplied snapshot revision, including the normal new-epoch revision
    zero case used by M5 structural and observation events.
    """

    if not prior_binding.open:
        raise ValidationError("prior certificate binding must be open")
    if prior_binding.epoch_id >= snapshot.epoch_id:
        raise ValidationError("epoch carry-forward requires a later epoch")
    if prior_binding.group_version_id != snapshot.group_version_id:
        raise ValidationError("prior binding belongs to another group")
    if prior_binding.certificate_digest != prior_artifact.certificate_digest:
        raise ValidationError("prior binding does not name the prior artifact")
    if prior_artifact.group_version_id != snapshot.group_version_id:
        raise ValidationError("prior artifact belongs to another group")

    structural_issues = _artifact_shape_issues(
        prior_artifact,
        snapshot,
        require_policy_match=False,
        require_active_observations=True,
        verify_digest=False,
    )
    blocking_issues = tuple(
        issue
        for issue in structural_issues
        if issue not in {"selected_edge_inactive", "selected_observation_inactive"}
    )
    if blocking_issues:
        raise ValidationError(
            "prior certificate is malformed: " + ", ".join(blocking_issues)
        )

    policy_changed = (
        prior_artifact.decision_policy_version != snapshot.decision_policy_version
    )
    if "selected_edge_inactive" not in structural_issues:
        rows: list[GroupMatchingCertificateRow] = []
        repairs = 0
        representative_reads = 0
        for row in prior_artifact.rows:
            if snapshot.observation_active(
                row.requirement_ordinal,
                row.text_hash,
                row.selected_observation_id,
            ):
                rows.append(row)
                continue
            observation_id = snapshot.least_observation_id(
                row.requirement_ordinal,
                row.text_hash,
            )
            if observation_id is None:
                raise AssertionError("active selected edge has no observation")
            repairs += 1
            representative_reads += 1
            rows.append(replace(row, selected_observation_id=observation_id))

        digest_input_bytes = 0
        if policy_changed or repairs:
            artifact, digest_input_bytes = _artifact_from_rows(snapshot, rows)
        else:
            artifact = prior_artifact
        opened = _open_validated_certificate_binding(snapshot, artifact)
        if policy_changed:
            kind = (
                CertificateTransitionKind.EPOCH_REBIND_REPAIR
                if repairs
                else CertificateTransitionKind.EPOCH_REBIND
            )
        else:
            kind = (
                CertificateTransitionKind.EPOCH_REPAIR
                if repairs
                else CertificateTransitionKind.EPOCH_RETAIN
            )
        return CertificateTransitionResult(
            kind,
            artifact,
            None,
            opened,
            MatchingWorkCounters(
                certificate_repairs=repairs,
                policy_rebindings=int(policy_changed),
                ordered_index_operations=representative_reads,
                representative_observations_read=representative_reads,
                certificate_digest_input_bytes=digest_input_bytes,
                group_local_state_operations=1,
            ),
        )

    reconstruction = reconstruct_certificate(snapshot)
    policy_work = MatchingWorkCounters(
        policy_rebindings=int(policy_changed),
        group_local_state_operations=1,
    )
    if reconstruction.artifact is None:
        return CertificateTransitionResult(
            CertificateTransitionKind.EPOCH_INCOMPLETE,
            None,
            None,
            None,
            reconstruction.work + policy_work,
        )
    artifact = reconstruction.artifact
    opened = _open_validated_certificate_binding(snapshot, artifact)
    return CertificateTransitionResult(
        (
            CertificateTransitionKind.EPOCH_REBIND_REBUILD
            if policy_changed
            else CertificateTransitionKind.EPOCH_REBUILD
        ),
        artifact,
        None,
        opened,
        reconstruction.work + policy_work,
    )


def close_incomplete_certificate(
    snapshot: CertificateEvidenceView,
    *,
    hall_state: HallMaskState,
    prior_artifact: GroupMatchingCertificateArtifact,
    prior_binding: WorkingGroupCertificateBinding,
) -> CertificateTransitionResult:
    """Close a binding after Hall state proves incompleteness, without rematching."""

    _require_prior_binding(prior_artifact, prior_binding, snapshot)
    prior_issues = _artifact_shape_issues(
        prior_artifact,
        snapshot,
        require_policy_match=True,
        require_active_observations=True,
        verify_digest=False,
    )
    blocking_issues = tuple(
        issue
        for issue in prior_issues
        if issue not in {"selected_edge_inactive", "selected_observation_inactive"}
    )
    if blocking_issues:
        raise ValidationError(
            "prior certificate is malformed: " + ", ".join(blocking_issues)
        )
    if hall_state.requirement_count != snapshot.requirement_count:
        raise ValidationError("Hall state belongs to a different group shape")
    if hall_state.mask_histogram != snapshot.hall_histogram():
        raise ValidationError("Hall state does not match the certificate snapshot")
    if hall_state.complete:
        raise ValidationError("cannot close a certificate for a complete group")
    closed = close_certificate_binding(
        prior_binding,
        closing_revision=snapshot.revision,
    )
    return CertificateTransitionResult(
        CertificateTransitionKind.CLOSE,
        None,
        closed,
        None,
        MatchingWorkCounters(group_local_state_operations=1),
    )
