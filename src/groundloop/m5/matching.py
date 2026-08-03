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

from groundloop.errors import ValidationError

MAX_REQUIREMENTS = 8
GROUP_CERTIFICATE_VERSION = "m5-group-certificate-v1"
_HEX = frozenset("0123456789abcdef")


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
                self.policy_candidate_observations
                + other.policy_candidate_observations
            ),
            ordered_policy_range_probes=(
                self.ordered_policy_range_probes
                + other.ordered_policy_range_probes
            ),
            ordered_index_operations=(
                self.ordered_index_operations + other.ordered_index_operations
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
                self.hall_subset_entries_examined
                + other.hall_subset_entries_examined
            ),
            hall_neighbor_entries_changed=(
                self.hall_neighbor_entries_changed
                + other.hall_neighbor_entries_changed
            ),
            hall_deficiency_entries_examined=(
                self.hall_deficiency_entries_examined
                + other.hall_deficiency_entries_examined
            ),
            certificate_repairs=(
                self.certificate_repairs + other.certificate_repairs
            ),
            certificate_reconstructions=(
                self.certificate_reconstructions
                + other.certificate_reconstructions
            ),
            policy_rebindings=self.policy_rebindings + other.policy_rebindings,
            representative_hashes_read=(
                self.representative_hashes_read + other.representative_hashes_read
            ),
            representative_observations_read=(
                self.representative_observations_read
                + other.representative_observations_read
            ),
            augmenting_searches=(
                self.augmenting_searches + other.augmenting_searches
            ),
            augmenting_requirement_visits=(
                self.augmenting_requirement_visits
                + other.augmenting_requirement_visits
            ),
            augmenting_edge_visits=(
                self.augmenting_edge_visits + other.augmenting_edge_visits
            ),
            certificate_digest_input_bytes=(
                self.certificate_digest_input_bytes
                + other.certificate_digest_input_bytes
            ),
            group_local_state_operations=(
                self.group_local_state_operations
                + other.group_local_state_operations
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
                self.policy_candidate_observations
                - other.policy_candidate_observations
            ),
            ordered_policy_range_probes=(
                self.ordered_policy_range_probes
                - other.ordered_policy_range_probes
            ),
            ordered_index_operations=(
                self.ordered_index_operations - other.ordered_index_operations
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
                self.hall_subset_entries_examined
                - other.hall_subset_entries_examined
            ),
            hall_neighbor_entries_changed=(
                self.hall_neighbor_entries_changed
                - other.hall_neighbor_entries_changed
            ),
            hall_deficiency_entries_examined=(
                self.hall_deficiency_entries_examined
                - other.hall_deficiency_entries_examined
            ),
            certificate_repairs=(
                self.certificate_repairs - other.certificate_repairs
            ),
            certificate_reconstructions=(
                self.certificate_reconstructions
                - other.certificate_reconstructions
            ),
            policy_rebindings=self.policy_rebindings - other.policy_rebindings,
            representative_hashes_read=(
                self.representative_hashes_read - other.representative_hashes_read
            ),
            representative_observations_read=(
                self.representative_observations_read
                - other.representative_observations_read
            ),
            augmenting_searches=(
                self.augmenting_searches - other.augmenting_searches
            ),
            augmenting_requirement_visits=(
                self.augmenting_requirement_visits
                - other.augmenting_requirement_visits
            ),
            augmenting_edge_visits=(
                self.augmenting_edge_visits - other.augmenting_edge_visits
            ),
            certificate_digest_input_bytes=(
                self.certificate_digest_input_bytes
                - other.certificate_digest_input_bytes
            ),
            group_local_state_operations=(
                self.group_local_state_operations
                - other.group_local_state_operations
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


def _canonical_hash_masks(
    requirement_count: int,
    hash_masks: Mapping[str, int] | Iterable[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    _require_requirement_count(requirement_count)
    items = (
        tuple(hash_masks.items())
        if isinstance(hash_masks, Mapping)
        else tuple(hash_masks)
    )
    seen_hashes: set[str] = set()
    canonical: list[tuple[str, int]] = []
    full_mask = (1 << requirement_count) - 1
    for text_hash, mask in items:
        _require_text("text_hash", text_hash)
        _require_integer("hash mask", mask, minimum=1, maximum=full_mask)
        if text_hash in seen_hashes:
            raise ValidationError(f"duplicate text hash: {text_hash}")
        seen_hashes.add(text_hash)
        canonical.append((text_hash, mask))
    return tuple(sorted(canonical))


def affected_group_matching(
    requirement_count: int,
    hash_masks: Mapping[str, int] | Iterable[tuple[str, int]],
) -> AffectedMatchingResult:
    """Return a deterministic maximum matching for one affected group.

    Requirements are attempted in ordinal order.  Each augmenting search visits
    candidate hashes in lexicographic order.  Stateful callers may retain a
    previously valid certificate instead; this function is the deliberately
    simple affected-group full-matching baseline and deterministic constructor.
    """

    canonical = _canonical_hash_masks(requirement_count, hash_masks)
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
        for ordinal in sorted(requirement_hash)
    )
    work = MatchingWorkCounters(
        augmenting_searches=requirement_count,
        augmenting_requirement_visits=requirement_visits,
        augmenting_edge_visits=edge_visits,
    )
    return AffectedMatchingResult(requirement_count, pairs, work)


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
        neighbor_counts[subset] = (
            distinct_hash_count - subset_sums[full_mask ^ subset]
        )
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
        canonical = _canonical_hash_masks(requirement_count, hash_masks)
        masks = tuple(mask for _, mask in canonical)
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

    canonical = tuple(sorted(transitions))
    seen: set[str] = set()
    current = state
    work = MatchingWorkCounters()
    changed = False
    full_mask = (1 << state.requirement_count) - 1
    for transition in canonical:
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
        if len(set(self.requirement_version_ids)) != len(
            self.requirement_version_ids
        ):
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


@dataclass(frozen=True, slots=True, order=True)
class GroupMatchingCertificateRow:
    requirement_ordinal: int
    requirement_version_id: str
    text_hash: str
    selected_observation_id: str


@dataclass(frozen=True, slots=True)
class GroupMatchingCertificateArtifact:
    certificate_digest: str
    decision_policy_version: str
    certificate_version: str
    group_version_id: str
    requirement_count: int
    rows: tuple[GroupMatchingCertificateRow, ...]


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


def compute_group_certificate_digest(
    *,
    decision_policy_version: str,
    group_version_id: str,
    requirement_count: int,
    rows: Sequence[GroupMatchingCertificateRow],
) -> tuple[str, int]:
    """Return the exact frozen digest and length-framed preimage byte count."""

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
    parts = (
        GROUP_CERTIFICATE_VERSION,
        *_typed_text(decision_policy_version),
        *_typed_text(group_version_id),
        *_typed_int(requirement_count),
        *_typed_sequence(row_fields),
    )
    return _stable_digest_with_size(*parts)


def _artifact_from_rows(
    snapshot: CertificateSnapshot,
    rows: Sequence[GroupMatchingCertificateRow],
) -> tuple[GroupMatchingCertificateArtifact, int]:
    canonical_rows = tuple(rows)
    digest, digest_input_bytes = compute_group_certificate_digest(
        decision_policy_version=snapshot.decision_policy_version,
        group_version_id=snapshot.group_version_id,
        requirement_count=snapshot.requirement_count,
        rows=canonical_rows,
    )
    artifact = GroupMatchingCertificateArtifact(
        certificate_digest=digest,
        decision_policy_version=snapshot.decision_policy_version,
        certificate_version=GROUP_CERTIFICATE_VERSION,
        group_version_id=snapshot.group_version_id,
        requirement_count=snapshot.requirement_count,
        rows=canonical_rows,
    )
    validation = validate_certificate_artifact(artifact, snapshot)
    if not validation.valid:
        raise AssertionError(
            "constructed certificate is invalid: " + ", ".join(validation.issues)
        )
    return artifact, digest_input_bytes


def _artifact_shape_issues(
    artifact: GroupMatchingCertificateArtifact,
    snapshot: CertificateSnapshot,
    *,
    require_policy_match: bool,
    require_active_observations: bool,
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
        observations = snapshot.observations_for(ordinal, row.text_hash)
        if not observations:
            issues.append("selected_edge_inactive")
        elif require_active_observations and not snapshot.has_observation(
            ordinal,
            row.text_hash,
            row.selected_observation_id,
        ):
            issues.append("selected_observation_inactive")

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
    snapshot: CertificateSnapshot,
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
    snapshot: CertificateSnapshot,
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
    snapshot: CertificateSnapshot,
) -> tuple[tuple[str, int], ...]:
    candidates: list[tuple[str, int]] = []
    for bucket in snapshot.mask_hash_buckets:
        limit = min(len(bucket.text_hashes), snapshot.requirement_count)
        candidates.extend(
            (text_hash, bucket.mask) for text_hash in bucket.text_hashes[:limit]
        )
    return tuple(candidates)


def reconstruct_certificate(
    snapshot: CertificateSnapshot,
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
            observation_ids = snapshot.observations_for(
                pair.requirement_ordinal,
                pair.text_hash,
            )
            if not observation_ids:
                raise AssertionError("candidate matching selected an absent edge")
            representative_observations += 1
            rows.append(
                GroupMatchingCertificateRow(
                    requirement_ordinal=pair.requirement_ordinal,
                    requirement_version_id=snapshot.requirement_version_ids[
                        pair.requirement_ordinal
                    ],
                    text_hash=pair.text_hash,
                    selected_observation_id=observation_ids[0],
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
    snapshot: CertificateSnapshot,
    artifact: GroupMatchingCertificateArtifact,
) -> WorkingGroupCertificateBinding:
    validation = validate_certificate_artifact(artifact, snapshot)
    if not validation.valid:
        raise ValidationError(
            "cannot bind invalid certificate: " + ", ".join(validation.issues)
        )
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
    snapshot: CertificateSnapshot,
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
    snapshot: CertificateSnapshot,
) -> tuple[WorkingGroupCertificateBinding, WorkingGroupCertificateBinding]:
    closed = close_certificate_binding(
        binding,
        closing_revision=snapshot.revision,
    )
    opened = open_certificate_binding(snapshot, artifact)
    return closed, opened


def build_or_rebuild_certificate(
    snapshot: CertificateSnapshot,
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
        opened = open_certificate_binding(snapshot, artifact)
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
    snapshot: CertificateSnapshot,
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
        observation_ids = snapshot.observations_for(
            row.requirement_ordinal,
            row.text_hash,
        )
        if not observation_ids:
            raise CertificateRebuildRequired("a selected certificate edge disappeared")
        if snapshot.has_observation(
            row.requirement_ordinal,
            row.text_hash,
            row.selected_observation_id,
        ):
            repaired_rows.append(row)
            continue
        repairs += 1
        representative_reads += 1
        repaired_rows.append(replace(row, selected_observation_id=observation_ids[0]))

    if repairs == 0:
        validation = validate_bound_certificate(
            prior_artifact,
            prior_binding,
            snapshot,
        )
        if not validation.valid:
            raise ValidationError(
                "retained certificate is invalid: " + ", ".join(validation.issues)
            )
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
    snapshot: CertificateSnapshot,
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
            if snapshot.has_observation(
                row.requirement_ordinal,
                row.text_hash,
                row.selected_observation_id,
            ):
                rebound_rows.append(row)
                continue
            observations = snapshot.observations_for(
                row.requirement_ordinal,
                row.text_hash,
            )
            if not observations:
                raise AssertionError("active selected edge has no observation")
            repairs += 1
            representative_reads += 1
            rebound_rows.append(
                replace(row, selected_observation_id=observations[0])
            )
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


def close_incomplete_certificate(
    snapshot: CertificateSnapshot,
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
    expected_histogram = [0] * (1 << snapshot.requirement_count)
    for bucket in snapshot.mask_hash_buckets:
        expected_histogram[bucket.mask] = len(bucket.text_hashes)
    if hall_state.mask_histogram != tuple(expected_histogram):
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
