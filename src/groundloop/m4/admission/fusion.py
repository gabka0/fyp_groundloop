"""Deterministic channel fusion, pair deduplication and lineage accounting."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    AdmittedPair,
    CandidatePolicyManifest,
    ChannelHit,
    PairKey,
    stable_m4_digest,
)

_APPROXIMATE_CHANNELS = (AdmissionChannel.VECTOR, AdmissionChannel.LEXICAL)


@dataclass(frozen=True, slots=True)
class AdmissionAccounting:
    inserted_chunk_count: int
    approximate_cap_per_inserted_chunk: int
    approximate_pair_count: int
    mandatory_lineage_pair_count: int
    lineage_excess_pair_count: int
    admitted_pair_count: int
    verifier_call_upper_bound: int

    def __post_init__(self) -> None:
        counts = (
            self.inserted_chunk_count,
            self.approximate_pair_count,
            self.mandatory_lineage_pair_count,
            self.lineage_excess_pair_count,
            self.admitted_pair_count,
            self.verifier_call_upper_bound,
        )
        if any(count < 0 for count in counts):
            raise ValidationError("admission accounting counts must be nonnegative")
        if self.approximate_cap_per_inserted_chunk <= 0:
            raise ValidationError("approximate cap must be positive")
        expected_bound = (
            self.inserted_chunk_count * self.approximate_cap_per_inserted_chunk
            + self.lineage_excess_pair_count
        )
        if self.verifier_call_upper_bound != expected_bound:
            raise ValidationError("verifier-call bound does not match frozen formula")
        if self.admitted_pair_count > self.verifier_call_upper_bound:
            raise ValidationError("admitted pairs exceed the frozen call bound")


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    channel_hits: tuple[ChannelHit, ...]
    admitted_pairs: tuple[AdmittedPair, ...]
    accounting: AdmissionAccounting


def lineage_channel_hits(
    *,
    epoch_id: int,
    candidate_policy_id: str,
    pairs: Iterable[PairKey],
) -> tuple[ChannelHit, ...]:
    unique = tuple(
        sorted(set(pairs), key=lambda pair: (pair.chunk_version_id, pair.claim_id))
    )
    ranks: defaultdict[str, int] = defaultdict(int)
    hits: list[ChannelHit] = []
    for pair in unique:
        ranks[pair.chunk_version_id] += 1
        rank = ranks[pair.chunk_version_id]
        hits.append(
            ChannelHit(
                epoch_id=epoch_id,
                pair=pair,
                candidate_policy_id=candidate_policy_id,
                channel=AdmissionChannel.LINEAGE,
                rank=rank,
                score=None,
                channel_artifact_hash=stable_m4_digest(
                    "m4-lineage-channel-hit-v1",
                    candidate_policy_id,
                    str(epoch_id),
                    pair.chunk_version_id,
                    pair.claim_id,
                    str(rank),
                ),
            )
        )
    return tuple(hits)


def _canonical_hits(hits: Iterable[ChannelHit]) -> tuple[ChannelHit, ...]:
    by_key: dict[tuple[int, PairKey, str, AdmissionChannel], ChannelHit] = {}
    for hit in hits:
        key = (hit.epoch_id, hit.pair, hit.candidate_policy_id, hit.channel)
        existing = by_key.get(key)
        if existing is not None and existing != hit:
            raise ArtifactConflictError("channel-hit key has conflicting payloads")
        by_key[key] = hit
    return tuple(
        sorted(
            by_key.values(),
            key=lambda hit: (
                hit.pair.chunk_version_id,
                hit.channel.value,
                hit.rank,
                hit.pair.claim_id,
            ),
        )
    )


def _validate_channel_ranks(hits: tuple[ChannelHit, ...]) -> None:
    grouped: defaultdict[tuple[str, AdmissionChannel], list[ChannelHit]] = (
        defaultdict(list)
    )
    for hit in hits:
        grouped[(hit.pair.chunk_version_id, hit.channel)].append(hit)
    for group in grouped.values():
        ordered = sorted(group, key=lambda hit: (hit.rank, hit.pair.claim_id))
        if tuple(hit.rank for hit in ordered) != tuple(range(1, len(ordered) + 1)):
            raise ValidationError("channel ranks must be contiguous and one-based")


def fuse_admission_channels(
    *,
    inserted_chunk_ids: tuple[str, ...],
    manifest: CandidatePolicyManifest,
    vector_hits: Iterable[ChannelHit],
    lexical_hits: Iterable[ChannelHit],
    lineage_hits: Iterable[ChannelHit] = (),
) -> AdmissionResult:
    """Round-robin VECTOR then LEXICAL; add all mandatory lineage afterward."""
    if inserted_chunk_ids != tuple(sorted(set(inserted_chunk_ids))):
        raise ValidationError("inserted_chunk_ids must be sorted and unique")
    all_hits = _canonical_hits((*vector_hits, *lexical_hits, *lineage_hits))
    _validate_channel_ranks(all_hits)
    epochs = {hit.epoch_id for hit in all_hits}
    if len(epochs) > 1:
        raise ValidationError("one admission result cannot mix epochs")
    if manifest.fusion_version != "rank-interleave-v1":
        raise ValidationError("unsupported deterministic fusion version")
    inserted = set(inserted_chunk_ids)
    for hit in all_hits:
        if hit.candidate_policy_id != manifest.policy_id:
            raise ValidationError("channel hit uses a different candidate policy")
        if hit.pair.chunk_version_id not in inserted:
            raise ValidationError("channel hit targets a non-inserted chunk")
        if hit.channel not in {*_APPROXIMATE_CHANNELS, AdmissionChannel.LINEAGE}:
            raise ValidationError("CORE fusion received an unsupported channel")
        if (
            hit.channel is AdmissionChannel.LINEAGE
            and not manifest.lineage_safety_override
        ):
            raise ValidationError("lineage hit supplied while safety override is off")

    hits_by_chunk_channel: defaultdict[
        tuple[str, AdmissionChannel], list[ChannelHit]
    ] = defaultdict(list)
    reasons_by_pair: defaultdict[PairKey, set[AdmissionChannel]] = defaultdict(set)
    for hit in all_hits:
        hits_by_chunk_channel[(hit.pair.chunk_version_id, hit.channel)].append(hit)
        reasons_by_pair[hit.pair].add(hit.channel)
    for group in hits_by_chunk_channel.values():
        group.sort(key=lambda hit: (hit.rank, hit.pair.claim_id))

    approximate_rank: dict[PairKey, int] = {}
    lineage_pairs: set[PairKey] = {
        hit.pair for hit in all_hits if hit.channel is AdmissionChannel.LINEAGE
    }
    for chunk_id in inserted_chunk_ids:
        positions = {channel: 0 for channel in _APPROXIMATE_CHANNELS}
        selected_for_chunk = 0
        while selected_for_chunk < manifest.approximate_cap_per_inserted_chunk:
            advanced = False
            for channel in _APPROXIMATE_CHANNELS:
                group = hits_by_chunk_channel[(chunk_id, channel)]
                position = positions[channel]
                if position >= len(group):
                    continue
                hit = group[position]
                positions[channel] += 1
                advanced = True
                if hit.pair in approximate_rank:
                    continue
                selected_for_chunk += 1
                approximate_rank[hit.pair] = selected_for_chunk
                if selected_for_chunk == manifest.approximate_cap_per_inserted_chunk:
                    break
            if not advanced:
                break

    approximate_pairs = set(approximate_rank)
    lineage_excess = lineage_pairs - approximate_pairs
    fused_rank = dict(approximate_rank)
    for chunk_id in inserted_chunk_ids:
        next_rank = max(
            (
                rank
                for pair, rank in fused_rank.items()
                if pair.chunk_version_id == chunk_id
            ),
            default=0,
        )
        for pair in sorted(
            (pair for pair in lineage_excess if pair.chunk_version_id == chunk_id),
            key=lambda item: item.claim_id,
        ):
            next_rank += 1
            fused_rank[pair] = next_rank

    admitted_pairs = tuple(
        sorted(
            (
                AdmittedPair(
                    epoch_id=next(
                        hit.epoch_id for hit in all_hits if hit.pair == pair
                    ),
                    pair=pair,
                    candidate_policy_id=manifest.policy_id,
                    fused_rank=rank,
                    reasons=tuple(sorted(reasons_by_pair[pair], key=str)),
                    mandatory_lineage=AdmissionChannel.LINEAGE
                    in reasons_by_pair[pair],
                )
                for pair, rank in fused_rank.items()
            ),
            key=lambda item: (
                item.pair.chunk_version_id,
                item.fused_rank,
                item.pair.claim_id,
            ),
        )
    )
    accounting = AdmissionAccounting(
        inserted_chunk_count=len(inserted_chunk_ids),
        approximate_cap_per_inserted_chunk=(
            manifest.approximate_cap_per_inserted_chunk
        ),
        approximate_pair_count=len(approximate_pairs),
        mandatory_lineage_pair_count=len(lineage_pairs),
        lineage_excess_pair_count=len(lineage_excess),
        admitted_pair_count=len(admitted_pairs),
        verifier_call_upper_bound=(
            len(inserted_chunk_ids) * manifest.approximate_cap_per_inserted_chunk
            + len(lineage_excess)
        ),
    )
    return AdmissionResult(all_hits, admitted_pairs, accounting)
