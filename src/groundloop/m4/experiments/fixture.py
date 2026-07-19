"""Small frozen table-driven fixture for the one-command M4.6 run."""

from __future__ import annotations

from dataclasses import dataclass

from groundloop.domain import VerificationLabel
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    JudgmentSourceKind,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.experiments.contracts import RankedChannelCandidate
from groundloop.m4.oracles import (
    ControlledWorkload,
    build_controlled_dynamic_workload_v1,
)
from groundloop.m4.oracles.identity import judgment_digest

_HEX = frozenset("0123456789abcdef")


@dataclass(frozen=True, slots=True, order=True)
class FixtureClaim:
    claim_id: str
    answer_version_id: str
    required: bool

    def __post_init__(self) -> None:
        if not self.claim_id.strip() or not self.answer_version_id.strip():
            raise ValidationError("fixture claim identities must be non-empty")


@dataclass(frozen=True, slots=True)
class FixtureHistory:
    history_id: str
    claims: tuple[FixtureClaim, ...]
    chunk_text_hashes: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.history_id.strip():
            raise ValidationError("fixture history ID must be non-empty")
        if self.claims != tuple(sorted(self.claims)):
            raise ValidationError("fixture claims must use canonical order")
        claim_ids = tuple(claim.claim_id for claim in self.claims)
        if len(claim_ids) != len(set(claim_ids)):
            raise ValidationError("fixture history contains duplicate claims")
        if not any(claim.required for claim in self.claims):
            raise ValidationError("fixture history requires a required claim")
        if self.chunk_text_hashes != tuple(sorted(self.chunk_text_hashes)):
            raise ValidationError("fixture chunk hashes must use canonical order")
        chunk_ids = tuple(chunk_id for chunk_id, _ in self.chunk_text_hashes)
        if len(chunk_ids) != len(set(chunk_ids)):
            raise ValidationError("fixture history contains duplicate chunks")
        if any(
            len(text_hash) != 64
            or any(character not in _HEX for character in text_hash)
            for _, text_hash in self.chunk_text_hashes
        ):
            raise ValidationError("fixture chunk text hashes must be SHA-256")


@dataclass(frozen=True, slots=True)
class FixtureEvent:
    event_id: str
    judgments: tuple[PairJudgment, ...]
    candidates: tuple[RankedChannelCandidate, ...]

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise ValidationError("fixture event ID must be non-empty")
        pairs = tuple(judgment.pair for judgment in self.judgments)
        if pairs != tuple(sorted(set(pairs))):
            raise ValidationError("fixture judgments must be pair-sorted and unique")
        if self.candidates != tuple(sorted(set(self.candidates))):
            raise ValidationError("fixture candidates must be sorted and unique")
        if not {candidate.pair for candidate in self.candidates} <= set(pairs):
            raise ValidationError("fixture candidate lacks a judgment-table row")
        grouped: dict[tuple[str, AdmissionChannel], list[int]] = {}
        for candidate in self.candidates:
            key = (candidate.pair.chunk_version_id, candidate.channel)
            grouped.setdefault(key, []).append(candidate.rank)
        if any(
            sorted(ranks) != list(range(1, len(ranks) + 1))
            for ranks in grouped.values()
        ):
            raise ValidationError("fixture candidate ranks must be contiguous")


@dataclass(frozen=True, slots=True)
class FrozenControlledFixture:
    schema_version: str
    workload: ControlledWorkload
    histories: tuple[FixtureHistory, ...]
    events: tuple[FixtureEvent, ...]

    def __post_init__(self) -> None:
        if self.schema_version != "m4-controlled-evaluation-fixture-v1":
            raise ValidationError("unsupported controlled fixture schema")
        if self.histories != tuple(
            sorted(self.histories, key=lambda item: item.history_id)
        ):
            raise ValidationError("fixture histories must use canonical order")
        if self.events != tuple(sorted(self.events, key=lambda item: item.event_id)):
            raise ValidationError("fixture events must use canonical order")
        workload_history_ids = {
            history.history_id for history in self.workload.histories
        }
        if {history.history_id for history in self.histories} != workload_history_ids:
            raise ValidationError("fixture histories differ from workload histories")
        workload_event_ids = {
            event.event_id
            for history in self.workload.histories
            for event in history.events
        }
        if {event.event_id for event in self.events} != workload_event_ids:
            raise ValidationError("fixture events differ from workload events")

    @property
    def manifest_hash(self) -> str:
        parts = [
            "m4-controlled-evaluation-fixture-v1",
            self.schema_version,
            self.workload.manifest_hash,
        ]
        for history in self.histories:
            parts.extend((history.history_id, "claims"))
            for claim in history.claims:
                parts.extend(
                    (
                        claim.claim_id,
                        claim.answer_version_id,
                        "required" if claim.required else "optional",
                    )
                )
            parts.append("chunks")
            for chunk_id, text_hash in history.chunk_text_hashes:
                parts.extend((chunk_id, text_hash))
        for event in self.events:
            parts.extend((event.event_id, "judgments"))
            for judgment in event.judgments:
                parts.append(judgment_digest(judgment))
            parts.append("candidates")
            for candidate in event.candidates:
                parts.extend(
                    (
                        candidate.pair.claim_id,
                        candidate.pair.chunk_version_id,
                        candidate.channel.value,
                        str(candidate.rank),
                    )
                )
        return stable_m4_digest(*parts)


def _judgment(
    *,
    pair: PairKey,
    label: VerificationLabel,
    split_id: str,
) -> PairJudgment:
    scores = {
        VerificationLabel.SUPPORT: (0.90, 0.05, 0.05),
        VerificationLabel.REFUTE: (0.05, 0.90, 0.05),
        VerificationLabel.NEUTRAL: (0.05, 0.05, 0.90),
    }[label]
    return PairJudgment(
        pair=pair,
        source_kind=JudgmentSourceKind.MODEL,
        source_artifact_id="controlled-table-verifier-v1",
        decision_policy_or_guideline_id="controlled-decision-v1",
        derived_label=label,
        input_hash=stable_m4_digest(
            "m4-controlled-judgment-input-v1",
            pair.claim_id,
            pair.chunk_version_id,
        ),
        split_id=split_id,
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
    )


def _ranked(
    chunk_id: str,
    decoy_claim: str,
    required_claim: str,
    *,
    second_version: bool,
) -> tuple[RankedChannelCandidate, ...]:
    decoy = PairKey(decoy_claim, chunk_id)
    required = PairKey(required_claim, chunk_id)
    if second_version:
        rankings = (
            (required, AdmissionChannel.VECTOR, 1),
            (decoy, AdmissionChannel.VECTOR, 2),
            (decoy, AdmissionChannel.LEXICAL, 1),
            (required, AdmissionChannel.LEXICAL, 2),
            (required, AdmissionChannel.LINEAGE, 1),
            (decoy, AdmissionChannel.FRONTIER, 1),
        )
    else:
        rankings = (
            (decoy, AdmissionChannel.VECTOR, 1),
            (required, AdmissionChannel.VECTOR, 2),
            (required, AdmissionChannel.LEXICAL, 1),
            (decoy, AdmissionChannel.LEXICAL, 2),
            (decoy, AdmissionChannel.LINEAGE, 1),
            (required, AdmissionChannel.FRONTIER, 1),
        )
    return tuple(
        sorted(
            RankedChannelCandidate(pair=pair, channel=channel, rank=rank)
            for pair, channel, rank in rankings
        )
    )


def build_frozen_controlled_fixture_v1() -> FrozenControlledFixture:
    """Create deterministic scores and channel ranks without SQL or models."""
    workload = build_controlled_dynamic_workload_v1()
    histories: list[FixtureHistory] = []
    events: list[FixtureEvent] = []
    for history in workload.histories:
        first_event, second_event, delete_event = history.events
        decoy_claim, required_claim = first_event.registered_claim_ids
        answer_id = first_event.registered_answer_ids[0]
        first_chunk = first_event.inserted_chunk_version_ids[0]
        second_chunk = second_event.inserted_chunk_version_ids[0]
        histories.append(
            FixtureHistory(
                history_id=history.history_id,
                claims=tuple(
                    sorted(
                        (
                            FixtureClaim(decoy_claim, answer_id, False),
                            FixtureClaim(required_claim, answer_id, True),
                        )
                    )
                ),
                chunk_text_hashes=tuple(
                    sorted(
                        (
                            (
                                first_chunk,
                                stable_m4_digest(
                                    "m4-controlled-chunk-text", first_chunk
                                ),
                            ),
                            (
                                second_chunk,
                                stable_m4_digest(
                                    "m4-controlled-chunk-text", second_chunk
                                ),
                            ),
                        )
                    )
                ),
            )
        )
        first_pairs = (
            _judgment(
                pair=PairKey(decoy_claim, first_chunk),
                label=VerificationLabel.NEUTRAL,
                split_id=history.split.value,
            ),
            _judgment(
                pair=PairKey(required_claim, first_chunk),
                label=VerificationLabel.SUPPORT,
                split_id=history.split.value,
            ),
        )
        second_pairs = (
            _judgment(
                pair=PairKey(decoy_claim, second_chunk),
                label=VerificationLabel.NEUTRAL,
                split_id=history.split.value,
            ),
            _judgment(
                pair=PairKey(required_claim, second_chunk),
                label=VerificationLabel.REFUTE,
                split_id=history.split.value,
            ),
        )
        events.extend(
            (
                FixtureEvent(
                    event_id=first_event.event_id,
                    judgments=tuple(sorted(first_pairs, key=lambda item: item.pair)),
                    candidates=_ranked(
                        first_chunk,
                        decoy_claim,
                        required_claim,
                        second_version=False,
                    ),
                ),
                FixtureEvent(
                    event_id=second_event.event_id,
                    judgments=tuple(sorted(second_pairs, key=lambda item: item.pair)),
                    candidates=_ranked(
                        second_chunk,
                        decoy_claim,
                        required_claim,
                        second_version=True,
                    ),
                ),
                FixtureEvent(
                    event_id=delete_event.event_id,
                    judgments=(),
                    candidates=(),
                ),
            )
        )
    return FrozenControlledFixture(
        schema_version="m4-controlled-evaluation-fixture-v1",
        workload=workload,
        histories=tuple(sorted(histories, key=lambda item: item.history_id)),
        events=tuple(sorted(events, key=lambda item: item.event_id)),
    )
