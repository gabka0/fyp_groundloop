"""Controlled dynamic M4 workload identities, independent of runtime logic."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairKey, UpdateKind, stable_m4_digest

_HEX = frozenset("0123456789abcdef")


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _require_sorted_unique(name: str, values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValidationError(f"{name} must be sorted and unique")
    if any(not value.strip() for value in values):
        raise ValidationError(f"{name} must contain only non-empty values")


class WorkloadSplit(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"


@dataclass(frozen=True, slots=True)
class ControlledEventSpec:
    event_id: str
    event_index: int
    update_kind: UpdateKind
    corpus_snapshot_before_hash: str
    corpus_snapshot_after_hash: str
    inserted_chunk_version_ids: tuple[str, ...]
    deactivated_chunk_version_ids: tuple[str, ...]
    registered_claim_ids: tuple[str, ...]
    registered_answer_ids: tuple[str, ...]
    deliberate_miss_pairs: tuple[PairKey, ...] = ()

    def __post_init__(self) -> None:
        _require_text("event_id", self.event_id)
        if type(self.event_index) is not int or self.event_index < 0:
            raise ValidationError("event_index must be a nonnegative integer")
        if not isinstance(self.update_kind, UpdateKind):
            raise ValidationError("update_kind must be a frozen M4 update kind")
        _require_sha256(
            "corpus_snapshot_before_hash", self.corpus_snapshot_before_hash
        )
        _require_sha256(
            "corpus_snapshot_after_hash", self.corpus_snapshot_after_hash
        )
        if self.corpus_snapshot_before_hash == self.corpus_snapshot_after_hash:
            raise ValidationError("controlled event must advance the corpus snapshot")
        for name, values in (
            ("inserted_chunk_version_ids", self.inserted_chunk_version_ids),
            ("deactivated_chunk_version_ids", self.deactivated_chunk_version_ids),
            ("registered_claim_ids", self.registered_claim_ids),
            ("registered_answer_ids", self.registered_answer_ids),
        ):
            _require_sorted_unique(name, values)
        if not self.registered_claim_ids or not self.registered_answer_ids:
            raise ValidationError("controlled event requires claims and answers")
        if set(self.inserted_chunk_version_ids) & set(
            self.deactivated_chunk_version_ids
        ):
            raise ValidationError("inserted and deactivated chunks must be disjoint")
        expected_shape = {
            UpdateKind.INSERT: (True, False),
            UpdateKind.DELETE: (False, True),
            UpdateKind.REPLACE: (True, True),
        }[self.update_kind]
        actual_shape = (
            bool(self.inserted_chunk_version_ids),
            bool(self.deactivated_chunk_version_ids),
        )
        if actual_shape != expected_shape:
            raise ValidationError("chunk changes conflict with update kind")
        if self.deliberate_miss_pairs != tuple(
            sorted(set(self.deliberate_miss_pairs))
        ):
            raise ValidationError("deliberate_miss_pairs must be sorted and unique")
        claim_ids = set(self.registered_claim_ids)
        inserted_ids = set(self.inserted_chunk_version_ids)
        if any(
            pair.claim_id not in claim_ids
            or pair.chunk_version_id not in inserted_ids
            for pair in self.deliberate_miss_pairs
        ):
            raise ValidationError("deliberate miss pair lies outside inserted domain")

    @property
    def manifest_hash(self) -> str:
        parts = [
            "m4-controlled-event-v1",
            self.event_id,
            str(self.event_index),
            self.update_kind.value,
            self.corpus_snapshot_before_hash,
            self.corpus_snapshot_after_hash,
            *self.inserted_chunk_version_ids,
            "deactivated",
            *self.deactivated_chunk_version_ids,
            "claims",
            *self.registered_claim_ids,
            "answers",
            *self.registered_answer_ids,
            "misses",
        ]
        for pair in self.deliberate_miss_pairs:
            parts.extend((pair.claim_id, pair.chunk_version_id))
        return stable_m4_digest(*parts)


@dataclass(frozen=True, slots=True)
class ControlledHistorySpec:
    history_id: str
    split: WorkloadSplit
    split_component_id: str
    history_seed: int
    initial_corpus_snapshot_hash: str
    lineage_component_ids: tuple[str, ...]
    claim_family_ids: tuple[str, ...]
    normalized_content_hashes: tuple[str, ...]
    events: tuple[ControlledEventSpec, ...]

    def __post_init__(self) -> None:
        for name, value in (
            ("history_id", self.history_id),
            ("split_component_id", self.split_component_id),
        ):
            _require_text(name, value)
        if not isinstance(self.split, WorkloadSplit):
            raise ValidationError("split must be development or test")
        if type(self.history_seed) is not int or self.history_seed < 0:
            raise ValidationError("history_seed must be a nonnegative integer")
        _require_sha256(
            "initial_corpus_snapshot_hash", self.initial_corpus_snapshot_hash
        )
        for name, values in (
            ("lineage_component_ids", self.lineage_component_ids),
            ("claim_family_ids", self.claim_family_ids),
            ("normalized_content_hashes", self.normalized_content_hashes),
        ):
            _require_sorted_unique(name, values)
        if not self.lineage_component_ids or not self.claim_family_ids:
            raise ValidationError(
                "history requires lineage and claim-family identities"
            )
        for content_hash in self.normalized_content_hashes:
            _require_sha256("normalized_content_hash", content_hash)
        if not self.events:
            raise ValidationError("controlled history requires at least one event")
        event_ids: set[str] = set()
        expected_before = self.initial_corpus_snapshot_hash
        for expected_index, event in enumerate(self.events):
            if event.event_index != expected_index:
                raise ValidationError("history event indexes must be contiguous")
            if event.event_id in event_ids:
                raise ValidationError("history contains a duplicate event ID")
            if event.corpus_snapshot_before_hash != expected_before:
                raise ValidationError("history corpus snapshot chain is broken")
            event_ids.add(event.event_id)
            expected_before = event.corpus_snapshot_after_hash

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-history-v1",
            self.history_id,
            self.split.value,
            self.split_component_id,
            str(self.history_seed),
            self.initial_corpus_snapshot_hash,
            *self.lineage_component_ids,
            "claim-families",
            *self.claim_family_ids,
            "content",
            *self.normalized_content_hashes,
            "events",
            *(event.manifest_hash for event in self.events),
        )


@dataclass(frozen=True, slots=True)
class ControlledWorkload:
    schema_version: str
    dataset_version: str
    generator_seed: int
    split_seed: int
    histories: tuple[ControlledHistorySpec, ...]

    def __post_init__(self) -> None:
        if self.schema_version != "m4-controlled-workload-v1":
            raise ValidationError("unsupported controlled workload schema")
        _require_text("dataset_version", self.dataset_version)
        for name, seed in (
            ("generator_seed", self.generator_seed),
            ("split_seed", self.split_seed),
        ):
            if type(seed) is not int or seed < 0:
                raise ValidationError(f"{name} must be a nonnegative integer")
        if not self.histories:
            raise ValidationError("controlled workload requires histories")
        history_ids = tuple(history.history_id for history in self.histories)
        if len(history_ids) != len(set(history_ids)):
            raise ValidationError("workload history IDs must be globally unique")
        if self.histories != tuple(
            sorted(self.histories, key=lambda history: history.history_id)
        ):
            raise ValidationError("workload histories must use canonical ID order")

        event_ids: set[str] = set()
        identity_owner: dict[tuple[str, str], tuple[str, WorkloadSplit]] = {}
        for history in self.histories:
            for event in history.events:
                if event.event_id in event_ids:
                    raise ValidationError("workload event IDs must be globally unique")
                event_ids.add(event.event_id)
            identities: tuple[tuple[str, tuple[str, ...]], ...] = (
                ("split-component", (history.split_component_id,)),
                ("lineage", history.lineage_component_ids),
                ("claim-family", history.claim_family_ids),
                ("content", history.normalized_content_hashes),
                (
                    "claim",
                    tuple(
                        sorted(
                            {
                                claim_id
                                for event in history.events
                                for claim_id in event.registered_claim_ids
                            }
                        )
                    ),
                ),
                (
                    "answer",
                    tuple(
                        sorted(
                            {
                                answer_id
                                for event in history.events
                                for answer_id in event.registered_answer_ids
                            }
                        )
                    ),
                ),
                (
                    "chunk",
                    tuple(
                        sorted(
                            {
                                chunk_id
                                for event in history.events
                                for chunk_id in (
                                    *event.inserted_chunk_version_ids,
                                    *event.deactivated_chunk_version_ids,
                                )
                            }
                        )
                    ),
                ),
            )
            for kind, values in identities:
                for value in values:
                    key = (kind, value)
                    owner = identity_owner.get(key)
                    if owner is not None and owner[0] != history.history_id:
                        if owner[1] is not history.split:
                            raise ValidationError(
                                f"{kind} identity leaks across development/test"
                            )
                        raise ValidationError(
                            f"{kind} identity links independent histories"
                        )
                    identity_owner[key] = (history.history_id, history.split)

    @property
    def seed_manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-seeds-v1",
            str(self.generator_seed),
            str(self.split_seed),
            *(
                part
                for history in self.histories
                for part in (history.history_id, str(history.history_seed))
            ),
        )

    @property
    def split_manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-splits-v1",
            *(
                part
                for history in self.histories
                for part in (
                    history.history_id,
                    history.split.value,
                    history.split_component_id,
                    *history.lineage_component_ids,
                    *history.claim_family_ids,
                    *history.normalized_content_hashes,
                    history.manifest_hash,
                )
            ),
        )

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-workload-v1",
            self.schema_version,
            self.dataset_version,
            self.seed_manifest_hash,
            self.split_manifest_hash,
            *(history.manifest_hash for history in self.histories),
        )

    def histories_for_split(
        self, split: WorkloadSplit
    ) -> tuple[ControlledHistorySpec, ...]:
        return tuple(history for history in self.histories if history.split is split)

    def to_canonical_json(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "dataset_version": self.dataset_version,
            "generator_seed": self.generator_seed,
            "split_seed": self.split_seed,
            "seed_manifest_hash": self.seed_manifest_hash,
            "split_manifest_hash": self.split_manifest_hash,
            "manifest_hash": self.manifest_hash,
            "histories": [
                {
                    "history_id": history.history_id,
                    "split": history.split.value,
                    "split_component_id": history.split_component_id,
                    "history_seed": history.history_seed,
                    "initial_corpus_snapshot_hash": (
                        history.initial_corpus_snapshot_hash
                    ),
                    "lineage_component_ids": list(history.lineage_component_ids),
                    "claim_family_ids": list(history.claim_family_ids),
                    "normalized_content_hashes": list(
                        history.normalized_content_hashes
                    ),
                    "manifest_hash": history.manifest_hash,
                    "events": [
                        {
                            "event_id": event.event_id,
                            "event_index": event.event_index,
                            "update_kind": event.update_kind.value,
                            "corpus_snapshot_before_hash": (
                                event.corpus_snapshot_before_hash
                            ),
                            "corpus_snapshot_after_hash": (
                                event.corpus_snapshot_after_hash
                            ),
                            "inserted_chunk_version_ids": list(
                                event.inserted_chunk_version_ids
                            ),
                            "deactivated_chunk_version_ids": list(
                                event.deactivated_chunk_version_ids
                            ),
                            "registered_claim_ids": list(event.registered_claim_ids),
                            "registered_answer_ids": list(
                                event.registered_answer_ids
                            ),
                            "deliberate_miss_pairs": [
                                {
                                    "claim_id": pair.claim_id,
                                    "chunk_version_id": pair.chunk_version_id,
                                }
                                for pair in event.deliberate_miss_pairs
                            ],
                            "manifest_hash": event.manifest_hash,
                        }
                        for event in history.events
                    ],
                }
                for history in self.histories
            ],
        }
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )


def build_controlled_dynamic_workload_v1() -> ControlledWorkload:
    """Build four independent insert/replace/delete histories deterministically."""
    histories: list[ControlledHistorySpec] = []
    splits = (
        WorkloadSplit.DEVELOPMENT,
        WorkloadSplit.DEVELOPMENT,
        WorkloadSplit.TEST,
        WorkloadSplit.TEST,
    )
    for ordinal, split in enumerate(splits):
        prefix = f"{split.value}-h{ordinal % 2 + 1}"
        claim_ids = (f"{prefix}-c-decoy", f"{prefix}-c-missed")
        answer_ids = (f"{prefix}-answer",)
        chunk_v1 = f"{prefix}-chunk-v1"
        chunk_v2 = f"{prefix}-chunk-v2"
        snapshots = tuple(
            stable_m4_digest("controlled-snapshot", prefix, str(index))
            for index in range(4)
        )
        events = (
            ControlledEventSpec(
                event_id=f"{prefix}-insert",
                event_index=0,
                update_kind=UpdateKind.INSERT,
                corpus_snapshot_before_hash=snapshots[0],
                corpus_snapshot_after_hash=snapshots[1],
                inserted_chunk_version_ids=(chunk_v1,),
                deactivated_chunk_version_ids=(),
                registered_claim_ids=claim_ids,
                registered_answer_ids=answer_ids,
                deliberate_miss_pairs=(PairKey(claim_ids[1], chunk_v1),),
            ),
            ControlledEventSpec(
                event_id=f"{prefix}-replace",
                event_index=1,
                update_kind=UpdateKind.REPLACE,
                corpus_snapshot_before_hash=snapshots[1],
                corpus_snapshot_after_hash=snapshots[2],
                inserted_chunk_version_ids=(chunk_v2,),
                deactivated_chunk_version_ids=(chunk_v1,),
                registered_claim_ids=claim_ids,
                registered_answer_ids=answer_ids,
            ),
            ControlledEventSpec(
                event_id=f"{prefix}-delete",
                event_index=2,
                update_kind=UpdateKind.DELETE,
                corpus_snapshot_before_hash=snapshots[2],
                corpus_snapshot_after_hash=snapshots[3],
                inserted_chunk_version_ids=(),
                deactivated_chunk_version_ids=(chunk_v2,),
                registered_claim_ids=claim_ids,
                registered_answer_ids=answer_ids,
            ),
        )
        histories.append(
            ControlledHistorySpec(
                history_id=prefix,
                split=split,
                split_component_id=f"{prefix}-split-component",
                history_seed=2026071900 + ordinal,
                initial_corpus_snapshot_hash=snapshots[0],
                lineage_component_ids=(f"{prefix}-lineage",),
                claim_family_ids=(f"{prefix}-claim-family",),
                normalized_content_hashes=tuple(
                    sorted(
                        (
                            stable_m4_digest("controlled-content", prefix, "v1"),
                            stable_m4_digest("controlled-content", prefix, "v2"),
                        )
                    )
                ),
                events=events,
            )
        )
    return ControlledWorkload(
        schema_version="m4-controlled-workload-v1",
        dataset_version="m4-controlled-dynamic-v1",
        generator_seed=20260719,
        split_seed=20260720,
        histories=tuple(sorted(histories, key=lambda history: history.history_id)),
    )
