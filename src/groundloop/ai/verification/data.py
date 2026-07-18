"""Leakage-safe preparation primitives for SciFact and WiCE-derived pairs."""

from __future__ import annotations

import hashlib
import json
import random
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from groundloop.ai.verification.constants import Label
from groundloop.errors import ValidationError

_WHITESPACE = re.compile(r"\s+")
_SPLIT_PRIORITY = {"train": 0, "development": 1, "test": 2, "transfer": 3}


def normalize_pair_text(text: str) -> str:
    return _WHITESPACE.sub(" ", text.strip()).casefold()


@dataclass(frozen=True, slots=True)
class VerificationExample:
    example_id: str
    source: str
    source_revision: str
    split: str
    claim_group_id: str
    claim: str
    evidence: str
    label: Label
    construction: str

    def __post_init__(self) -> None:
        if self.split not in _SPLIT_PRIORITY:
            raise ValidationError(f"unsupported split: {self.split}")
        for name, value in (
            ("example_id", self.example_id),
            ("source", self.source),
            ("source_revision", self.source_revision),
            ("claim_group_id", self.claim_group_id),
            ("claim", self.claim),
            ("evidence", self.evidence),
            ("construction", self.construction),
        ):
            if not value.strip():
                raise ValidationError(f"{name} must be non-empty")

    def to_json(self) -> dict[str, object]:
        payload = asdict(self)
        payload["label"] = self.label.name.casefold()
        return payload

    @classmethod
    def from_json(cls, payload: Mapping[str, object]) -> VerificationExample:
        try:
            label = Label[str(payload["label"]).upper()]
            return cls(
                example_id=str(payload["example_id"]),
                source=str(payload["source"]),
                source_revision=str(payload["source_revision"]),
                split=str(payload["split"]),
                claim_group_id=str(payload["claim_group_id"]),
                claim=str(payload["claim"]),
                evidence=str(payload["evidence"]),
                label=label,
                construction=str(payload["construction"]),
            )
        except (KeyError, ValueError) as error:
            raise ValidationError("malformed verification example") from error


@dataclass(frozen=True, slots=True)
class DeduplicationReport:
    input_examples: int
    output_examples: int
    within_split_pair_duplicates: int
    cross_split_pair_duplicates: int
    cross_split_claim_groups_removed: int


def map_scifact_label(label: str) -> Label:
    normalized = label.strip().upper()
    if normalized == "SUPPORT":
        return Label.SUPPORT
    if normalized in {"CONTRADICT", "CONTRADICTS"}:
        return Label.REFUTE
    raise ValidationError(f"unsupported SciFact label: {label!r}")


def map_wice_label(label: str) -> Label:
    normalized = label.strip().casefold()
    if normalized == "supported":
        return Label.SUPPORT
    if normalized in {"partially_supported", "not_supported"}:
        return Label.NEUTRAL
    raise ValidationError(f"unsupported WiCE label: {label!r}")


def deterministic_neutral_document(
    *,
    claim_id: str,
    forbidden_doc_ids: set[str],
    split_doc_ids: Sequence[str],
    seed: int,
) -> str:
    candidates = sorted(set(split_doc_ids) - forbidden_doc_ids)
    if not candidates:
        raise ValidationError("no split-local non-evidence document is available")
    derived_seed = int.from_bytes(
        hashlib.sha256(f"{seed}:{claim_id}".encode()).digest()[:8], "big"
    )
    return candidates[random.Random(derived_seed).randrange(len(candidates))]


def deduplicate_examples(
    examples: Iterable[VerificationExample],
) -> tuple[tuple[VerificationExample, ...], DeduplicationReport]:
    """Keep claim groups in one split, protecting test over dev over train."""
    materialized = tuple(examples)
    chosen_split: dict[str, str] = {}
    for example in materialized:
        normalized_claim = normalize_pair_text(example.claim)
        current = chosen_split.get(normalized_claim)
        if current is None or _SPLIT_PRIORITY[example.split] > _SPLIT_PRIORITY[current]:
            chosen_split[normalized_claim] = example.split

    group_removed = 0
    pair_seen: dict[tuple[str, str], str] = {}
    kept_by_pair: dict[tuple[str, str], VerificationExample] = {}
    within = 0
    cross_pair = 0
    ordered = sorted(
        materialized,
        key=lambda item: (
            -_SPLIT_PRIORITY[item.split],
            item.source,
            item.claim_group_id,
            item.example_id,
        ),
    )
    for example in ordered:
        claim_key = normalize_pair_text(example.claim)
        if chosen_split[claim_key] != example.split:
            group_removed += 1
            continue
        pair_key = (claim_key, normalize_pair_text(example.evidence))
        prior_split = pair_seen.get(pair_key)
        if prior_split is not None:
            if prior_split == example.split:
                within += 1
            else:
                cross_pair += 1
            continue
        pair_seen[pair_key] = example.split
        kept_by_pair[pair_key] = example

    kept = tuple(
        sorted(
            kept_by_pair.values(),
            key=lambda item: (
                _SPLIT_PRIORITY[item.split],
                item.source,
                item.claim_group_id,
                item.example_id,
            ),
        )
    )
    # Defensive assertion: no normalized claim is now distributed across splits.
    split_by_claim: defaultdict[str, set[str]] = defaultdict(set)
    for example in kept:
        split_by_claim[normalize_pair_text(example.claim)].add(example.split)
    if any(len(splits) != 1 for splits in split_by_claim.values()):
        raise AssertionError("claim-group leakage survived deduplication")
    return kept, DeduplicationReport(
        input_examples=len(materialized),
        output_examples=len(kept),
        within_split_pair_duplicates=within,
        cross_split_pair_duplicates=cross_pair,
        cross_split_claim_groups_removed=group_removed,
    )


def read_jsonl(path: Path) -> tuple[dict[str, object], ...]:
    if not path.is_file():
        raise FileNotFoundError(f"required JSONL artifact is absent: {path}")
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValidationError(f"{path}:{line_number} is not a JSON object")
        rows.append(payload)
    return tuple(rows)


def write_examples(path: Path, examples: Sequence[VerificationExample]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(
        json.dumps(example.to_json(), sort_keys=True, separators=(",", ":")) + "\n"
        for example in examples
    )
    path.write_text(content, encoding="utf-8")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def dataset_summary(examples: Sequence[VerificationExample]) -> dict[str, object]:
    counts = Counter(
        (example.split, example.label.name.casefold()) for example in examples
    )
    return {
        "examples": len(examples),
        "claim_groups": len(
            {normalize_pair_text(example.claim) for example in examples}
        ),
        "counts": {
            split: {
                label.name.casefold(): counts[(split, label.name.casefold())]
                for label in Label
            }
            for split in _SPLIT_PRIORITY
            if any(example.split == split for example in examples)
        },
    }
