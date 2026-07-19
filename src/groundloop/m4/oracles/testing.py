"""Deterministic score adapters for oracle tests and bounded fixtures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from groundloop.errors import ValidationError
from groundloop.m4.contracts import PairJudgment, PairKey


class PairJudge(Protocol):
    """Batch-oriented immutable pair-judgment boundary.

    Real-model adapters may implement this protocol in a later wave. Wave 1
    uses only :class:`DeterministicJudgmentTable`.
    """

    def judge_pairs(self, pairs: tuple[PairKey, ...]) -> tuple[PairJudgment, ...]:
        """Return exactly one judgment for every requested pair in order."""


@dataclass(frozen=True, slots=True)
class DeterministicJudgmentTable:
    """A complete immutable lookup table; missing pairs are hard failures."""

    judgments: tuple[PairJudgment, ...]

    def __post_init__(self) -> None:
        pairs = tuple(judgment.pair for judgment in self.judgments)
        if pairs != tuple(sorted(set(pairs))):
            raise ValidationError("judgment table pairs must be sorted and unique")

    def judge_pairs(self, pairs: tuple[PairKey, ...]) -> tuple[PairJudgment, ...]:
        if pairs != tuple(sorted(set(pairs))):
            raise ValidationError("requested pairs must be sorted and unique")
        by_pair = {judgment.pair: judgment for judgment in self.judgments}
        missing = tuple(pair for pair in pairs if pair not in by_pair)
        if missing:
            rendered = ", ".join(
                f"({pair.claim_id},{pair.chunk_version_id})" for pair in missing
            )
            raise ValidationError(f"deterministic judgment table misses: {rendered}")
        return tuple(by_pair[pair] for pair in pairs)
