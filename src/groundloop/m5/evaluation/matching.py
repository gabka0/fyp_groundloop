"""Small independent matching routines used only by controlled evaluation.

These routines intentionally do not import the optimized M5 matching kernel.
They are an evaluation-side oracle for graphs with at most eight left nodes.
"""

from __future__ import annotations

from functools import cache

from groundloop.errors import ValidationError


def _canonical_edges(
    edges_by_ordinal: tuple[tuple[str, ...], ...],
) -> tuple[tuple[str, ...], ...]:
    if not 1 <= len(edges_by_ordinal) <= 8:
        raise ValidationError("evaluation matching requires one to eight requirements")
    result: list[tuple[str, ...]] = []
    for edges in edges_by_ordinal:
        canonical = tuple(sorted(set(edges)))
        if canonical != edges:
            raise ValidationError("matching edges must be unique and sorted")
        result.append(canonical)
    return tuple(result)


def maximum_matching(
    edges_by_ordinal: tuple[tuple[str, ...], ...],
) -> tuple[int, tuple[str | None, ...]]:
    """Return an exact deterministic maximum matching and ordinal assignment."""

    edges = _canonical_edges(edges_by_ordinal)
    owner_by_hash: dict[str, int] = {}
    assignment: list[str | None] = [None] * len(edges)

    def augment(ordinal: int, seen: set[str]) -> bool:
        for text_hash in edges[ordinal]:
            if text_hash in seen:
                continue
            seen.add(text_hash)
            previous = owner_by_hash.get(text_hash)
            if previous is None or augment(previous, seen):
                owner_by_hash[text_hash] = ordinal
                assignment[ordinal] = text_hash
                return True
        return False

    size = 0
    for ordinal in range(len(edges)):
        if augment(ordinal, set()):
            size += 1
    return size, tuple(assignment)


def perfect_matching_count(edges_by_ordinal: tuple[tuple[str, ...], ...]) -> int:
    """Count exact distinct-representative assignments for a bounded graph."""

    edges = _canonical_edges(edges_by_ordinal)
    all_hashes = tuple(
        sorted({value for requirement in edges for value in requirement})
    )
    bit_by_hash = {value: 1 << index for index, value in enumerate(all_hashes)}

    @cache
    def visit(ordinal: int, used_mask: int) -> int:
        if ordinal == len(edges):
            return 1
        count = 0
        for text_hash in edges[ordinal]:
            bit = bit_by_hash[text_hash]
            if not used_mask & bit:
                count += visit(ordinal + 1, used_mask | bit)
        return count

    return visit(0, 0)


def hall_complete(edges_by_ordinal: tuple[tuple[str, ...], ...]) -> bool:
    size, _ = maximum_matching(edges_by_ordinal)
    return size == len(edges_by_ordinal)


__all__ = ["hall_complete", "maximum_matching", "perfect_matching_count"]
