from __future__ import annotations

from itertools import product

from groundloop.m5.evaluation.matching import (
    maximum_matching,
    perfect_matching_count,
)


def _brute_force_counts(edges: tuple[tuple[str, ...], ...]) -> tuple[int, int]:
    maximum_size = 0
    perfect_count = 0

    def visit(ordinal: int, used: frozenset[str], matched: int) -> None:
        nonlocal maximum_size, perfect_count
        if ordinal == len(edges):
            maximum_size = max(maximum_size, matched)
            perfect_count += int(matched == len(edges))
            return
        visit(ordinal + 1, used, matched)
        for text_hash in edges[ordinal]:
            if text_hash not in used:
                visit(ordinal + 1, used | {text_hash}, matched + 1)

    visit(0, frozenset(), 0)
    return maximum_size, perfect_count


def test_evaluation_matching_is_exact_for_every_small_bipartite_graph() -> None:
    graphs_checked = 0
    for requirement_count in range(1, 5):
        for hash_count in range(1, 5):
            masks = range(1 << hash_count)
            for graph_masks in product(masks, repeat=requirement_count):
                edges = tuple(
                    tuple(
                        str(hash_ordinal)
                        for hash_ordinal in range(hash_count)
                        if mask & (1 << hash_ordinal)
                    )
                    for mask in graph_masks
                )
                expected_size, expected_perfect_count = _brute_force_counts(edges)
                size, assignment = maximum_matching(edges)

                assert size == expected_size
                assert perfect_matching_count(edges) == expected_perfect_count
                selected = tuple(value for value in assignment if value is not None)
                assert len(selected) == size
                assert len(selected) == len(set(selected))
                assert all(
                    value is None or value in edges[ordinal]
                    for ordinal, value in enumerate(assignment)
                )
                graphs_checked += 1

    assert graphs_checked == 74_954
