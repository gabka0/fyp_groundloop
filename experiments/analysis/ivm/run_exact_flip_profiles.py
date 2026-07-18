#!/usr/bin/env python3
"""Emit reproducible candidate-visit profiles for the exact-flip index."""

from __future__ import annotations

import argparse
import json

from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.optimized.exact_flip import ExactFlipIndex

STAMP = ModelStamp("complexity-profile", "v1", "p1")


def _observation(
    observation_id: str, scores: tuple[float, float, float]
) -> SemanticObservation:
    return SemanticObservation(
        observation_id=observation_id,
        subject_kind=SubjectKind.CLAIM,
        subject_id="claim",
        chunk_version_id=f"chunk-{observation_id}",
        task_type="verify",
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
        producer=STAMP,
        input_hash=f"input-{observation_id}",
    )


def profile(size: int, sparse_flips: int) -> list[dict[str, int | str]]:
    if size <= 0 or not 0 <= sparse_flips <= size:
        raise ValueError("require size > 0 and 0 <= sparse_flips <= size")
    old = DecisionPolicy("old", 0.8, 0.8)
    new = DecisionPolicy("new", 0.6, 0.8)
    rows: list[dict[str, int | str]] = []
    for name, potential_count in (
        ("f_zero", 0),
        ("f_sparse", sparse_flips),
        ("f_dense", size),
    ):
        index = ExactFlipIndex()
        raw_interval_population = 0
        for item in range(size):
            if item < potential_count:
                scores = (0.7, 0.1, 0.2)
            else:
                # Its support score lies in the raw interval, but neutral
                # dominance makes this row unable to change under ts.
                scores = (0.7, 0.1, 0.95)
            observation = _observation(f"{name}-{item}", scores)
            index.add(observation)
            if 0.6 <= observation.support_score < 0.8:
                raw_interval_population += 1
        query = index.exact_flips(old, new)
        rows.append(
            {
                "profile": name,
                "E": size,
                "raw_interval_population": raw_interval_population,
                "visited_candidates": len(query.observation_ids),
                "f": potential_count,
                "searches": query.searches,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=10_000)
    parser.add_argument("--sparse-flips", type=int, default=10)
    args = parser.parse_args()
    print(json.dumps(profile(args.size, args.sparse_flips), sort_keys=True))


if __name__ == "__main__":
    main()
