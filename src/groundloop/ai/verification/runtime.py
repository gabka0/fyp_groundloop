"""Shared explicit-command helpers; never invoked by ordinary tests."""

from __future__ import annotations

import json
import random
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

from groundloop.ai.verification.data import VerificationExample
from groundloop.errors import ValidationError


def load_examples(path: Path) -> tuple[VerificationExample, ...]:
    if not path.is_file():
        raise FileNotFoundError(
            f"prepared verifier split is absent: {path}; run prepare.py first"
        )
    examples: list[VerificationExample] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValidationError(f"{path}:{line_number} is not a JSON object")
        examples.append(VerificationExample.from_json(payload))
    if not examples:
        raise ValidationError(f"prepared verifier split is empty: {path}")
    return tuple(examples)


def group_bounded_sample(
    examples: Sequence[VerificationExample], *, maximum: int, seed: int
) -> tuple[VerificationExample, ...]:
    """Bound work without splitting examples derived from one claim."""
    if maximum <= 0:
        raise ValidationError("maximum example count must be positive")
    if len(examples) <= maximum:
        return tuple(examples)
    groups: defaultdict[str, list[VerificationExample]] = defaultdict(list)
    for example in examples:
        groups[example.claim_group_id].append(example)
    group_ids = sorted(groups)
    random.Random(seed).shuffle(group_ids)
    selected: list[VerificationExample] = []
    for group_id in group_ids:
        group = groups[group_id]
        if selected and len(selected) + len(group) > maximum:
            continue
        selected.extend(group)
        if len(selected) >= maximum:
            break
    if not selected:
        raise ValidationError("bounded sampling selected no complete claim group")
    return tuple(sorted(selected, key=lambda item: item.example_id))


def require_directory(path: Path, command: str) -> None:
    if not path.is_dir():
        raise FileNotFoundError(
            f"required artifact is absent: {path}; run {command} first"
        )
