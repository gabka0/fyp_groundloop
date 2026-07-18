from __future__ import annotations

import pytest

from groundloop.ai.verification.constants import Label
from groundloop.ai.verification.data import (
    VerificationExample,
    deduplicate_examples,
    deterministic_neutral_document,
    map_scifact_label,
    map_wice_label,
)
from groundloop.errors import ValidationError


def _example(
    example_id: str, split: str, claim: str, evidence: str
) -> VerificationExample:
    return VerificationExample(
        example_id=example_id,
        source="fixture",
        source_revision="revision-1",
        split=split,
        claim_group_id=f"group-{example_id}",
        claim=claim,
        evidence=evidence,
        label=Label.NEUTRAL,
        construction="fixture",
    )


def test_frozen_dataset_label_mapping() -> None:
    assert map_scifact_label("SUPPORT") is Label.SUPPORT
    assert map_scifact_label("CONTRADICT") is Label.REFUTE
    assert map_wice_label("supported") is Label.SUPPORT
    assert map_wice_label("partially_supported") is Label.NEUTRAL
    assert map_wice_label("not_supported") is Label.NEUTRAL
    with pytest.raises(ValidationError):
        map_wice_label("refuted")


def test_cross_split_claim_group_is_kept_only_in_protected_test() -> None:
    train = _example("train", "train", "Same claim", "train evidence")
    test = _example("test", "test", " same   claim ", "test evidence")
    kept, report = deduplicate_examples((train, test))
    assert kept == (test,)
    assert report.cross_split_claim_groups_removed == 1


def test_cross_split_pair_duplicate_and_normalization_are_deduplicated() -> None:
    development = _example("dev", "development", "Claim", "Evidence here")
    test = _example("test", "test", " claim ", " evidence  here ")
    kept, report = deduplicate_examples((development, test))
    assert kept == (test,)
    # Claim-group isolation removes the lower-priority row before pair dedup.
    assert report.cross_split_claim_groups_removed == 1


def test_neutral_sampling_is_split_local_deterministic_and_excludes_evidence() -> None:
    first = deterministic_neutral_document(
        claim_id="claim-1",
        forbidden_doc_ids={"doc-1"},
        split_doc_ids=("doc-1", "doc-2", "doc-3"),
        seed=17,
    )
    second = deterministic_neutral_document(
        claim_id="claim-1",
        forbidden_doc_ids={"doc-1"},
        split_doc_ids=("doc-1", "doc-2", "doc-3"),
        seed=17,
    )
    assert first == second
    assert first in {"doc-2", "doc-3"}
