from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from groundloop.domain import SubjectKind
from groundloop.errors import ValidationError
from groundloop.m5.digests import (
    int_field,
    sequence_field,
    stable_m5_digest,
    text_field,
)
from groundloop.m5.evaluation.records import (
    PrimaryExclusionReason,
    RejectReason,
    WiceSplit,
)
from groundloop.m5.evaluation.wice import load_and_adapt_wice
from m5.evaluation.helpers import Rows, parent_row, subclaim_row


def _one_split_rows(
    *,
    evidence: list[str],
    supporting: list[object],
) -> Rows:
    return {
        WiceSplit.TRAIN: (
            [parent_row("same", evidence)],
            [subclaim_row("same-0", evidence, supporting)],
        )
    }


def test_hashes_are_verified_before_rows_are_parsed(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    root, manifest_path = wice_fixture_factory(
        _one_split_rows(evidence=["evidence"], supporting=[[0]])
    )
    source = root / "data/entailment_retrieval/claim/train.jsonl"
    source.write_bytes(source.read_bytes() + b"not-json\n")

    with pytest.raises(ValidationError, match="byte-count mismatch"):
        load_and_adapt_wice(root, manifest_path)


def test_atomic_units_keep_original_ordinals_and_project_only_the_least(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    root, manifest_path = wice_fixture_factory(
        _one_split_rows(
            evidence=[" second\t sentence ", "first sentence"],
            supporting=[[1, 0], [0, 1], [1, 0]],
        )
    )
    result = load_and_adapt_wice(root, manifest_path)

    assert len(result.evidence_units) == 1
    unit = result.evidence_units[0]
    assert unit.normalized_sentences == ("first sentence", "second sentence")
    assert unit.content_json == (
        '{"schema":"wice-evidence-unit-text-v1",'
        '"sentences":["first sentence","second sentence"]}'
    )
    assert unit.rendered_text == "first sentence\n\nsecond sentence"
    source_id = "wice:train:same:evidence"
    expected_unit_id = stable_m5_digest(
        "wice-evidence-unit-v1",
        int_field(2),
        sequence_field(
            (
                sequence_field(
                    (text_field(source_id), int_field(0), text_field("second sentence"))
                ),
                sequence_field(
                    (text_field(source_id), int_field(1), text_field("first sentence"))
                ),
            )
        ),
    )
    assert unit.evidence_unit_id == expected_unit_id
    annotations = tuple(
        item for item in result.source_annotations if item.parent_meta_id == "same"
    )
    assert tuple(item.evidence_set_ordinal for item in annotations) == (0, 1, 2)
    assert len({item.source_annotation_id for item in annotations}) == 3
    assert {item.evidence_unit_id for item in annotations} == {unit.evidence_unit_id}
    assert len(result.controlled_projections) == 1
    assert (
        result.controlled_projections[0].source_annotation_id
        == annotations[0].source_annotation_id
    )


def test_same_external_ids_in_two_splits_never_cross_contaminate(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    rows: Rows = {
        WiceSplit.TRAIN: (
            [parent_row("collision", ["train evidence"])],
            [subclaim_row("collision-0", ["train evidence"], [[0]])],
        ),
        WiceSplit.DEV: (
            [parent_row("collision", ["dev evidence"])],
            [subclaim_row("collision-0", ["dev evidence"], [[0]])],
        ),
    }
    root, manifest_path = wice_fixture_factory(rows)
    result = load_and_adapt_wice(root, manifest_path)

    assert len(result.source_claims) == 2
    assert len(result.source_requirements) == 2
    assert len(result.controlled_projections) == 2
    projections_by_split = {
        projection.split: projection for projection in result.controlled_projections
    }
    annotations_by_split = {
        annotation.split: annotation for annotation in result.source_annotations
    }
    assert set(projections_by_split) == {WiceSplit.TRAIN, WiceSplit.DEV}
    for split in (WiceSplit.TRAIN, WiceSplit.DEV):
        assert (
            projections_by_split[split].source_annotation_id
            == annotations_by_split[split].source_annotation_id
        )
    assert (
        projections_by_split[WiceSplit.TRAIN].observation.chunk_version_id
        != projections_by_split[WiceSplit.DEV].observation.chunk_version_id
    )


def test_hall_failure_is_retained_with_a_separate_source_label(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    evidence = ["one shared sentence"]
    rows: Rows = {
        WiceSplit.TRAIN: (
            [parent_row("hall", evidence)],
            [
                subclaim_row("hall-0", evidence, [[0]], claim="First requirement"),
                subclaim_row("hall-1", evidence, [[0]], claim="Second requirement"),
            ],
        )
    }
    root, manifest_path = wice_fixture_factory(rows)
    result = load_and_adapt_wice(root, manifest_path)

    assert len(result.source_semantic_states) == 1
    assert result.source_semantic_states[0].source_semantic_label is True
    assert result.source_semantic_states[0].direct_source_support is False
    assert len(result.exact_system_states) == 1
    exact = result.exact_system_states[0]
    assert exact.requirement_count == 2
    assert exact.distinct_content_count == 1
    assert exact.matching_size == 1
    assert exact.groundloop_sdr_complete is False
    assert exact.perfect_matching_count == 0
    assert result.audit.split_audits[0].hall_failing_parents == 1


def test_all_membership_rejections_are_explicit_and_units_are_never_split(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    evidence = ["valid", " \t ", "x" * 1201]
    supporting: list[object] = [
        "not-an-array",
        [],
        [True],
        [-1],
        [99],
        [0, 0],
        [1],
        [2],
        [0],
    ]
    root, manifest_path = wice_fixture_factory(
        _one_split_rows(evidence=evidence, supporting=supporting)
    )
    result = load_and_adapt_wice(root, manifest_path)

    reasons = {item.reason for item in result.rejects}
    assert {
        RejectReason.MALFORMED_MEMBERSHIP,
        RejectReason.EMPTY_SUPPORTING_SET,
        RejectReason.NONINTEGER_SENTENCE_INDEX,
        RejectReason.NEGATIVE_SENTENCE_INDEX,
        RejectReason.OUT_OF_RANGE_SENTENCE_INDEX,
        RejectReason.DUPLICATE_SENTENCE_INDEX,
        RejectReason.EMPTY_NORMALIZED_SENTENCE,
        RejectReason.EVIDENCE_UNIT_OVERLENGTH,
    } <= reasons
    assert len(result.evidence_units) == 1
    assert result.evidence_units[0].normalized_sentences == ("valid",)
    assert result.audit.split_audits[0].primary_overlength_annotations == 1


def test_semantically_equal_but_byte_different_evidence_arrays_are_rejected(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    root, manifest_path = wice_fixture_factory(
        _one_split_rows(evidence=["a"], supporting=[[0]])
    )
    subclaim_path = root / "data/entailment_retrieval/subclaim/train.jsonl"
    raw = subclaim_path.read_bytes().replace(b'"evidence":["a"]', b'"evidence":["a" ]')
    subclaim_path.write_bytes(raw)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for spec in manifest["source_files"]:
        if spec["split"] == "train" and spec["kind"] == "subclaim":
            spec["bytes"] = len(raw)
            spec["sha256"] = hashlib.sha256(raw).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    result = load_and_adapt_wice(root, manifest_path)

    assert RejectReason.EVIDENCE_ARRAY_BYTE_MISMATCH in {
        item.reason for item in result.rejects
    }
    assert PrimaryExclusionReason.INVALID_SUBCLAIM_MAPPING in {
        item.reason for item in result.primary_exclusions
    }
    assert not result.source_claims


def test_wice_projection_is_requirement_only_and_zero_call(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    root, manifest_path = wice_fixture_factory(
        _one_split_rows(evidence=["a"], supporting=[[0]])
    )
    result = load_and_adapt_wice(root, manifest_path)

    projection = result.controlled_projections[0]
    assert projection.observation.subject_kind is SubjectKind.REQUIREMENT
    assert projection.observation.task_type == "verify_requirement_v1"
    assert (
        projection.observation.support_score,
        projection.observation.refute_score,
        projection.observation.neutral_score,
    ) == (1.0, 0.0, 0.0)
    assert projection.event_payload_hash != projection.event_id
    assert result.audit.direct_claim_projection_count == 0
    assert result.audit.model_call_count == 0
    assert result.audit.test_selection_performed is False


def test_full_controlled_projection_golden_vector(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    rows: Rows = {
        WiceSplit.TRAIN: (
            [parent_row("golden", ["evidence"])],
            [subclaim_row("golden-0", ["evidence"], [[0]])],
        )
    }
    root, manifest_path = wice_fixture_factory(rows)
    result = load_and_adapt_wice(root, manifest_path)
    requirement = result.source_requirements[0]
    unit = result.evidence_units[0]
    annotation = result.source_annotations[0]
    projection = result.controlled_projections[0]

    assert result.manifest.manifest_hash == (
        "65e5eb3dd181076244cc853766c78adf8b3058eb52f78de959c38cee78354556"
    )
    assert requirement.requirement_version_id == (
        "ac8b0703ac771b870d512245b2e68b6b740902d0f18f294f30217f25b270caa0"
    )
    assert unit.evidence_unit_id == (
        "73a5daa5db71b3f01ed4f197008d3ae2a8751abc751c1405c4bea3f9d8a4b539"
    )
    assert unit.text_hash == (
        "ef53b984c6797b2b97b71677408f87f131c5217a571bf799f6f092ac8d9ffd1a"
    )
    assert annotation.source_annotation_id == (
        "3b35aabc0b79747a6e78b46282ee9647caf39aa9302724153367b309ee594761"
    )
    assert projection.observation.input_hash == (
        "2404627e7e0264ca9c0b0a96b8451d641d9ff562da3f8dddfe03bc4f6ebb7e44"
    )
    assert projection.observation_id == (
        "1b700342f6a9d3026a85b7b7969458a13e256e706deb077a31a2ea43842540e2"
    )
    assert projection.event_id == (
        "e0ff8d3d3a7450ac269f51e1f154bd611ca27a39185492403e52bb7cd811d5ef"
    )
    assert projection.event_payload_hash == (
        "04056059e41239ee8df64649aa9e5c7c7c99bcec936f04e308d7b4fa71becce4"
    )
    assert (
        projection.observation.support_score,
        projection.observation.refute_score,
        projection.observation.neutral_score,
    ) == (1.0, 0.0, 0.0)
