from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from groundloop.m5.evaluation.config import (
    evaluation_config_identity_dict,
    load_controlled_evaluation_config,
)
from groundloop.m5.evaluation.records import (
    AdapterReject,
    PrimaryExclusionReason,
    RejectReason,
    SourceLabel,
    WiceSplit,
)
from groundloop.m5.evaluation.wice import (
    _build_evidence_unit,
    _Subclaim,
    adapt_wice,
    load_and_adapt_wice,
)
from m5.evaluation.helpers import Rows, parent_row, subclaim_row

_PINNED_CONFIG_SHA256 = (
    "4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f"
)
_PINNED_MANIFEST_SHA256 = (
    "c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2"
)
_PINNED_WICE_COMMIT = "ddeb6c183665e2a20c5f03c5aa07f03888b9870f"
_PINNED_SOURCE_VALUE = os.environ.get("GROUNDLOOP_WICE_SOURCE_ROOT")
_PINNED_SOURCE_ROOT = (
    Path(_PINNED_SOURCE_VALUE).resolve() if _PINNED_SOURCE_VALUE else None
)


def _refresh_manifest_source(
    root: Path, manifest_path: Path, source_path: Path
) -> None:
    payload = cast(
        dict[str, object], json.loads(manifest_path.read_text(encoding="utf-8"))
    )
    source_specs = cast(list[dict[str, object]], payload["source_files"])
    relative_path = source_path.relative_to(root).as_posix()
    raw = source_path.read_bytes()
    for spec in source_specs:
        if spec["relative_path"] == relative_path:
            spec["rows"] = len(raw.splitlines())
            spec["bytes"] = len(raw)
            spec["sha256"] = hashlib.sha256(raw).hexdigest()
            break
    else:
        raise AssertionError(f"manifest has no source entry for {relative_path}")
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")


def _matrix_rows() -> Rows:
    claims = [
        parent_row("no-final", ["evidence"]),
        parent_row("parent-negative", ["evidence"], label="not_supported"),
        parent_row("too-many", ["evidence"]),
        parent_row("ordinal-gap", ["evidence"]),
        parent_row("invalid-map", ["evidence"]),
        parent_row("unsupported", ["evidence"]),
        parent_row("duplicate-requirement", ["evidence"]),
        parent_row("unrepresentable", ["evidence"]),
        parent_row("byte-mismatch", ["same"]),
        parent_row("membership", ["valid", " \t ", "x" * 1201]),
        parent_row("malformed-parent", ["evidence"], claim=" \t "),
        parent_row("duplicate-parent", ["evidence"]),
        parent_row("duplicate-parent", ["evidence"]),
    ]
    subclaims = [
        *(
            subclaim_row(
                f"too-many-{ordinal}",
                ["evidence"],
                [[0]],
                claim=f"Requirement {ordinal}",
            )
            for ordinal in range(9)
        ),
        subclaim_row("ordinal-gap-1", ["evidence"], [[0]]),
        subclaim_row("invalid-map-0", ["evidence"], [[0]]),
        subclaim_row("invalid-map-0", ["evidence"], [[0]]),
        subclaim_row(
            "unsupported-0",
            ["evidence"],
            [],
            label="not_supported",
        ),
        subclaim_row(
            "duplicate-requirement-0",
            ["evidence"],
            [[0]],
            claim="Same text",
        ),
        subclaim_row(
            "duplicate-requirement-1",
            ["evidence"],
            [[0]],
            claim=" Same\ttext ",
        ),
        subclaim_row("unrepresentable-0", ["evidence"], [[]]),
        subclaim_row("byte-mismatch-0", ["same"], [[0]]),
        subclaim_row(
            "membership-0",
            ["valid", " \t ", "x" * 1201],
            ["not-an-array", [], [True], [-1], [99], [0, 0], [1], [2], [0]],
        ),
        subclaim_row("missing-parent-0", ["evidence"], [[0]]),
        subclaim_row("invalid-subclaim-id", ["evidence"], [[0]]),
        subclaim_row("malformed-subclaim-0", ["evidence"], [[0]], claim=" \n "),
        {
            **subclaim_row("invalid-supporting-0", ["evidence"], [[0]]),
            "supporting_sentences": "not-an-array",
        },
    ]
    return {WiceSplit.TRAIN: (claims, subclaims)}


def _inject_raw_row_cases(root: Path, manifest_path: Path) -> None:
    claim_path = root / "data/entailment_retrieval/claim/train.jsonl"
    claim_path.write_bytes(
        claim_path.read_bytes()
        + b" \t\n"
        + b'{"evidence":[]\n'
        + (
            b'{"claim":"duplicate key","evidence":[],"evidence":[],'
            b'"label":"supported","meta":{"id":"duplicate-key"}}\n'
        )
        + b"[]\n"
        + (
            b'{"claim":"missing evidence","label":"supported",'
            b'"meta":{"id":"missing-evidence"}}\n'
        )
    )

    subclaim_path = root / "data/entailment_retrieval/subclaim/train.jsonl"
    lines = subclaim_path.read_bytes().splitlines(keepends=True)
    for index, line in enumerate(lines):
        if b'"id":"byte-mismatch-0"' not in line:
            continue
        replacement = line.replace(b'"evidence":["same"]', b'"evidence":[ "same"]', 1)
        if replacement == line:
            raise AssertionError("byte-mismatch fixture did not contain evidence")
        lines[index] = replacement
        break
    else:
        raise AssertionError("byte-mismatch fixture row was not found")
    subclaim_path.write_bytes(b"".join(lines))

    _refresh_manifest_source(root, manifest_path, claim_path)
    _refresh_manifest_source(root, manifest_path, subclaim_path)


def test_public_adapter_rejection_and_exclusion_matrix(
    wice_fixture_factory: Callable[[Rows], tuple[Path, Path]],
) -> None:
    root, manifest_path = wice_fixture_factory(_matrix_rows())
    _inject_raw_row_cases(root, manifest_path)

    result = load_and_adapt_wice(root, manifest_path)

    expected_public_rejections = set(RejectReason) - {
        RejectReason.CONFLICTING_SOURCE_TEXT
    }
    assert {item.reason for item in result.rejects} == expected_public_rejections
    assert {reason for reason, _ in result.audit.rejection_counts} == {
        item.value for item in expected_public_rejections
    }
    assert {item.reason for item in result.primary_exclusions} == set(
        PrimaryExclusionReason
    )
    assert {reason for reason, _ in result.audit.primary_exclusion_counts} == {
        item.value for item in PrimaryExclusionReason
    }


def test_conflicting_source_text_guard_emits_its_frozen_rejection() -> None:
    subclaim = _Subclaim(
        split=WiceSplit.TRAIN,
        row_number=1,
        meta_id="conflict-0",
        parent_meta_id="conflict",
        source_ordinal=0,
        claim="Requirement",
        label=SourceLabel.SUPPORTED,
        evidence=("new source text",),
        evidence_bytes=b'["new source text"]',
        supporting_sentences=([0],),
    )
    rejects: list[AdapterReject] = []

    built = _build_evidence_unit(
        subclaim=subclaim,
        evidence_set_ordinal=0,
        membership=[0],
        source_text_by_index={("wice:train:conflict:evidence", 0): "old source text"},
        rejects=rejects,
    )

    assert built is None
    assert tuple(item.reason for item in rejects) == (
        RejectReason.CONFLICTING_SOURCE_TEXT,
    )


@pytest.mark.skipif(
    _PINNED_SOURCE_ROOT is None
    or not (
        _PINNED_SOURCE_ROOT / "data/entailment_retrieval/claim/train.jsonl"
    ).is_file(),
    reason="set GROUNDLOOP_WICE_SOURCE_ROOT to the pinned local WiCE checkout",
)
def test_pinned_wice_bytes_reproduce_the_frozen_adapter_audit() -> None:
    assert _PINNED_SOURCE_ROOT is not None
    repository_root = Path(__file__).resolve().parents[3]
    config = load_controlled_evaluation_config(
        repository_root / "configs/m5/controlled_evaluation_v1.json"
    )

    result = adapt_wice(_PINNED_SOURCE_ROOT, config.source_manifest)
    identity = evaluation_config_identity_dict(config)
    split_audits = {item.split: item for item in result.audit.split_audits}

    assert config.canonical_sha256 == _PINNED_CONFIG_SHA256
    assert result.manifest.manifest_hash == _PINNED_MANIFEST_SHA256
    assert result.manifest.official_commit == _PINNED_WICE_COMMIT
    assert result.manifest.license.annotation_license == "ODC-BY"
    assert identity["primary_gate_identity"] == {
        "classification": "checked_in_frozen_primary",
        "eligible": True,
        "config_hash_matches": True,
        "source_manifest_hash_matches": True,
        "required_config_canonical_sha256": _PINNED_CONFIG_SHA256,
        "required_source_manifest_canonical_sha256": _PINNED_MANIFEST_SHA256,
    }
    assert len(result.source_claims) == 684
    assert len(result.source_requirements) == 1_796
    assert len(result.controlled_projections) == 3_573
    assert result.audit.rejection_counts == (("evidence_unit_overlength", 13),)
    assert result.audit.primary_exclusion_counts == (
        ("parent_not_supported", 1_281),
        ("unrepresentable_positive_requirement", 2),
    )
    assert {
        split: (
            audit.representable_primary_parents,
            audit.hall_complete_parents,
            audit.hall_failing_parents,
            audit.primary_overlength_annotations,
        )
        for split, audit in split_audits.items()
    } == {
        WiceSplit.TRAIN: (460, 362, 98, 0),
        WiceSplit.DEV: (114, 98, 16, 4),
        WiceSplit.TEST: (110, 89, 21, 2),
    }
    assert result.audit.direct_claim_projection_count == 0
    assert result.audit.model_call_count == 0
    assert result.audit.test_selection_performed is False
