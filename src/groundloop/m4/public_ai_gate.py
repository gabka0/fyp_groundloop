"""Untouched VitaminC revision gate for GroundLoop's frozen M3 AI artifacts.

The gate is deliberately empirical.  It compares frozen model outputs with
human dataset labels and never turns either into objective truth.  It adds a
change-sensitive evaluation that M3 did not have: paired, near-identical
Wikipedia evidence revisions with SUPPORT/REFUTE and SUPPORT/NEUTRAL endpoint
changes.  Exact IVM correctness remains a separate database claim.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import math
import os
import platform
import random
import resource
import subprocess
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.ai.contracts import AtomicClaim, ChunkDraft, stable_digest
from groundloop.ai.embeddings.bge import (
    BGE_MODEL_ID,
    BGE_QUERY_PREFIX,
    BGE_REVISION,
)
from groundloop.ai.verification.adapter import PinnedMiniLMVerifier
from groundloop.ai.verification.artifacts import file_sha256, tree_digest
from groundloop.ai.verification.calibration import TemperatureCalibration
from groundloop.ai.verification.constants import LABELS, Label
from groundloop.ai.verification.data import normalize_pair_text
from groundloop.domain import normalized_text_hash
from groundloop.errors import ValidationError

_HEX = frozenset("0123456789abcdef")
_METRIC_NAMES = ("accuracy", "macro_f1", "multiclass_brier", "nll", "ece")
_SOURCE_LABELS = {
    "SUPPORTS": Label.SUPPORT,
    "REFUTES": Label.REFUTE,
    "NOT ENOUGH INFO": Label.NEUTRAL,
}


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(Mapping[str, object], value)


def _string(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{key} must be a non-empty string")
    return value


def _integer(mapping: Mapping[str, object], key: str) -> int:
    value = mapping.get(key)
    if type(value) is not int:
        raise ValidationError(f"{key} must be an integer")
    return value


def _number(mapping: Mapping[str, object], key: str) -> float:
    value = mapping.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValidationError(f"{key} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValidationError(f"{key} must be finite")
    return result


def _require_hash(path: Path, expected: str, name: str) -> str:
    if len(expected) != 64 or any(character not in _HEX for character in expected):
        raise ValidationError(f"{name} config value is not SHA-256")
    observed = file_sha256(path)
    if observed != expected:
        raise ValidationError(
            f"{name} mismatch: expected {expected}, observed {observed}"
        )
    return observed


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class GateConfig:
    path: Path
    file_sha256: str
    seed: int
    bootstrap_resamples: int
    ece_bins: int
    threads: int
    verifier_batch_size: int
    embedding_batch_size: int
    source: Mapping[str, object]
    dataset: Mapping[str, object]
    m3: Mapping[str, object]
    verifier: Mapping[str, object]
    embedding: Mapping[str, object]

    @classmethod
    def read(cls, path: Path) -> GateConfig:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError(f"cannot read M4.12 config: {path}") from error
        root = _mapping(payload, "config")
        if _string(root, "schema_version") != ("groundloop-m4-vitaminc-gate-config-v1"):
            raise ValidationError("unsupported M4.12 config schema")
        seed = _integer(root, "seed")
        resamples = _integer(root, "bootstrap_resamples")
        bins = _integer(root, "ece_bins")
        threads = _integer(root, "threads")
        verifier_batch = _integer(root, "verifier_batch_size")
        embedding_batch = _integer(root, "embedding_batch_size")
        if (
            seed < 0
            or resamples <= 0
            or bins <= 0
            or not 1 <= threads <= 8
            or verifier_batch <= 0
            or embedding_batch <= 0
        ):
            raise ValidationError("invalid M4.12 numeric config")
        return cls(
            path,
            file_sha256(path),
            seed,
            resamples,
            bins,
            threads,
            verifier_batch,
            embedding_batch,
            _mapping(root.get("source"), "source"),
            _mapping(root.get("dataset"), "dataset"),
            _mapping(root.get("m3"), "m3"),
            _mapping(root.get("verifier"), "verifier"),
            _mapping(root.get("embedding"), "embedding"),
        )


@dataclass(frozen=True, slots=True)
class VitaminCRow:
    unique_id: str
    case_id: str
    wiki_revision_id: str
    source_label: str
    claim: str
    evidence: str
    page: str
    revision_type: str

    @property
    def label(self) -> Label:
        return _SOURCE_LABELS[self.source_label]

    @property
    def suffix(self) -> int:
        try:
            return int(self.unique_id.rsplit("_", 1)[1])
        except (IndexError, ValueError) as error:
            raise ValidationError(
                f"invalid VitaminC unique_id: {self.unique_id}"
            ) from error

    @classmethod
    def from_json(cls, value: object) -> VitaminCRow:
        row = _mapping(value, "VitaminC row")
        revision_type = _string(row, "revision_type")
        raw_revision_id = row.get("wiki_revision_id", "")
        if not isinstance(raw_revision_id, str):
            raise ValidationError("wiki_revision_id must be a string when present")
        result = cls(
            _string(row, "unique_id"),
            _string(row, "case_id"),
            raw_revision_id,
            _string(row, "label"),
            _string(row, "claim"),
            _string(row, "evidence"),
            _string(row, "page"),
            revision_type,
        )
        if result.source_label not in _SOURCE_LABELS:
            raise ValidationError(f"unsupported VitaminC label: {result.source_label}")
        if result.revision_type not in {"real", "synthetic"}:
            raise ValidationError("unsupported VitaminC revision type")
        if result.revision_type == "real" and not result.wiki_revision_id:
            raise ValidationError("real VitaminC rows require wiki_revision_id")
        return result


@dataclass(frozen=True, slots=True)
class SelectedCase:
    stratum: str
    selection_key: str
    case_id: str
    page: str
    rows: tuple[VitaminCRow, ...]


@dataclass(frozen=True, slots=True)
class DataAudit:
    cases: tuple[SelectedCase, ...]
    rows: tuple[VitaminCRow, ...]
    payload: Mapping[str, object]


def _load_rows(path: Path) -> tuple[VitaminCRow, ...]:
    rows: list[VitaminCRow] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ValidationError(f"cannot read VitaminC split: {path}") from error
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            rows.append(VitaminCRow.from_json(json.loads(line)))
        except (json.JSONDecodeError, ValidationError) as error:
            raise ValidationError(f"malformed {path}:{line_number}") from error
    if not rows:
        raise ValidationError(f"VitaminC split is empty: {path}")
    return tuple(rows)


def _git_head(repository: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def validate_primary_source(
    repository: Path, archive: Path, data_root: Path, config: GateConfig
) -> Mapping[str, object]:
    revision = _string(config.source, "revision")
    if _git_head(repository) != revision:
        raise ValidationError("VitaminC source repository is not at the frozen commit")
    source_hashes = {
        "README.md": _require_hash(
            repository / "README.md",
            _string(config.source, "readme_sha256"),
            "source README",
        ),
        "DATA_LICENSE": _require_hash(
            repository / "DATA_LICENSE",
            _string(config.source, "data_license_sha256"),
            "source data license",
        ),
        "LICENSE": _require_hash(
            repository / "LICENSE",
            _string(config.source, "code_license_sha256"),
            "source code license",
        ),
    }
    archive_sha = _require_hash(
        archive, _string(config.dataset, "archive_sha256"), "VitaminC archive"
    )
    inner_hashes = {
        "train.jsonl": _require_hash(
            data_root / "train.jsonl",
            _string(config.dataset, "train_sha256"),
            "VitaminC train",
        ),
        "dev.jsonl": _require_hash(
            data_root / "dev.jsonl",
            _string(config.dataset, "development_sha256"),
            "VitaminC development",
        ),
        "test.jsonl": _require_hash(
            data_root / "test.jsonl",
            _string(config.dataset, "test_sha256"),
            "VitaminC test",
        ),
        "LICENSE": _require_hash(
            data_root / "LICENSE",
            _string(config.dataset, "embedded_license_sha256"),
            "VitaminC embedded license",
        ),
        "README.txt": _require_hash(
            data_root / "README.txt",
            _string(config.dataset, "embedded_readme_sha256"),
            "VitaminC embedded readme",
        ),
    }
    return {
        "official_repository": _string(config.source, "repository"),
        "official_repository_revision": revision,
        "source_file_sha256": source_hashes,
        "archive_url": _string(config.dataset, "archive_url"),
        "hosting_repository_revision": _string(
            config.dataset, "hosting_repository_revision"
        ),
        "archive_sha256": archive_sha,
        "archive_member_sha256": inner_hashes,
        "data_license": (
            "Wikipedia article terms, or CC BY-SA 3.0 where unavailable; "
            "synthetic annotations additionally derive from FEVER. This gate "
            "uses only revision_type=real."
        ),
    }


def _normalized_sets(
    rows: Sequence[VitaminCRow],
) -> tuple[set[str], set[str], set[tuple[str, str]]]:
    claims = {normalize_pair_text(row.claim) for row in rows}
    evidence = {normalize_pair_text(row.evidence) for row in rows}
    pairs = {
        (normalize_pair_text(row.claim), normalize_pair_text(row.evidence))
        for row in rows
    }
    return claims, evidence, pairs


def exact_normalized_overlap_counts(
    vitamin_rows: Sequence[VitaminCRow], m3_pairs: Sequence[tuple[str, str]]
) -> Mapping[str, int]:
    """Count exact normalized overlap without interpreting semantic similarity."""
    test_claims, test_evidence, test_pairs = _normalized_sets(vitamin_rows)
    m3_claims = {normalize_pair_text(claim) for claim, _ in m3_pairs}
    m3_evidence = {normalize_pair_text(evidence) for _, evidence in m3_pairs}
    normalized_m3_pairs = {
        (normalize_pair_text(claim), normalize_pair_text(evidence))
        for claim, evidence in m3_pairs
    }
    return {
        "normalized_claims": len(test_claims & m3_claims),
        "normalized_evidence": len(test_evidence & m3_evidence),
        "normalized_pairs": len(test_pairs & normalized_m3_pairs),
    }


def _load_m3_rows(path: Path) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        value = _mapping(json.loads(line), f"M3 row {line_number}")
        result.append((_string(value, "claim"), _string(value, "evidence")))
    return tuple(result)


def _case_layout(rows: Sequence[VitaminCRow]) -> Mapping[int, VitaminCRow] | None:
    if len(rows) != 4 or len({row.revision_type for row in rows}) != 1:
        return None
    by_suffix = {row.suffix: row for row in rows}
    if set(by_suffix) != {1, 2, 3, 4}:
        return None
    if not (
        by_suffix[1].claim == by_suffix[2].claim
        and by_suffix[3].claim == by_suffix[4].claim
        and by_suffix[1].evidence == by_suffix[3].evidence
        and by_suffix[2].evidence == by_suffix[4].evidence
        and len({row.claim for row in rows}) == 2
        and len({row.evidence for row in rows}) == 2
        and len({(row.claim, row.evidence) for row in rows}) == 4
    ):
        return None
    for first, second in ((1, 2), (3, 4)):
        endpoint_labels = {by_suffix[first].label, by_suffix[second].label}
        if Label.SUPPORT not in endpoint_labels or len(endpoint_labels) != 2:
            return None
    return by_suffix


def _sample_manifest(cases: Sequence[SelectedCase]) -> tuple[list[dict[str, str]], str]:
    items: list[dict[str, str]] = []
    for case in cases:
        for row in sorted(case.rows, key=lambda item: item.unique_id):
            items.append(
                {
                    "stratum": case.stratum,
                    "selection_key": case.selection_key,
                    "unique_id": row.unique_id,
                    "case_id": row.case_id,
                    "page": row.page,
                    "wiki_revision_id": row.wiki_revision_id,
                    "revision_type": row.revision_type,
                    "label": row.source_label,
                    "claim_sha256": hashlib.sha256(row.claim.encode()).hexdigest(),
                    "evidence_sha256": hashlib.sha256(
                        row.evidence.encode()
                    ).hexdigest(),
                }
            )
    return items, _canonical_sha256(items)


def validate_and_sample_data(
    data_root: Path, m3_artifact_root: Path, config: GateConfig
) -> DataAudit:
    train = _load_rows(data_root / "train.jsonl")
    development = _load_rows(data_root / "dev.jsonl")
    test = _load_rows(data_root / "test.jsonl")
    if (
        len(test) != _integer(config.dataset, "test_rows")
        or len({row.case_id for row in test}) != _integer(config.dataset, "test_cases")
        or len({row.page for row in test}) != _integer(config.dataset, "test_pages")
    ):
        raise ValidationError("VitaminC test dimensions drifted")
    type_counts = Counter(row.revision_type for row in test)
    if type_counts["real"] != _integer(config.dataset, "test_real_rows") or type_counts[
        "synthetic"
    ] != _integer(config.dataset, "test_synthetic_rows"):
        raise ValidationError("VitaminC test revision-type counts drifted")

    train_pages = {normalize_pair_text(row.page) for row in train}
    development_pages = {normalize_pair_text(row.page) for row in development}
    test_pages = {normalize_pair_text(row.page) for row in test}
    page_intersections = {
        "train_development": len(train_pages & development_pages),
        "train_test": len(train_pages & test_pages),
        "development_test": len(development_pages & test_pages),
    }
    excluded_pages = train_pages | development_pages

    m3_prepared = m3_artifact_root / "prepared"
    _require_hash(
        m3_prepared / "manifest.json",
        _string(config.m3, "prepared_manifest_sha256"),
        "M3 prepared manifest",
    )
    m3_pairs: list[tuple[str, str]] = []
    for split, config_key in (
        ("train", "train_sha256"),
        ("development", "development_sha256"),
        ("test", "test_sha256"),
    ):
        path = m3_prepared / f"{split}.jsonl"
        _require_hash(path, _string(config.m3, config_key), f"M3 {split}")
        m3_pairs.extend(_load_m3_rows(path))
    m3_overlap = exact_normalized_overlap_counts(test, m3_pairs)
    if any(m3_overlap.values()):
        raise ValidationError("VitaminC test is not disjoint from M3 prepared data")

    grouped: defaultdict[str, list[VitaminCRow]] = defaultdict(list)
    for row in test:
        grouped[row.case_id].append(row)
    patterns = {
        "support_refute": Counter({"SUPPORTS": 2, "REFUTES": 2}),
        "support_neutral": Counter({"SUPPORTS": 2, "NOT ENOUGH INFO": 2}),
    }
    candidates: dict[str, list[SelectedCase]] = {name: [] for name in patterns}
    for case_id, rows in grouped.items():
        layout = _case_layout(rows)
        if (
            layout is None
            or rows[0].revision_type != "real"
            or normalize_pair_text(rows[0].page) in excluded_pages
        ):
            continue
        observed = Counter(row.source_label for row in rows)
        for name, expected in patterns.items():
            if observed == expected:
                selection_key = hashlib.sha256(
                    f"{config.seed}\0{name}\0{rows[0].page}\0{case_id}".encode()
                ).hexdigest()
                candidates[name].append(
                    SelectedCase(
                        name,
                        selection_key,
                        case_id,
                        rows[0].page,
                        tuple(rows),
                    )
                )
    for values in candidates.values():
        values.sort(key=lambda item: item.selection_key)

    raw_order = config.dataset.get("sample_strata_order")
    if not isinstance(raw_order, list) or any(
        not isinstance(item, str) for item in raw_order
    ):
        raise ValidationError("sample_strata_order must be a string list")
    order = tuple(cast(list[str], raw_order))
    if order != ("support_refute", "support_neutral"):
        raise ValidationError("M4.12 stratum order drifted")
    target = _integer(config.dataset, "sample_cases_per_stratum")
    used_pages: set[str] = set()
    selected: list[SelectedCase] = []
    for name in order:
        count = 0
        for case in candidates[name]:
            page_key = normalize_pair_text(case.page)
            if page_key in used_pages:
                continue
            selected.append(case)
            used_pages.add(page_key)
            count += 1
            if count == target:
                break
        if count != target:
            raise ValidationError(f"not enough eligible VitaminC cases for {name}")
    sample_rows = tuple(
        row
        for case in selected
        for row in sorted(case.rows, key=lambda item: item.unique_id)
    )
    manifest, manifest_sha = _sample_manifest(selected)
    if manifest_sha != _string(config.dataset, "sample_manifest_sha256"):
        raise ValidationError(
            "deterministic VitaminC sample identity drifted: " + manifest_sha
        )
    sample_counts = Counter(row.label for row in sample_rows)
    expectations = {
        Label.SUPPORT: _integer(config.dataset, "sample_support"),
        Label.REFUTE: _integer(config.dataset, "sample_refute"),
        Label.NEUTRAL: _integer(config.dataset, "sample_neutral"),
    }
    if (
        len(sample_rows) != _integer(config.dataset, "sample_rows")
        or len(selected) != _integer(config.dataset, "sample_cases")
        or len(used_pages) != _integer(config.dataset, "sample_pages")
        or any(
            sample_counts[label] != expected for label, expected in expectations.items()
        )
    ):
        raise ValidationError("deterministic VitaminC sample dimensions drifted")
    return DataAudit(
        tuple(selected),
        sample_rows,
        {
            "full_test": {
                "rows": len(test),
                "cases": len(grouped),
                "pages": len(test_pages),
                "revision_type_counts": dict(sorted(type_counts.items())),
                "official_split_page_intersections": page_intersections,
                "warning": (
                    "The downloaded official splits are not perfectly page-disjoint. "
                    "The bounded gate therefore excludes every test page appearing "
                    "in VitaminC train or development."
                ),
            },
            "groundloop_m3_disjointness": m3_overlap,
            "selection": {
                "uses_model_outputs": False,
                "revision_type": "real",
                "one_case_per_page": True,
                "pages_seen_in_vitaminc_train_or_development_excluded": True,
                "strata_order": list(order),
                "cases_per_stratum": target,
                "candidate_cases": {
                    name: len(values) for name, values in candidates.items()
                },
                "rows": len(sample_rows),
                "cases": len(selected),
                "pages": len(used_pages),
                "class_counts": {
                    LABELS[int(label)]: sample_counts[label] for label in Label
                },
                "sample_manifest_sha256": manifest_sha,
                "manifest_rows": len(manifest),
                "sampling_scope": (
                    "label-stratified diagnostic sample; not an unbiased estimate "
                    "of the full VitaminC test distribution"
                ),
            },
        },
    )


def validate_model_artifacts(
    m3_artifact_root: Path, embedding_cache: Path, config: GateConfig
) -> Mapping[str, object]:
    checkpoint = m3_artifact_root / "checkpoints" / "minilm2-m3-bounded-v1"
    checkpoint_tree = tree_digest(checkpoint)
    if checkpoint_tree != _string(config.verifier, "checkpoint_tree_sha256"):
        raise ValidationError("verifier checkpoint tree hash drifted")
    verifier_weights = _require_hash(
        checkpoint / "model.safetensors",
        _string(config.verifier, "weights_sha256"),
        "verifier weights",
    )
    calibration_path = m3_artifact_root / "reports" / "temperature_calibration.json"
    calibration_sha = _require_hash(
        calibration_path,
        _string(config.verifier, "calibration_file_sha256"),
        "calibration",
    )
    calibration = TemperatureCalibration.read_json(calibration_path)
    if (
        calibration.calibration_version
        != _string(config.verifier, "calibration_version")
        or calibration.fit_split != _string(config.verifier, "calibration_fit_split")
        or calibration.fit_split != "development"
        or not math.isclose(
            calibration.temperature,
            _number(config.verifier, "temperature"),
            rel_tol=0.0,
            abs_tol=0.0,
        )
    ):
        raise ValidationError("calibration identity drifted")

    snapshot = (
        embedding_cache / "models--BAAI--bge-small-en-v1.5" / "snapshots" / BGE_REVISION
    )
    bge_tree = tree_digest(snapshot)
    if bge_tree != _string(config.embedding, "snapshot_tree_sha256"):
        raise ValidationError("BGE snapshot tree hash drifted")
    bge_weights = _require_hash(
        snapshot / "model.safetensors",
        _string(config.embedding, "weights_sha256"),
        "BGE weights",
    )
    if (
        _string(config.embedding, "model_id") != BGE_MODEL_ID
        or _string(config.embedding, "revision") != BGE_REVISION
    ):
        raise ValidationError("BGE frozen identity drifted")
    return {
        "verifier": {
            "logical_model_id": _string(config.verifier, "logical_model_id"),
            "logical_revision": _string(config.verifier, "logical_revision"),
            "checkpoint_tree_sha256": checkpoint_tree,
            "weights_sha256": verifier_weights,
            "calibration_file_sha256": calibration_sha,
            "calibration_version": calibration.calibration_version,
            "temperature": calibration.temperature,
            "calibration_fit_split": calibration.fit_split,
            "calibration_fit_examples": calibration.example_count,
            "max_length": _integer(config.verifier, "max_length"),
        },
        "embedding": {
            "model_id": BGE_MODEL_ID,
            "revision": BGE_REVISION,
            "snapshot_tree_sha256": bge_tree,
            "weights_sha256": bge_weights,
            "max_length": _integer(config.embedding, "max_length"),
            "query_prefix_sha256": hashlib.sha256(
                BGE_QUERY_PREFIX.encode()
            ).hexdigest(),
        },
    }


def _validate_probabilities(
    probabilities: Sequence[tuple[float, float, float]], labels: Sequence[Label]
) -> None:
    if not probabilities or len(probabilities) != len(labels):
        raise ValidationError("metrics require nonempty aligned rows")
    for row in probabilities:
        if any(not math.isfinite(value) or value < 0.0 for value in row):
            raise ValidationError("probabilities must be finite and nonnegative")
        if not math.isclose(sum(row), 1.0, rel_tol=1e-6, abs_tol=1e-6):
            raise ValidationError("probabilities must sum to one")


def classification_metrics(
    probabilities: Sequence[tuple[float, float, float]],
    labels: Sequence[Label],
    *,
    ece_bins: int,
) -> dict[str, object]:
    _validate_probabilities(probabilities, labels)
    if ece_bins <= 0:
        raise ValidationError("ECE bins must be positive")
    predictions = tuple(
        Label(max(range(3), key=lambda index: row[index])) for row in probabilities
    )
    matrix = [[0, 0, 0] for _ in Label]
    for expected, predicted in zip(labels, predictions, strict=True):
        matrix[int(expected)][int(predicted)] += 1
    per_class: list[dict[str, object]] = []
    present_f1: list[float] = []
    for label in Label:
        support = sum(expected == label for expected in labels)
        predicted_count = sum(predicted == label for predicted in predictions)
        true_positive = matrix[int(label)][int(label)]
        precision = true_positive / predicted_count if predicted_count else None
        recall = true_positive / support if support else None
        if support:
            p = precision or 0.0
            r = recall or 0.0
            f1 = 2.0 * p * r / (p + r) if p + r else 0.0
            present_f1.append(f1)
        else:
            f1 = None
        per_class.append(
            {
                "label": LABELS[int(label)],
                "support": support,
                "predicted": predicted_count,
                "true_positive": true_positive,
                "precision": precision,
                "recall": recall,
                "f1": f1,
            }
        )
    count = len(labels)
    accuracy = (
        sum(
            predicted == expected
            for predicted, expected in zip(predictions, labels, strict=True)
        )
        / count
    )
    brier = (
        sum(
            sum(
                (probability - float(index == int(expected))) ** 2
                for index, probability in enumerate(row)
            )
            for row, expected in zip(probabilities, labels, strict=True)
        )
        / count
    )
    nll = (
        -sum(
            math.log(max(row[int(expected)], 1e-15))
            for row, expected in zip(probabilities, labels, strict=True)
        )
        / count
    )
    reliability: list[dict[str, object]] = []
    ece = 0.0
    for index in range(ece_bins):
        lower = index / ece_bins
        upper = (index + 1) / ece_bins
        members = [
            item
            for item, row in enumerate(probabilities)
            if lower <= max(row) < upper
            or (index == ece_bins - 1 and math.isclose(max(row), 1.0))
        ]
        if members:
            confidence = sum(max(probabilities[item]) for item in members) / len(
                members
            )
            bin_accuracy = sum(
                predictions[item] == labels[item] for item in members
            ) / len(members)
            gap = abs(confidence - bin_accuracy)
            ece += len(members) * gap / count
        else:
            confidence = 0.0
            bin_accuracy = 0.0
            gap = 0.0
        reliability.append(
            {
                "lower": lower,
                "upper": upper,
                "count": len(members),
                "mean_confidence": confidence,
                "accuracy": bin_accuracy,
                "absolute_gap": gap,
            }
        )
    return {
        "example_count": count,
        "class_counts": {
            LABELS[int(label)]: sum(expected == label for expected in labels)
            for label in Label
        },
        "per_class": per_class,
        "macro_f1": sum(present_f1) / len(present_f1),
        "macro_f1_semantics": "unweighted mean over present human-label classes",
        "accuracy": accuracy,
        "confusion_matrix": matrix,
        "confusion_matrix_order": list(LABELS),
        "multiclass_brier": brier,
        "nll": nll,
        "nll_probability_floor": 1e-15,
        "ece": ece,
        "ece_bins": ece_bins,
        "reliability": reliability,
    }


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def page_bootstrap_classification(
    probabilities: Sequence[tuple[float, float, float]],
    labels: Sequence[Label],
    pages: Sequence[str],
    *,
    ece_bins: int,
    seed: int,
    resamples: int,
) -> Mapping[str, object]:
    _validate_probabilities(probabilities, labels)
    if len(pages) != len(labels) or resamples <= 0:
        raise ValidationError("page bootstrap inputs are invalid")
    grouped: defaultdict[str, list[int]] = defaultdict(list)
    for index, page in enumerate(pages):
        grouped[page].append(index)
    page_ids = tuple(sorted(grouped))
    generator = random.Random(seed)
    estimates: dict[str, list[float]] = {name: [] for name in _METRIC_NAMES}
    for _ in range(resamples):
        sampled = [page_ids[generator.randrange(len(page_ids))] for _ in page_ids]
        indices = [index for page in sampled for index in grouped[page]]
        point = classification_metrics(
            [probabilities[index] for index in indices],
            [labels[index] for index in indices],
            ece_bins=ece_bins,
        )
        for name in _METRIC_NAMES:
            estimates[name].append(cast(float, point[name]))
    return {
        "independent_unit": "Wikipedia page",
        "units": len(page_ids),
        "seed": seed,
        "resamples": resamples,
        "method": "page-cluster-percentile-bootstrap-v1",
        "level": 0.95,
        "intervals": {
            name: {
                "lower": _percentile(values, 0.025),
                "upper": _percentile(values, 0.975),
            }
            for name, values in estimates.items()
        },
    }


def _row_pair(row: VitaminCRow) -> tuple[AtomicClaim, ChunkDraft]:
    chunk_id = stable_digest("m4-vitaminc-evidence-v1", row.unique_id, row.evidence)
    return (
        AtomicClaim(
            stable_digest("m4-vitaminc-claim-v1", row.case_id, row.claim),
            row.claim,
            True,
            (chunk_id,),
        ),
        ChunkDraft(
            chunk_id,
            stable_digest("m4-vitaminc-document-v1", row.case_id, row.evidence),
            0,
            row.evidence,
            normalized_text_hash(row.evidence),
            "vitaminc-contrastive-evidence-v1",
        ),
    )


def run_verifier(
    rows: Sequence[VitaminCRow], m3_artifact_root: Path, config: GateConfig
) -> tuple[
    tuple[tuple[float, float, float], ...],
    tuple[tuple[float, float, float], ...],
    Mapping[str, object],
]:
    checkpoint = m3_artifact_root / "checkpoints" / "minilm2-m3-bounded-v1"
    calibration = TemperatureCalibration.read_json(
        m3_artifact_root / "reports" / "temperature_calibration.json"
    )
    verifier = PinnedMiniLMVerifier(
        model_path=str(checkpoint),
        model_revision=_string(config.verifier, "logical_revision"),
        temperature=calibration.temperature,
        max_length=_integer(config.verifier, "max_length"),
        batch_size=config.verifier_batch_size,
        local_files_only=True,
        artifact_sha256=tree_digest(checkpoint),
        logical_model_id=_string(config.verifier, "logical_model_id"),
        calibration_version=calibration.calibration_version,
    )
    started = time.perf_counter()
    results = verifier.verify_batch(tuple(_row_pair(row) for row in rows))
    elapsed = time.perf_counter() - started
    if len(results) != len(rows) or any(
        result.raw_logits is None for result in results
    ):
        raise RuntimeError("verifier returned incomplete or unauditable output")
    probabilities = tuple(
        (result.scores.support, result.scores.refute, result.scores.neutral)
        for result in results
    )
    logits = tuple(
        cast(tuple[float, float, float], result.raw_logits) for result in results
    )
    diagnostics = verifier.last_diagnostics
    return (
        probabilities,
        logits,
        {
            "wall_seconds_including_model_load": elapsed,
            "rows_per_second_including_model_load": len(rows) / elapsed,
            "rows": diagnostics.examples,
            "batches": diagnostics.batches,
            "batch_size": config.verifier_batch_size,
            "max_length": diagnostics.max_length,
            "truncated_rows": diagnostics.truncated_examples,
            "failures": 0,
            "timeouts": 0,
            "timeout_policy": "none; explicit local fail-fast run",
        },
    )


def transition_metrics(
    cases: Sequence[SelectedCase],
    probability_by_id: Mapping[str, tuple[float, float, float]],
    *,
    seed: int,
    resamples: int,
) -> Mapping[str, object]:
    records: list[dict[str, object]] = []
    for case in cases:
        layout = _case_layout(case.rows)
        if layout is None:
            raise ValidationError("selected case lost its contrastive layout")
        for first, second in ((1, 2), (3, 4)):
            left = layout[first]
            right = layout[second]
            left_scores = probability_by_id[left.unique_id]
            right_scores = probability_by_id[right.unique_id]
            left_prediction = Label(max(range(3), key=lambda i: left_scores[i]))
            right_prediction = Label(max(range(3), key=lambda i: right_scores[i]))
            records.append(
                {
                    "page": case.page,
                    "stratum": case.stratum,
                    "detected_change": float(left_prediction != right_prediction),
                    "both_endpoints_correct": float(
                        left_prediction == left.label
                        and right_prediction == right.label
                    ),
                    "bidirectional_margin_correct": float(
                        left_scores[int(left.label)] > right_scores[int(left.label)]
                        and right_scores[int(right.label)]
                        > left_scores[int(right.label)]
                    ),
                }
            )
    names = (
        "detected_change",
        "both_endpoints_correct",
        "bidirectional_margin_correct",
    )

    def summarize(selected: Sequence[Mapping[str, object]]) -> dict[str, float]:
        return {
            name: sum(cast(float, row[name]) for row in selected) / len(selected)
            for name in names
        }

    page_records: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for record in records:
        page_records[str(record["page"])].append(record)
    page_ids = tuple(sorted(page_records))
    generator = random.Random(seed)
    boot: dict[str, list[float]] = {name: [] for name in names}
    for _ in range(resamples):
        sampled_pages = [page_ids[generator.randrange(len(page_ids))] for _ in page_ids]
        sampled = [row for page in sampled_pages for row in page_records[page]]
        point = summarize(sampled)
        for name in names:
            boot[name].append(point[name])
    return {
        "semantic_unit": (
            "one claim evaluated against the two paired evidence versions in its case"
        ),
        "claim_transitions": len(records),
        "human_label_changes": len(records),
        "point": summarize(records),
        "by_stratum": {
            stratum: summarize(members)
            for stratum in ("support_refute", "support_neutral")
            if (members := [row for row in records if row["stratum"] == stratum])
        },
        "bootstrap": {
            "independent_unit": "Wikipedia page",
            "units": len(page_ids),
            "seed": seed,
            "resamples": resamples,
            "method": "page-cluster-percentile-bootstrap-v1",
            "level": 0.95,
            "intervals": {
                name: {
                    "lower": _percentile(values, 0.025),
                    "upper": _percentile(values, 0.975),
                }
                for name, values in boot.items()
            },
        },
    }


def _bootstrap_scalar_by_page(
    values: Mapping[str, Sequence[float]], *, seed: int, resamples: int
) -> Mapping[str, object]:
    pages = tuple(sorted(values))
    generator = random.Random(seed)
    estimates: list[float] = []
    for _ in range(resamples):
        sampled = [pages[generator.randrange(len(pages))] for _ in pages]
        flattened = [value for page in sampled for value in values[page]]
        estimates.append(sum(flattened) / len(flattened))
    return {
        "independent_unit": "Wikipedia page",
        "units": len(pages),
        "seed": seed,
        "resamples": resamples,
        "method": "page-cluster-percentile-bootstrap-v1",
        "level": 0.95,
        "lower": _percentile(estimates, 0.025),
        "upper": _percentile(estimates, 0.975),
    }


def run_bge_contrastive(
    cases: Sequence[SelectedCase], embedding_cache: Path, config: GateConfig
) -> tuple[Mapping[str, object], Mapping[str, tuple[float, int]]]:
    try:
        np = importlib.import_module("numpy")
        sentence_transformers = importlib.import_module("sentence_transformers")
    except ImportError as error:
        raise RuntimeError("M4.12 requires sentence-transformers") from error
    started = time.perf_counter()
    model = sentence_transformers.SentenceTransformer(
        BGE_MODEL_ID,
        revision=BGE_REVISION,
        cache_folder=str(embedding_cache),
        local_files_only=True,
        trust_remote_code=False,
        device="cpu",
    )
    load_seconds = time.perf_counter() - started
    max_length = _integer(config.embedding, "max_length")
    if int(model.max_seq_length) != max_length:
        raise ValidationError("BGE maximum sequence length drifted")
    query_inputs: list[str] = []
    evidence_inputs: list[str] = []
    query_rows: list[tuple[SelectedCase, VitaminCRow, VitaminCRow]] = []
    for case in cases:
        layout = _case_layout(case.rows)
        if layout is None:
            raise ValidationError("selected case lost its contrastive layout")
        evidence_inputs.extend((layout[1].evidence, layout[2].evidence))
        query_rows.extend(((case, layout[1], layout[2]), (case, layout[3], layout[4])))
        query_inputs.extend(
            (
                f"{BGE_QUERY_PREFIX}{layout[1].claim}",
                f"{BGE_QUERY_PREFIX}{layout[3].claim}",
            )
        )
    query_tokens = [
        len(model.tokenizer.encode(text, add_special_tokens=True, truncation=False))
        for text in query_inputs
    ]
    evidence_tokens = [
        len(model.tokenizer.encode(text, add_special_tokens=True, truncation=False))
        for text in evidence_inputs
    ]
    inference_started = time.perf_counter()
    query_vectors = np.asarray(
        model.encode(
            query_inputs,
            batch_size=config.embedding_batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        ),
        dtype=np.float32,
    )
    evidence_vectors = np.asarray(
        model.encode(
            evidence_inputs,
            batch_size=config.embedding_batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        ),
        dtype=np.float32,
    )
    inference_seconds = time.perf_counter() - inference_started
    if query_vectors.shape != (len(query_rows), 384) or evidence_vectors.shape != (
        len(evidence_inputs),
        384,
    ):
        raise ValidationError("BGE returned an unexpected matrix shape")
    if not np.all(np.isfinite(query_vectors)) or not np.all(
        np.isfinite(evidence_vectors)
    ):
        raise ValidationError("BGE returned non-finite vectors")

    by_row: dict[str, tuple[float, int]] = {}
    correct_by_page: defaultdict[str, list[float]] = defaultdict(list)
    reciprocal_by_page: defaultdict[str, list[float]] = defaultdict(list)
    ties = 0
    for index, (case, version_a, version_b) in enumerate(query_rows):
        evidence_start = 2 * cases.index(case)
        scores = (
            float(query_vectors[index] @ evidence_vectors[evidence_start]),
            float(query_vectors[index] @ evidence_vectors[evidence_start + 1]),
        )
        support_version = 0 if version_a.label == Label.SUPPORT else 1
        order = sorted(range(2), key=lambda item: (-scores[item], item))
        rank = order.index(support_version) + 1
        ties += int(math.isclose(scores[0], scores[1], rel_tol=0.0, abs_tol=1e-12))
        correct_by_page[case.page].append(float(rank == 1))
        reciprocal_by_page[case.page].append(1.0 / rank)
        by_row[version_a.unique_id] = (scores[0], rank if support_version == 0 else 0)
        by_row[version_b.unique_id] = (scores[1], rank if support_version == 1 else 0)
    recall = sum(
        value for values in correct_by_page.values() for value in values
    ) / len(query_rows)
    mrr = sum(
        value for values in reciprocal_by_page.values() for value in values
    ) / len(query_rows)
    return (
        {
            "role": (
                "two-candidate contrastive ranking: for each claim, rank the "
                "human-SUPPORT evidence version above its near-identical REFUTE or "
                "NEUTRAL counterpart; not global retrieval or affected-claim recall"
            ),
            "queries": len(query_rows),
            "candidate_versions_per_query": 2,
            "point": {"support_version_recall_at_1": recall, "mrr": mrr},
            "bootstrap": {
                "support_version_recall_at_1": _bootstrap_scalar_by_page(
                    correct_by_page,
                    seed=config.seed + 200,
                    resamples=config.bootstrap_resamples,
                ),
                "mrr": _bootstrap_scalar_by_page(
                    reciprocal_by_page,
                    seed=config.seed + 201,
                    resamples=config.bootstrap_resamples,
                ),
            },
            "ties_at_1e_12": ties,
            "telemetry": {
                "model_load_seconds": load_seconds,
                "inference_seconds": inference_seconds,
                "total_seconds": time.perf_counter() - started,
                "query_inputs": len(query_inputs),
                "evidence_inputs": len(evidence_inputs),
                "batch_size": config.embedding_batch_size,
                "query_truncated": sum(value > max_length for value in query_tokens),
                "evidence_truncated": sum(
                    value > max_length for value in evidence_tokens
                ),
                "failures": 0,
                "timeouts": 0,
                "timeout_policy": "none; explicit local fail-fast run",
            },
        },
        by_row,
    )


def _dependencies() -> Mapping[str, str]:
    return {
        name: importlib.metadata.version(name)
        for name in ("numpy", "sentence-transformers", "torch", "transformers")
    }


def run_public_ai_gate(
    *,
    official_repository: Path,
    vitaminc_archive: Path,
    vitaminc_root: Path,
    m3_artifact_root: Path,
    embedding_cache: Path,
    config_path: Path,
    output_directory: Path,
) -> Mapping[str, object]:
    started = time.perf_counter()
    config = GateConfig.read(config_path)
    os.environ["OMP_NUM_THREADS"] = str(config.threads)
    os.environ["MKL_NUM_THREADS"] = str(config.threads)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    try:
        torch = importlib.import_module("torch")
        torch.set_num_threads(config.threads)
    except ImportError as error:
        raise RuntimeError("M4.12 requires PyTorch") from error
    source = validate_primary_source(
        official_repository, vitaminc_archive, vitaminc_root, config
    )
    data = validate_and_sample_data(vitaminc_root, m3_artifact_root, config)
    artifacts = validate_model_artifacts(m3_artifact_root, embedding_cache, config)
    probabilities, logits, verifier_telemetry = run_verifier(
        data.rows, m3_artifact_root, config
    )
    labels = tuple(row.label for row in data.rows)
    classification = classification_metrics(
        probabilities, labels, ece_bins=config.ece_bins
    )
    classification_bootstrap = page_bootstrap_classification(
        probabilities,
        labels,
        tuple(row.page for row in data.rows),
        ece_bins=config.ece_bins,
        seed=config.seed,
        resamples=config.bootstrap_resamples,
    )
    probability_by_id = dict(
        zip((row.unique_id for row in data.rows), probabilities, strict=True)
    )
    transitions = transition_metrics(
        data.cases,
        probability_by_id,
        seed=config.seed + 100,
        resamples=config.bootstrap_resamples,
    )
    embedding, embedding_by_row = run_bge_contrastive(
        data.cases, embedding_cache, config
    )

    output_directory.mkdir(parents=True, exist_ok=True)
    predictions_path = output_directory / "predictions.jsonl"
    prediction_lines: list[str] = []
    stratum_by_case = {case.case_id: case.stratum for case in data.cases}
    selection_key_by_case = {case.case_id: case.selection_key for case in data.cases}
    for index, row in enumerate(data.rows):
        predicted = LABELS[max(range(3), key=lambda item: probabilities[index][item])]
        bge_score, support_rank = embedding_by_row[row.unique_id]
        prediction_lines.append(
            json.dumps(
                {
                    "unique_id": row.unique_id,
                    "case_id": row.case_id,
                    "page_sha256": hashlib.sha256(row.page.encode()).hexdigest(),
                    "wiki_revision_id": row.wiki_revision_id,
                    "revision_type": row.revision_type,
                    "stratum": stratum_by_case[row.case_id],
                    "selection_key": selection_key_by_case[row.case_id],
                    "human_label": LABELS[int(row.label)],
                    "model_argmax": predicted,
                    "stored_probability_order": list(LABELS),
                    "probabilities": list(probabilities[index]),
                    "base_logit_order": ["contradiction", "entailment", "neutral"],
                    "base_logits": list(logits[index]),
                    "claim_sha256": hashlib.sha256(row.claim.encode()).hexdigest(),
                    "evidence_sha256": hashlib.sha256(
                        row.evidence.encode()
                    ).hexdigest(),
                    "bge_cosine_for_this_evidence_version": bge_score,
                    "bge_support_version_rank_if_this_row_is_support": (
                        support_rank if row.label == Label.SUPPORT else None
                    ),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    predictions_path.write_text("\n".join(prediction_lines) + "\n", encoding="utf-8")
    predictions_sha = file_sha256(predictions_path)

    report: dict[str, object] = {
        "schema_version": "groundloop-m4-vitaminc-gate-report-v1",
        "scientific_boundary": {
            "evaluates": (
                "frozen BGE and calibrated verifier behavior on untouched, "
                "contrastive Wikipedia-revision evidence with human labels"
            ),
            "does_not_evaluate": [
                "objective truth",
                "exact database IVM correctness",
                "global affected-claim discovery recall",
                "generation or claim extraction",
                "representative full-VitaminC accuracy",
            ],
            "human_model_separation": (
                "Human labels are evaluation targets. Model scores remain empirical "
                "judgments and are not promoted to objective truth."
            ),
        },
        "gate_verdict": {
            "provenance": "PASSED",
            "real_model_execution": "PASSED",
            "change_sensitive_ai_evidence": "MEASURED",
            "dynamic_m4_end_to_end_quality": "PARTIAL_ONLY",
            "reason": (
                "The gate measures revision-sensitive endpoint and transition "
                "behavior, but not global admission over natural corpus histories."
            ),
        },
        "config": {
            "path_name": config.path.name,
            "sha256": config.file_sha256,
            "canonical_semantic_sha256": _canonical_sha256(
                json.loads(config.path.read_text(encoding="utf-8"))
            ),
            "seed": config.seed,
            "bootstrap_resamples": config.bootstrap_resamples,
            "ece_bins": config.ece_bins,
        },
        "primary_source": source,
        "dataset": data.payload,
        "artifacts": artifacts,
        "verifier": {
            "endpoint_classification": classification,
            "page_cluster_bootstrap": classification_bootstrap,
            "contrastive_transitions": transitions,
            "telemetry": verifier_telemetry,
        },
        "embedding": embedding,
        "derived_predictions": {
            "path_name": predictions_path.name,
            "sha256": predictions_sha,
            "contains_raw_dataset_text": False,
        },
        "execution": {
            "wall_seconds": time.perf_counter() - started,
            "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "threads": config.threads,
            "platform": platform.platform(),
            "dependencies": _dependencies(),
            "failures": 0,
            "timeouts": 0,
        },
    }
    report_path = output_directory / "report.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report_sha = file_sha256(report_path)
    manifest = {
        "schema_version": "groundloop-m4-vitaminc-gate-output-manifest-v1",
        "config_sha256": config.file_sha256,
        "predictions_sha256": predictions_sha,
        "report_sha256": report_sha,
        "gate_verdict": report["gate_verdict"],
    }
    manifest_path = output_directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        "report": report,
        "report_sha256": report_sha,
        "predictions_sha256": predictions_sha,
        "manifest_sha256": file_sha256(manifest_path),
        "output_directory": str(output_directory),
    }
