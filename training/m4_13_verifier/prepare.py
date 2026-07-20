#!/usr/bin/env python3
"""Prepare the frozen M4.13 change-aware verifier datasets.

The preparation stage is intentionally model-free.  It uses source labels only
for the pre-registered strata, quarantines normalized Wikipedia pages shared by
official splits, and writes the terminal reserve behind a separate sealed
artifact surface.  Exact hashes are configuration inputs, not values learned
from the local files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.ai.verification.data import normalize_pair_text
from groundloop.errors import ValidationError
from groundloop.m4.public_ai_gate import semantic_result_hash

_HEX = frozenset("0123456789abcdef")
_SOURCE_TO_STORED = {
    "SUPPORTS": "support",
    "REFUTES": "refute",
    "NOT ENOUGH INFO": "neutral",
}
_STRATA = {
    "support_refute": Counter({"SUPPORTS": 2, "REFUTES": 2}),
    "support_neutral": Counter({"SUPPORTS": 2, "NOT ENOUGH INFO": 2}),
}
_STRATA_ORDER = ("support_refute", "support_neutral")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(Mapping[str, object], value)


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValidationError(f"{key} must be a non-empty string")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if type(item) is not int:
        raise ValidationError(f"{key} must be an integer")
    return item


def _sha256(value: Mapping[str, object], key: str) -> str:
    item = _string(value, key)
    if len(item) != 64 or any(character not in _HEX for character in item):
        raise ValidationError(f"{key} must be a lowercase SHA-256 digest")
    return item


def _git_sha1(value: Mapping[str, object], key: str) -> str:
    item = _string(value, key)
    if len(item) != 40 or any(character not in _HEX for character in item):
        raise ValidationError(f"{key} must be a lowercase Git SHA-1")
    return item


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def normalized_identities_sha256(values: Sequence[str] | set[str]) -> str:
    return canonical_sha256(sorted(values))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_file_hash(path: Path, expected: str, name: str) -> str:
    if not path.is_file():
        raise ValidationError(f"missing {name}: {path}")
    observed = _file_sha256(path)
    if observed != expected:
        raise ValidationError(
            f"{name} SHA-256 mismatch: expected {expected}, observed {observed}"
        )
    return observed


@dataclass(frozen=True, slots=True)
class DataConfig:
    """Hash-bound preparation configuration."""

    path: Path
    file_sha256: str
    payload: Mapping[str, object]

    @classmethod
    def read(cls, path: Path) -> DataConfig:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError(f"cannot read M4.13 data config: {path}") from error
        payload = _mapping(raw, "M4.13 data config")
        if _string(payload, "schema_version") != (
            "groundloop-m4-change-aware-data-config-v1"
        ):
            raise ValidationError("unsupported M4.13 data config schema")
        if _integer(payload, "selection_seed") < 0:
            raise ValidationError("selection_seed must be non-negative")
        if _integer(payload, "terminal_reserve_seed") < 0:
            raise ValidationError("terminal_reserve_seed must be non-negative")
        source = _mapping(payload.get("source"), "source config")
        dataset = _mapping(payload.get("dataset"), "dataset config")
        m3 = _mapping(payload.get("m3"), "M3 config")
        m4_12 = _mapping(
            payload.get("m4_12_consumed_diagnostic"), "M4.12 config"
        )
        _mapping(payload.get("corrected_m4_10"), "corrected M4.10 config")
        _git_sha1(source, "revision")
        for mapping, keys in (
            (
                source,
                ("readme_sha256", "data_license_sha256", "code_license_sha256"),
            ),
            (
                dataset,
                (
                    "archive_sha256",
                    "train_sha256",
                    "development_sha256",
                    "test_sha256",
                    "embedded_license_sha256",
                    "embedded_readme_sha256",
                ),
            ),
            (
                m3,
                (
                    "prepared_manifest_sha256",
                    "checkpoint_tree_sha256",
                    "weights_sha256",
                    "calibration_file_sha256",
                ),
            ),
            (
                m4_12,
                (
                    "config_file_sha256",
                    "config_canonical_semantic_sha256",
                    "sample_manifest_sha256",
                    "semantic_result_sha256",
                    "predictions_sha256",
                ),
            ),
        ):
            for key in keys:
                _sha256(mapping, key)
        return cls(path=path, file_sha256=_file_sha256(path), payload=payload)

    @property
    def source(self) -> Mapping[str, object]:
        return _mapping(self.payload.get("source"), "source config")

    @property
    def dataset(self) -> Mapping[str, object]:
        return _mapping(self.payload.get("dataset"), "dataset config")

    @property
    def m3(self) -> Mapping[str, object]:
        return _mapping(self.payload.get("m3"), "M3 config")

    @property
    def m4_12(self) -> Mapping[str, object]:
        return _mapping(
            self.payload.get("m4_12_consumed_diagnostic"), "M4.12 config"
        )

    @property
    def corrected_m4_10(self) -> Mapping[str, object]:
        return _mapping(
            self.payload.get("corrected_m4_10"), "corrected M4.10 config"
        )


def validate_terminal_prerequisites(config: DataConfig) -> Mapping[str, str]:
    """Reject terminal evaluation when any corrected M4.10 identity is pending."""
    section = config.corrected_m4_10
    if _string(section, "status") != "resolved":
        raise ValidationError("corrected M4.10 terminal inputs are unresolved")
    required = (
        "config_sha256",
        "source_manifest_sha256",
        "model_identity_sha256",
        "raw_oracle_artifacts_sha256",
        "structural_result_sha256",
        "study_manifest_sha256",
        "report_sha256",
        "empirical_bundle_sha256",
    )
    result = {key: _sha256(section, key) for key in required}
    if _string(section, "config_schema_version") != (
        "groundloop-m4-real-git-histories-v2"
    ):
        raise ValidationError("corrected M4.10 config schema is not v2")
    return result


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
    def suffix(self) -> int:
        try:
            return int(self.unique_id.rsplit("_", 1)[1])
        except (IndexError, ValueError) as error:
            raise ValidationError(
                f"invalid unique_id suffix: {self.unique_id}"
            ) from error

    @property
    def stored_label(self) -> str:
        try:
            return _SOURCE_TO_STORED[self.source_label]
        except KeyError as error:
            raise ValidationError(
                f"unsupported VitaminC label: {self.source_label}"
            ) from error

    @classmethod
    def from_json(cls, raw: object) -> VitaminCRow:
        value = _mapping(raw, "VitaminC row")
        revision = value.get("wiki_revision_id", "")
        if not isinstance(revision, str):
            raise ValidationError("wiki_revision_id must be a string")
        result = cls(
            unique_id=_string(value, "unique_id"),
            case_id=_string(value, "case_id"),
            wiki_revision_id=revision,
            source_label=_string(value, "label"),
            claim=_string(value, "claim"),
            evidence=_string(value, "evidence"),
            page=_string(value, "page"),
            revision_type=_string(value, "revision_type"),
        )
        if result.source_label not in _SOURCE_TO_STORED:
            raise ValidationError(f"unsupported VitaminC label: {result.source_label}")
        if result.revision_type not in {"real", "synthetic"}:
            raise ValidationError("unsupported VitaminC revision_type")
        if result.revision_type == "real" and not result.wiki_revision_id:
            raise ValidationError("real VitaminC row lacks wiki_revision_id")
        return result


@dataclass(frozen=True, slots=True)
class EligibleCase:
    stratum: str
    case_id: str
    page: str
    rows: tuple[VitaminCRow, ...]


@dataclass(frozen=True, slots=True)
class SelectedCase:
    stratum: str
    selection_key: str
    case_id: str
    page: str
    rows: tuple[VitaminCRow, ...]


def _load_vitaminc_rows(path: Path) -> tuple[VitaminCRow, ...]:
    rows: list[VitaminCRow] = []
    try:
        source = path.open(encoding="utf-8")
    except OSError as error:
        raise ValidationError(f"cannot read VitaminC split: {path}") from error
    with source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                rows.append(VitaminCRow.from_json(json.loads(line)))
            except (json.JSONDecodeError, ValidationError) as error:
                raise ValidationError(f"malformed {path}:{line_number}") from error
    if not rows:
        raise ValidationError(f"empty VitaminC split: {path}")
    return tuple(rows)


def validate_atomic_case(
    rows: Sequence[VitaminCRow], *, stratum: str
) -> EligibleCase:
    """Validate the indivisible four-row VitaminC revision-case contract."""
    if stratum not in _STRATA:
        raise ValidationError(f"unsupported stratum: {stratum}")
    if len(rows) != 4:
        raise ValidationError("eligible VitaminC case must contain four rows")
    if len({row.case_id for row in rows}) != 1:
        raise ValidationError("eligible VitaminC case has multiple case IDs")
    if len({row.page for row in rows}) != 1:
        raise ValidationError("eligible VitaminC case has multiple raw pages")
    if len({normalize_pair_text(row.page) for row in rows}) != 1:
        raise ValidationError("eligible VitaminC case has multiple normalized pages")
    if {row.revision_type for row in rows} != {"real"}:
        raise ValidationError("eligible VitaminC case must be revision_type=real")
    if len({row.unique_id for row in rows}) != 4:
        raise ValidationError("eligible VitaminC case has duplicate unique IDs")
    by_suffix = {row.suffix: row for row in rows}
    if len(by_suffix) != 4 or set(by_suffix) != {1, 2, 3, 4}:
        raise ValidationError("eligible VitaminC suffixes must be exactly 1,2,3,4")
    if any(not row.unique_id.startswith(f"{row.case_id}_") for row in rows):
        raise ValidationError("VitaminC unique_id does not belong to its case_id")
    if not (
        by_suffix[1].claim == by_suffix[2].claim
        and by_suffix[3].claim == by_suffix[4].claim
        and by_suffix[1].evidence == by_suffix[3].evidence
        and by_suffix[2].evidence == by_suffix[4].evidence
        and len({row.claim for row in rows}) == 2
        and len({row.evidence for row in rows}) == 2
        and len({(row.claim, row.evidence) for row in rows}) == 4
    ):
        raise ValidationError("VitaminC case does not form the required 2x2 layout")
    for first, second in ((1, 2), (3, 4)):
        labels = {by_suffix[first].source_label, by_suffix[second].source_label}
        if "SUPPORTS" not in labels or len(labels) != 2:
            raise ValidationError("each VitaminC transition must change from SUPPORT")
    if Counter(row.source_label for row in rows) != _STRATA[stratum]:
        raise ValidationError("VitaminC case labels do not match its stratum")
    return EligibleCase(
        stratum=stratum,
        case_id=rows[0].case_id,
        page=rows[0].page,
        rows=tuple(rows),
    )


def _eligible_cases(
    rows: Sequence[VitaminCRow], *, excluded_pages: set[str]
) -> Mapping[str, tuple[EligibleCase, ...]]:
    grouped: defaultdict[str, list[VitaminCRow]] = defaultdict(list)
    for row in rows:
        grouped[row.case_id].append(row)
    candidates: dict[str, list[EligibleCase]] = {
        stratum: [] for stratum in _STRATA_ORDER
    }
    for case_rows in grouped.values():
        if len(case_rows) != 4:
            continue
        if normalize_pair_text(case_rows[0].page) in excluded_pages:
            continue
        observed = Counter(row.source_label for row in case_rows)
        for stratum, expected in _STRATA.items():
            if observed != expected:
                continue
            try:
                case = validate_atomic_case(case_rows, stratum=stratum)
            except ValidationError:
                continue
            candidates[stratum].append(case)
    return {key: tuple(value) for key, value in candidates.items()}


def deterministic_select(
    candidates: Mapping[str, Sequence[EligibleCase]],
    *,
    seed: int,
    cases_per_stratum: int,
    split: str | None,
    excluded_pages: set[str] | None = None,
) -> tuple[SelectedCase, ...]:
    """Select cases using source identities and labels, never model outputs."""
    if seed < 0 or cases_per_stratum <= 0:
        raise ValidationError("invalid deterministic selection parameters")
    used_pages = set() if excluded_pages is None else set(excluded_pages)
    selected: list[SelectedCase] = []
    for stratum in _STRATA_ORDER:
        ranked: list[SelectedCase] = []
        for candidate in candidates.get(stratum, ()):
            parts = [str(seed)]
            if split is not None:
                parts.append(split)
            parts.extend((stratum, candidate.page, candidate.case_id))
            selection_key = hashlib.sha256("\0".join(parts).encode()).hexdigest()
            ranked.append(
                SelectedCase(
                    stratum=stratum,
                    selection_key=selection_key,
                    case_id=candidate.case_id,
                    page=candidate.page,
                    rows=candidate.rows,
                )
            )
        ranked.sort(key=lambda item: item.selection_key)
        selected_in_stratum = 0
        for selected_case in ranked:
            normalized_page = normalize_pair_text(selected_case.page)
            if normalized_page in used_pages:
                continue
            used_pages.add(normalized_page)
            selected.append(selected_case)
            selected_in_stratum += 1
            if selected_in_stratum == cases_per_stratum:
                break
        if selected_in_stratum != cases_per_stratum:
            raise ValidationError(f"not enough eligible cases for {stratum}")
    return tuple(selected)


def identity_manifest(cases: Sequence[SelectedCase]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for case in cases:
        for row in sorted(case.rows, key=lambda item: item.unique_id):
            rows.append(
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
    return rows


def _materialized_rows(
    cases: Sequence[SelectedCase], *, split: str
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for case in cases:
        for row in sorted(case.rows, key=lambda item: item.unique_id):
            rows.append(
                {
                    "schema_version": "groundloop-m4-13-vitaminc-row-v1",
                    "split": split,
                    "stratum": case.stratum,
                    "selection_key": case.selection_key,
                    "unique_id": row.unique_id,
                    "case_id": row.case_id,
                    "page": row.page,
                    "normalized_page_sha256": hashlib.sha256(
                        normalize_pair_text(row.page).encode()
                    ).hexdigest(),
                    "wiki_revision_id": row.wiki_revision_id,
                    "revision_type": row.revision_type,
                    "source_label": row.source_label,
                    "label": row.stored_label,
                    "claim": row.claim,
                    "evidence": row.evidence,
                    "claim_sha256": hashlib.sha256(row.claim.encode()).hexdigest(),
                    "evidence_sha256": hashlib.sha256(
                        row.evidence.encode()
                    ).hexdigest(),
                }
            )
    return rows


def normalized_page_quarantine(
    split_rows: Mapping[str, Sequence[VitaminCRow]],
) -> tuple[set[str], Mapping[str, set[str]], Mapping[str, int]]:
    """Return the union of all normalized identities shared by official splits."""
    required = {"train", "development", "test"}
    if set(split_rows) != required:
        raise ValidationError("page quarantine requires train, development and test")
    pages = {
        split: {normalize_pair_text(row.page) for row in rows}
        for split, rows in split_rows.items()
    }
    intersections = {
        "train_development": pages["train"] & pages["development"],
        "train_test": pages["train"] & pages["test"],
        "development_test": pages["development"] & pages["test"],
    }
    quarantine = set().union(*intersections.values())
    counts = {key: len(value) for key, value in intersections.items()}
    counts["all_three"] = len(
        pages["train"] & pages["development"] & pages["test"]
    )
    counts["unique_quarantined"] = len(quarantine)
    return quarantine, pages, counts


def _page_audit(
    split_rows: Mapping[str, Sequence[VitaminCRow]], config: DataConfig
) -> tuple[Mapping[str, object], set[str], Mapping[str, set[str]]]:
    quarantine, pages, observed_counts = normalized_page_quarantine(split_rows)
    raw_pages = {
        split: {row.page for row in rows} for split, rows in split_rows.items()
    }
    dimensions = _mapping(config.dataset.get("dimensions"), "dataset dimensions")
    for split, rows in split_rows.items():
        expected = _mapping(dimensions.get(split), f"{split} dimensions")
        observed = {
            "rows": len(rows),
            "cases": len({row.case_id for row in rows}),
            "raw_pages": len(raw_pages[split]),
            "normalized_pages": len(pages[split]),
        }
        for key, count in observed.items():
            if count != _integer(expected, key):
                raise ValidationError(
                    f"VitaminC {split} {key} drifted: {count}"
                )
    expected_intersections = _mapping(
        config.dataset.get("normalized_page_intersections"),
        "normalized page intersections",
    )
    for key, count in observed_counts.items():
        if count != _integer(expected_intersections, key):
            raise ValidationError(f"normalized-page {key} drifted: {count}")
    quarantine_hash = normalized_identities_sha256(quarantine)
    if quarantine_hash != _sha256(expected_intersections, "quarantine_sha256"):
        raise ValidationError("normalized-page quarantine identity drifted")
    return (
        {
            "normalization": "strip, collapse whitespace, casefold",
            "dimensions": {
                split: {
                    "rows": len(rows),
                    "cases": len({row.case_id for row in rows}),
                    "raw_pages": len(raw_pages[split]),
                    "normalized_pages": len(pages[split]),
                }
                for split, rows in split_rows.items()
            },
            "intersections": observed_counts,
            "quarantined_normalized_pages": len(quarantine),
            "quarantine_sha256": quarantine_hash,
        },
        quarantine,
        pages,
    )


def _selection_audit(
    cases: Sequence[SelectedCase], expected: Mapping[str, object]
) -> Mapping[str, object]:
    manifest = identity_manifest(cases)
    manifest_hash = canonical_sha256(manifest)
    labels = Counter(row["label"] for row in manifest)
    pages = {normalize_pair_text(case.page) for case in cases}
    observed = {
        "cases": len(cases),
        "rows": len(manifest),
        "pages": len(pages),
        "support": labels["SUPPORTS"],
        "refute": labels["REFUTES"],
        "neutral": labels["NOT ENOUGH INFO"],
    }
    for key, count in observed.items():
        if count != _integer(expected, key):
            raise ValidationError(f"selected {key} drifted: {count}")
    if manifest_hash != _sha256(expected, "manifest_sha256"):
        raise ValidationError(f"selected manifest identity drifted: {manifest_hash}")
    return {
        **observed,
        "manifest_sha256": manifest_hash,
        "uses_model_outputs": False,
        "one_case_per_normalized_page": True,
    }


def _git_head(repository: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValidationError(
            f"cannot inspect source repository: {repository}"
        ) from error
    return result.stdout.strip()


def _validate_sources(
    *,
    official_repository: Path,
    archive: Path,
    vitaminc_root: Path,
    config: DataConfig,
) -> Mapping[str, object]:
    if _git_head(official_repository) != _string(config.source, "revision"):
        raise ValidationError("VitaminC source checkout revision drifted")
    source_files = {
        "README.md": _require_file_hash(
            official_repository / "README.md",
            _sha256(config.source, "readme_sha256"),
            "VitaminC source README",
        ),
        "DATA_LICENSE": _require_file_hash(
            official_repository / "DATA_LICENSE",
            _sha256(config.source, "data_license_sha256"),
            "VitaminC source data license",
        ),
        "LICENSE": _require_file_hash(
            official_repository / "LICENSE",
            _sha256(config.source, "code_license_sha256"),
            "VitaminC source code license",
        ),
    }
    dataset_files = {
        "archive": _require_file_hash(
            archive, _sha256(config.dataset, "archive_sha256"), "VitaminC archive"
        ),
        "train.jsonl": _require_file_hash(
            vitaminc_root / "train.jsonl",
            _sha256(config.dataset, "train_sha256"),
            "VitaminC train",
        ),
        "dev.jsonl": _require_file_hash(
            vitaminc_root / "dev.jsonl",
            _sha256(config.dataset, "development_sha256"),
            "VitaminC development",
        ),
        "test.jsonl": _require_file_hash(
            vitaminc_root / "test.jsonl",
            _sha256(config.dataset, "test_sha256"),
            "VitaminC test",
        ),
        "LICENSE": _require_file_hash(
            vitaminc_root / "LICENSE",
            _sha256(config.dataset, "embedded_license_sha256"),
            "VitaminC embedded license",
        ),
        "README.txt": _require_file_hash(
            vitaminc_root / "README.txt",
            _sha256(config.dataset, "embedded_readme_sha256"),
            "VitaminC embedded README",
        ),
    }
    return {
        "official_repository": _string(config.source, "repository"),
        "official_repository_revision": _string(config.source, "revision"),
        "source_file_sha256": source_files,
        "archive_url": _string(config.dataset, "archive_url"),
        "hosting_repository_revision": _string(
            config.dataset, "hosting_repository_revision"
        ),
        "dataset_file_sha256": dataset_files,
        "revision_type_used": "real",
        "license_note": (
            "Wikipedia article terms or CC BY-SA 3.0 where unavailable; "
            "synthetic annotations also derive from FEVER. Preparation uses "
            "only revision_type=real."
        ),
    }


def _read_jsonl_mappings(path: Path, name: str) -> list[Mapping[str, object]]:
    rows: list[Mapping[str, object]] = []
    try:
        source = path.open(encoding="utf-8")
    except OSError as error:
        raise ValidationError(f"cannot read {name}: {path}") from error
    with source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                rows.append(_mapping(json.loads(line), f"{name} row {line_number}"))
            except json.JSONDecodeError as error:
                raise ValidationError(f"malformed {name}:{line_number}") from error
    if not rows:
        raise ValidationError(f"{name} is empty")
    return rows


def exact_normalized_overlap(
    vitamin_rows: Sequence[VitaminCRow], m3_rows: Sequence[Mapping[str, object]]
) -> Mapping[str, int]:
    vitamin_claims = {normalize_pair_text(row.claim) for row in vitamin_rows}
    vitamin_evidence = {normalize_pair_text(row.evidence) for row in vitamin_rows}
    vitamin_pairs = {
        (normalize_pair_text(row.claim), normalize_pair_text(row.evidence))
        for row in vitamin_rows
    }
    m3_claims = {normalize_pair_text(_string(row, "claim")) for row in m3_rows}
    m3_evidence = {
        normalize_pair_text(_string(row, "evidence")) for row in m3_rows
    }
    m3_pairs = {
        (
            normalize_pair_text(_string(row, "claim")),
            normalize_pair_text(_string(row, "evidence")),
        )
        for row in m3_rows
    }
    return {
        "normalized_claims": len(vitamin_claims & m3_claims),
        "normalized_evidence": len(vitamin_evidence & m3_evidence),
        "normalized_pairs": len(vitamin_pairs & m3_pairs),
    }


def _validate_m3(
    m3_artifact_root: Path, config: DataConfig
) -> tuple[Mapping[str, object], Mapping[str, list[Mapping[str, object]]]]:
    prepared = m3_artifact_root / "prepared"
    manifest_path = prepared / "manifest.json"
    _require_file_hash(
        manifest_path,
        _sha256(config.m3, "prepared_manifest_sha256"),
        "M3 prepared manifest",
    )
    manifest = _mapping(json.loads(manifest_path.read_text()), "M3 manifest")
    split_rows: dict[str, list[Mapping[str, object]]] = {}
    audit: dict[str, object] = {}
    for split, output_name in (
        ("train", "train_m3_replay.jsonl"),
        ("development", "development_m3.jsonl"),
        ("test", "test_m3.jsonl"),
    ):
        expected = _mapping(config.m3.get(split), f"M3 {split} config")
        path = prepared / f"{split}.jsonl"
        digest = _require_file_hash(
            path, _sha256(expected, "sha256"), f"M3 {split}"
        )
        rows = _read_jsonl_mappings(path, f"M3 {split}")
        groups = {_string(row, "claim_group_id") for row in rows}
        if len(rows) != _integer(expected, "rows"):
            raise ValidationError(f"M3 {split} row count drifted")
        if len(groups) != _integer(expected, "claim_groups"):
            raise ValidationError(f"M3 {split} claim-group count drifted")
        label_counts = Counter(_string(row, "label") for row in rows)
        if split == "test":
            for label in ("support", "refute", "neutral"):
                if label_counts[label] != _integer(expected, label):
                    raise ValidationError(f"M3 test {label} count drifted")
        split_rows[split] = rows
        audit[split] = {
            "source_name": f"{split}.jsonl",
            "output_name": output_name,
            "sha256": digest,
            "rows": len(rows),
            "claim_groups": len(groups),
            "label_counts": dict(sorted(label_counts.items())),
        }
    checksums = _mapping(manifest.get("checksums"), "M3 manifest checksums")
    for split in ("train", "development", "test"):
        expected = _mapping(config.m3.get(split), f"M3 {split} config")
        if _string(checksums, f"{split}.jsonl") != _sha256(expected, "sha256"):
            raise ValidationError(f"M3 manifest checksum for {split} drifted")
    return (
        {
            "prepared_manifest_sha256": _sha256(
                config.m3, "prepared_manifest_sha256"
            ),
            "splits": audit,
            "replay_contract": (
                "all 3,022 M3 train rows once per mixed epoch; complete development "
                "for selection/calibration; public test terminal-only"
            ),
        },
        split_rows,
    )


def _validate_m4_12(
    *,
    config_path: Path,
    artifact_root: Path,
    consumed_cases: Sequence[SelectedCase],
    config: DataConfig,
) -> Mapping[str, object]:
    expected = config.m4_12
    _require_file_hash(
        config_path,
        _sha256(expected, "config_file_sha256"),
        "M4.12 configuration",
    )
    try:
        report = _mapping(
            json.loads((artifact_root / "report.json").read_text(encoding="utf-8")),
            "M4.12 report",
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError("cannot read M4.12 report") from error
    observed_semantic = semantic_result_hash(report)
    if observed_semantic != _sha256(expected, "semantic_result_sha256"):
        raise ValidationError("M4.12 canonical semantic result drifted")
    report_config = _mapping(report.get("config"), "M4.12 report config")
    if _string(report_config, "sha256") != _sha256(
        expected, "config_file_sha256"
    ) or _string(report_config, "canonical_semantic_sha256") != _sha256(
        expected, "config_canonical_semantic_sha256"
    ):
        raise ValidationError("M4.12 report configuration identity drifted")
    predictions_path = artifact_root / "predictions.jsonl"
    prediction_digest = _require_file_hash(
        predictions_path,
        _sha256(expected, "predictions_sha256"),
        "M4.12 predictions",
    )
    predictions = _read_jsonl_mappings(predictions_path, "M4.12 predictions")
    expected_rows = {
        row["unique_id"]: row for row in identity_manifest(consumed_cases)
    }
    if len(predictions) != len(expected_rows) or len(predictions) != _integer(
        expected, "rows"
    ):
        raise ValidationError("M4.12 prediction completeness drifted")
    seen: set[str] = set()
    for prediction in predictions:
        unique_id = _string(prediction, "unique_id")
        if unique_id in seen or unique_id not in expected_rows:
            raise ValidationError("M4.12 prediction sample alignment drifted")
        seen.add(unique_id)
        identity = expected_rows[unique_id]
        comparisons = {
            "case_id": _string(prediction, "case_id"),
            "stratum": _string(prediction, "stratum"),
            "selection_key": _string(prediction, "selection_key"),
            "wiki_revision_id": _string(prediction, "wiki_revision_id"),
            "claim_sha256": _string(prediction, "claim_sha256"),
            "evidence_sha256": _string(prediction, "evidence_sha256"),
        }
        for key, observed in comparisons.items():
            if observed != identity[key]:
                raise ValidationError(f"M4.12 prediction {key} alignment drifted")
        if _string(prediction, "human_label") != _SOURCE_TO_STORED[
            str(identity["label"])
        ]:
            raise ValidationError("M4.12 prediction label alignment drifted")
    execution = _mapping(report.get("execution"), "M4.12 execution")
    if _integer(execution, "failures") != 0 or _integer(execution, "timeouts") != 0:
        raise ValidationError("M4.12 did not complete without failures/timeouts")
    manifest_hash = canonical_sha256(identity_manifest(consumed_cases))
    if manifest_hash != _sha256(expected, "sample_manifest_sha256"):
        raise ValidationError("consumed M4.12 sample identity drifted")
    return {
        "role": _string(expected, "role"),
        "config_file_sha256": _sha256(expected, "config_file_sha256"),
        "config_canonical_semantic_sha256": _sha256(
            expected, "config_canonical_semantic_sha256"
        ),
        "sample_manifest_sha256": manifest_hash,
        "semantic_result_sha256": observed_semantic,
        "predictions_sha256": prediction_digest,
        "rows": len(predictions),
        "cases": len(consumed_cases),
        "pages": len({normalize_pair_text(case.page) for case in consumed_cases}),
        "failures": 0,
        "timeouts": 0,
        "used_for_selection": False,
    }


def _write_json(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(value))
    return _file_sha256(path)


def _write_jsonl(path: Path, values: Sequence[Mapping[str, object]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as target:
        for value in values:
            target.write(json.dumps(value, sort_keys=True, separators=(",", ":")))
            target.write("\n")
    return _file_sha256(path)


def _copy_exact(source: Path, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    source_hash = _file_sha256(source)
    if _file_sha256(destination) != source_hash:
        raise ValidationError(f"copy integrity failure: {destination}")
    return source_hash


def prepare(
    *,
    official_repository: Path,
    vitaminc_archive: Path,
    vitaminc_root: Path,
    m3_artifact_root: Path,
    m4_12_config_path: Path,
    m4_12_artifact_root: Path,
    output_root: Path,
    config_path: Path,
) -> Mapping[str, object]:
    """Validate all frozen inputs and materialize disjoint external artifacts."""
    config = DataConfig.read(config_path)
    terminal_prerequisites = validate_terminal_prerequisites(config)
    source_audit = _validate_sources(
        official_repository=official_repository,
        archive=vitaminc_archive,
        vitaminc_root=vitaminc_root,
        config=config,
    )
    split_rows = {
        "train": _load_vitaminc_rows(vitaminc_root / "train.jsonl"),
        "development": _load_vitaminc_rows(vitaminc_root / "dev.jsonl"),
        "test": _load_vitaminc_rows(vitaminc_root / "test.jsonl"),
    }
    page_audit, quarantine, normalized_pages = _page_audit(split_rows, config)
    candidate_expectations = _mapping(
        config.dataset.get("candidate_case_counts_after_quarantine"),
        "candidate case counts",
    )
    sample_expectations = _mapping(config.dataset.get("samples"), "samples")
    selected: dict[str, tuple[SelectedCase, ...]] = {}
    selection_audits: dict[str, Mapping[str, object]] = {}
    for split in ("train", "development"):
        candidates = _eligible_cases(split_rows[split], excluded_pages=quarantine)
        expected_candidates = _mapping(
            candidate_expectations.get(split), f"{split} candidate counts"
        )
        for stratum in _STRATA_ORDER:
            if len(candidates[stratum]) != _integer(expected_candidates, stratum):
                raise ValidationError(f"{split} {stratum} candidate pool drifted")
        expected_sample = _mapping(
            sample_expectations.get(split), f"{split} sample"
        )
        selection = deterministic_select(
            candidates,
            seed=_integer(config.payload, "selection_seed"),
            cases_per_stratum=_integer(expected_sample, "cases_per_stratum"),
            split=split,
        )
        selected[split] = selection
        selection_audits[split] = {
            **_selection_audit(selection, expected_sample),
            "candidate_cases": {
                stratum: len(candidates[stratum]) for stratum in _STRATA_ORDER
            },
        }

    test_excluded = normalized_pages["train"] | normalized_pages["development"]
    test_candidates = _eligible_cases(split_rows["test"], excluded_pages=test_excluded)
    consumed = deterministic_select(
        test_candidates,
        seed=_integer(config.payload, "selection_seed"),
        cases_per_stratum=64,
        split=None,
    )
    consumed_pages = {normalize_pair_text(case.page) for case in consumed}
    m4_12_audit = _validate_m4_12(
        config_path=m4_12_config_path,
        artifact_root=m4_12_artifact_root,
        consumed_cases=consumed,
        config=config,
    )

    terminal_expected = _mapping(
        config.dataset.get("terminal_reserve"), "terminal reserve"
    )
    consumed_page_digest = normalized_identities_sha256(consumed_pages)
    if consumed_page_digest != _sha256(
        terminal_expected, "consumed_page_digest_sha256"
    ):
        raise ValidationError("consumed M4.12 page digest drifted")
    pool_expected = _mapping(
        terminal_expected.get("candidate_case_counts_after_exclusions"),
        "terminal candidate counts",
    )
    for stratum in _STRATA_ORDER:
        available = sum(
            normalize_pair_text(case.page) not in consumed_pages
            for case in test_candidates[stratum]
        )
        if available != _integer(pool_expected, stratum):
            raise ValidationError(f"terminal {stratum} candidate pool drifted")
    terminal = deterministic_select(
        test_candidates,
        seed=_integer(config.payload, "terminal_reserve_seed"),
        cases_per_stratum=_integer(terminal_expected, "cases_per_stratum"),
        split=None,
        excluded_pages=consumed_pages,
    )
    terminal_audit = _selection_audit(terminal, terminal_expected)
    terminal_pages = {normalize_pair_text(case.page) for case in terminal}
    terminal_page_digest = normalized_identities_sha256(terminal_pages)
    if terminal_page_digest != _sha256(
        terminal_expected, "reserve_page_digest_sha256"
    ):
        raise ValidationError("terminal reserve page identity drifted")
    if terminal_pages & consumed_pages:
        raise ValidationError("terminal reserve overlaps consumed M4.12 pages")

    m3_audit, m3_rows = _validate_m3(m3_artifact_root, config)
    all_m3_rows = [row for rows in m3_rows.values() for row in rows]
    full_test_overlap = exact_normalized_overlap(split_rows["test"], all_m3_rows)
    terminal_rows = tuple(row for case in terminal for row in case.rows)
    terminal_m3_overlap = exact_normalized_overlap(terminal_rows, all_m3_rows)
    if any(full_test_overlap.values()) or any(terminal_m3_overlap.values()):
        raise ValidationError("VitaminC test or reserve overlaps M3 prepared data")

    if output_root.exists():
        raise ValidationError(f"output root already exists: {output_root}")
    staging = output_root.with_name(f".{output_root.name}.tmp-{uuid.uuid4().hex}")
    try:
        prepared = staging / "prepared"
        sealed = staging / "sealed"
        source_directory = staging / "source"
        prepared.mkdir(parents=True)
        sealed.mkdir(parents=True)
        source_directory.mkdir(parents=True)

        artifact_hashes: dict[str, str] = {}
        for split in ("train", "development"):
            row_path = prepared / f"{split}_vitaminc.jsonl"
            manifest_path = prepared / f"{split}_vitaminc_manifest.json"
            artifact_hashes[row_path.name] = _write_jsonl(
                row_path, _materialized_rows(selected[split], split=split)
            )
            artifact_hashes[manifest_path.name] = _write_json(
                manifest_path, identity_manifest(selected[split])
            )
            expected_hash = _sha256(
                _mapping(sample_expectations.get(split), f"{split} sample"),
                "manifest_sha256",
            )
            if artifact_hashes[manifest_path.name] != expected_hash:
                raise ValidationError(f"written {split} manifest hash drifted")

        m3_copy_sources = {
            "train_m3_replay.jsonl": m3_artifact_root / "prepared" / "train.jsonl",
            "development_m3.jsonl": (
                m3_artifact_root / "prepared" / "development.jsonl"
            ),
            "test_m3.jsonl": m3_artifact_root / "prepared" / "test.jsonl",
        }
        for name, source in m3_copy_sources.items():
            artifact_hashes[name] = _copy_exact(source, prepared / name)

        terminal_materialized = _materialized_rows(terminal, split="terminal")
        terminal_jsonl_hash = _write_jsonl(
            sealed / "terminal_reserve.jsonl", terminal_materialized
        )
        terminal_manifest_hash = _write_json(
            sealed / "terminal_reserve_manifest.json", identity_manifest(terminal)
        )
        if terminal_manifest_hash != _sha256(
            terminal_expected, "manifest_sha256"
        ):
            raise ValidationError("written terminal reserve manifest hash drifted")
        terminal_identity = {
            "schema_version": "groundloop-m4-13-terminal-reserve-identity-v1",
            "role": "evaluator-only pre-frozen page-disjoint terminal diagnostic",
            "representativeness_note": (
                "not an untouched representative benchmark; M4.12 consumed a "
                "sibling label-stratified sample from the official test split"
            ),
            "seed": _integer(config.payload, "terminal_reserve_seed"),
            "strata_order": list(_STRATA_ORDER),
            "reserve_jsonl_sha256": terminal_jsonl_hash,
            "manifest_sha256": terminal_manifest_hash,
            "rows": terminal_audit["rows"],
            "cases": terminal_audit["cases"],
            "pages": terminal_audit["pages"],
            "class_counts": {
                "support": terminal_audit["support"],
                "refute": terminal_audit["refute"],
                "neutral": terminal_audit["neutral"],
            },
            "consumed_m4_12_page_digest_sha256": consumed_page_digest,
            "reserve_page_digest_sha256": terminal_page_digest,
            "groundloop_m3_exact_normalized_overlap": terminal_m3_overlap,
            "corrected_m4_10_terminal_inputs": terminal_prerequisites,
        }
        terminal_identity_hash = _write_json(
            sealed / "terminal_reserve_identity.json", terminal_identity
        )

        source_manifest = {
            "schema_version": "groundloop-m4-13-source-manifest-v1",
            "config_sha256": config.file_sha256,
            "source": source_audit,
            "normalized_page_audit": page_audit,
            "groundloop_m3": m3_audit,
            "m4_12_consumed_diagnostic": m4_12_audit,
            "m4_10_terminal_reference": terminal_prerequisites,
            "scientific_boundary": (
                "source labels supervise a bounded neural experiment; they do not "
                "alter GroundLoop's exact IVM correctness boundary"
            ),
        }
        source_manifest_hash = _write_json(
            source_directory / "source_manifest.json", source_manifest
        )

        dataset_manifest = {
            "schema_version": "groundloop-m4-13-dataset-manifest-v1",
            "config_sha256": config.file_sha256,
            "selection": {
                "seed": _integer(config.payload, "selection_seed"),
                "key": (
                    "sha256(seed || NUL || split || NUL || stratum || NUL || "
                    "raw_page || NUL || case_id)"
                ),
                "strata_order": list(_STRATA_ORDER),
                "uses_model_outputs": False,
                "revision_type": "real",
                "normalized_page_quarantine_sha256": page_audit[
                    "quarantine_sha256"
                ],
                "train": selection_audits["train"],
                "development": selection_audits["development"],
            },
            "label_mapping": {
                "SUPPORTS": {"stored": "support", "base_logit_index": 1},
                "REFUTES": {"stored": "refute", "base_logit_index": 0},
                "NOT ENOUGH INFO": {"stored": "neutral", "base_logit_index": 2},
                "stored_probability_order": ["support", "refute", "neutral"],
                "base_logit_order": ["contradiction", "entailment", "neutral"],
            },
            "training_surface": {
                "vitaminc_row_schema": "groundloop-m4-13-vitaminc-row-v1",
                "artifacts": {
                    key: value
                    for key, value in artifact_hashes.items()
                    if key != "test_m3.jsonl"
                },
                "m3": {
                    "prepared_manifest_sha256": m3_audit[
                        "prepared_manifest_sha256"
                    ],
                    "replay_contract": m3_audit["replay_contract"],
                    "splits": {
                        key: value
                        for key, value in cast(
                            Mapping[str, object], m3_audit["splits"]
                        ).items()
                        if key in {"train", "development"}
                    },
                },
                "terminal_paths_exposed": False,
                "accepts_test_path": False,
            },
            "sealed_terminal_reference": {
                "manifest_sha256": terminal_manifest_hash,
                "identity_sha256": terminal_identity_hash,
                "rows": terminal_audit["rows"],
                "cases": terminal_audit["cases"],
                "pages": terminal_audit["pages"],
                "normalized_page_exclusion_digest_sha256": terminal_page_digest,
                "contains_terminal_row_ids_text_or_labels": False,
                "original_m3_test": {
                    "sha256": _sha256(
                        _mapping(config.m3.get("test"), "M3 test config"),
                        "sha256",
                    ),
                    "rows": _integer(
                        _mapping(config.m3.get("test"), "M3 test config"),
                        "rows",
                    ),
                    "claim_groups": _integer(
                        _mapping(config.m3.get("test"), "M3 test config"),
                        "claim_groups",
                    ),
                },
            },
            "consumed_m4_12_reference": {
                "sample_manifest_sha256": m4_12_audit["sample_manifest_sha256"],
                "semantic_result_sha256": m4_12_audit["semantic_result_sha256"],
                "predictions_sha256": m4_12_audit["predictions_sha256"],
                "used_for_selection": False,
            },
            "full_vitaminc_test_m3_exact_normalized_overlap": full_test_overlap,
            "source_manifest_sha256": source_manifest_hash,
        }
        dataset_manifest_hash = _write_json(
            prepared / "dataset_manifest.json", dataset_manifest
        )
        staging.rename(output_root)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return {
        "schema_version": "groundloop-m4-13-preparation-result-v1",
        "config_sha256": config.file_sha256,
        "source_manifest_sha256": source_manifest_hash,
        "dataset_manifest_sha256": dataset_manifest_hash,
        "train_vitaminc_manifest_sha256": selection_audits["train"][
            "manifest_sha256"
        ],
        "development_vitaminc_manifest_sha256": selection_audits["development"]
        ["manifest_sha256"],
        "terminal_reserve_manifest_sha256": terminal_manifest_hash,
        "terminal_reserve_identity_sha256": terminal_identity_hash,
        "terminal_reserve_written_to_sealed_surface": True,
        "terminal_reserve_used_for_model_selection": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--official-repository", type=Path, required=True)
    parser.add_argument("--vitaminc-archive", type=Path, required=True)
    parser.add_argument("--vitaminc-root", type=Path, required=True)
    parser.add_argument("--m3-artifact-root", type=Path, required=True)
    parser.add_argument("--m4-12-config", type=Path, required=True)
    parser.add_argument("--m4-12-artifact-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m4/verifier/change_aware_v1.json"),
    )
    return parser


def main() -> None:
    arguments = _parser().parse_args()
    result = prepare(
        official_repository=arguments.official_repository,
        vitaminc_archive=arguments.vitaminc_archive,
        vitaminc_root=arguments.vitaminc_root,
        m3_artifact_root=arguments.m3_artifact_root,
        m4_12_config_path=arguments.m4_12_config,
        m4_12_artifact_root=arguments.m4_12_artifact_root,
        output_root=arguments.output_root,
        config_path=arguments.config,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
