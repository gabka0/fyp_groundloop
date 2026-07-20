#!/usr/bin/env python3
"""Equal-domain, group-balanced calibration for selected M4.13 checkpoints."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal, cast

from groundloop.ai.verification.artifacts import file_sha256
from groundloop.errors import ValidationError

from .losses import (
    BASE_LOGIT_ORDER,
    STORED_LABEL_ORDER,
    normalize_stored_label,
    stored_label_to_base_index,
)
from .train import (
    CONFIG_SCHEMA,
    DATASET_MANIFEST_SCHEMA,
    FROZEN_PRIMARY_SEED,
    _integer,
    _load_json,
    _load_jsonl,
    _mapping,
    _run_git,
    _string,
    _validate_dataset_manifest,
    load_completed_training_run,
)

DEVELOPMENT_LOGIT_SCHEMA = "groundloop-m4-13-development-logit-v1"
SELECTION_SCHEMA = "groundloop-m4-13-selection-v1"
CALIBRATION_SCHEMA = "groundloop-m4-13-group-balanced-temperature-v1"
CALIBRATION_METHOD = "equal-domain-group-balanced-log-temperature-golden-v1"
CALIBRATOR_IMPLEMENTATION_SCHEMA = (
    "groundloop-m4-13-calibrator-implementation-v1"
)
OLD_M3_TEMPERATURE = 1.1037657679769346

_CALIBRATOR_IMPLEMENTATION_PATHS = (
    "training/m4_13_verifier/calibrate.py",
    "training/m4_13_verifier/losses.py",
    "training/m4_13_verifier/train.py",
    "training/m4_13_verifier/__init__.py",
    "src/groundloop/ai/verification/artifacts.py",
    "src/groundloop/errors.py",
)


class CalibrationFailureStage(StrEnum):
    BEFORE_RESULT_WRITE = "before_result_write"
    BEFORE_RESULT_SEALING = "before_result_sealing"


@dataclass(frozen=True, slots=True)
class DevelopmentLogit:
    domain: Literal["m3", "vitaminc"]
    row_id: str
    group_id: str
    stored_label: str
    claim_sha256: str
    evidence_sha256: str
    logits: tuple[float, float, float]
    checkpoint_tree_sha256: str
    variant: str
    seed: int

    @property
    def base_label(self) -> int:
        return stored_label_to_base_index(self.stored_label)


@dataclass(frozen=True, slots=True)
class DomainNll:
    m3: float
    vitaminc: float
    combined: float


@dataclass(frozen=True, slots=True)
class CalibrationFit:
    candidate_temperature: float
    deployed_temperature: float
    accepted: bool
    rejection_reasons: tuple[str, ...]
    nll_before: DomainNll
    nll_candidate: DomainNll
    nll_deployed: DomainNll


@dataclass(frozen=True, slots=True)
class CalibrationRepositoryProvenance:
    git_head: str
    dirty: bool
    implementation_files_sha256: Mapping[str, str]
    implementation_sha256: str

    def to_manifest(self) -> dict[str, object]:
        return {
            "repository": {
                "git_head": self.git_head,
                "dirty": self.dirty,
            },
            "calibrator_implementation": {
                "schema_version": CALIBRATOR_IMPLEMENTATION_SCHEMA,
                "files_sha256": dict(self.implementation_files_sha256),
                "sha256": self.implementation_sha256,
            },
        }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def validate_calibration_repository_provenance(
    repository_root: Path,
) -> CalibrationRepositoryProvenance:
    """Bind one clean Git commit and every local calibrator dependency."""
    supplied_root = repository_root.resolve()
    discovered = Path(
        _run_git(supplied_root, "rev-parse", "--show-toplevel").strip()
    ).resolve()
    if discovered != supplied_root:
        raise ValidationError(
            "calibration repository root must be the exact Git worktree root"
        )
    git_head = _run_git(discovered, "rev-parse", "HEAD").strip()
    if len(git_head) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in git_head
    ):
        raise ValidationError(
            "calibration repository HEAD is not a canonical Git digest"
        )
    status = _run_git(
        discovered,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if status:
        raise ValidationError(
            "real calibration requires a clean repository with no tracked or "
            "nonignored untracked changes"
        )
    files: dict[str, str] = {}
    for relative in _CALIBRATOR_IMPLEMENTATION_PATHS:
        path = discovered / relative
        if not path.is_file():
            raise ValidationError(
                f"calibrator implementation dependency is absent: {relative}"
            )
        files[relative] = file_sha256(path)
    implementation = {
        "schema_version": CALIBRATOR_IMPLEMENTATION_SCHEMA,
        "files_sha256": files,
    }
    return CalibrationRepositoryProvenance(
        git_head=git_head,
        dirty=False,
        implementation_files_sha256=files,
        implementation_sha256=_canonical_sha256(implementation),
    )


def _load_development_logits(path: Path) -> tuple[DevelopmentLogit, ...]:
    if not path.is_file():
        raise FileNotFoundError(f"development logits are absent: {path}")
    rows: list[DevelopmentLogit] = []
    identities: set[tuple[str, str]] = set()
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        try:
            value = _mapping(json.loads(line), f"development logit row {line_number}")
        except json.JSONDecodeError as error:
            raise ValidationError(
                f"development logit row {line_number} is invalid JSON"
            ) from error
        if _string(value, "schema_version") != DEVELOPMENT_LOGIT_SCHEMA:
            raise ValidationError("unsupported development-logit schema")
        if _string(value, "split") != "development":
            raise ValidationError("calibration accepts development logits only")
        domain = _string(value, "domain")
        if domain not in {"m3", "vitaminc"}:
            raise ValidationError("unsupported calibration domain")
        base_order = value.get("base_logit_order")
        if not isinstance(base_order, list) or tuple(base_order) != BASE_LOGIT_ORDER:
            raise ValidationError("development logits changed the base-logit order")
        raw_logits = value.get("logits")
        if not isinstance(raw_logits, list) or len(raw_logits) != 3:
            raise ValidationError("development logits must contain three values")
        try:
            logits = cast(
                tuple[float, float, float], tuple(float(item) for item in raw_logits)
            )
        except (TypeError, ValueError) as error:
            raise ValidationError("development logits are not numeric") from error
        if any(not math.isfinite(item) for item in logits):
            raise ValidationError("development logits must be finite")
        row_id = _string(value, "row_id")
        key = (domain, row_id)
        if key in identities:
            raise ValidationError("development logits contain a duplicate row")
        identities.add(key)
        rows.append(
            DevelopmentLogit(
                cast(Literal["m3", "vitaminc"], domain),
                row_id,
                _string(value, "group_id"),
                normalize_stored_label(_string(value, "label")),
                _sha256_field(value, "claim_sha256"),
                _sha256_field(value, "evidence_sha256"),
                logits,
                _string(value, "checkpoint_tree_sha256"),
                _string(value, "variant"),
                _integer(value, "seed"),
            )
        )
    if not rows:
        raise ValidationError("development logits are empty")
    if {row.domain for row in rows} != {"m3", "vitaminc"}:
        raise ValidationError("calibration requires both development domains")
    return tuple(rows)


def _sha256_field(value: Mapping[str, object], key: str) -> str:
    result = _string(value, key)
    if len(result) != 64 or any(
        character not in "0123456789abcdef" for character in result
    ):
        raise ValidationError(f"{key} must be a lowercase SHA-256 digest")
    return result


def _row_nll(row: DevelopmentLogit, temperature: float) -> float:
    scaled = tuple(value / temperature for value in row.logits)
    maximum = max(scaled)
    log_denominator = maximum + math.log(
        sum(math.exp(value - maximum) for value in scaled)
    )
    return log_denominator - scaled[row.base_label]


def group_balanced_nll(
    rows: Sequence[DevelopmentLogit], temperature: float
) -> DomainNll:
    """Weight each domain equally and each claim group/case equally within it."""
    if not math.isfinite(temperature) or temperature <= 0:
        raise ValidationError("temperature must be finite and positive")
    grouped: dict[str, defaultdict[str, list[float]]] = {
        "m3": defaultdict(list),
        "vitaminc": defaultdict(list),
    }
    for row in rows:
        grouped[row.domain][row.group_id].append(_row_nll(row, temperature))
    domain_values: dict[str, float] = {}
    for domain in ("m3", "vitaminc"):
        if not grouped[domain]:
            raise ValidationError(f"calibration domain is empty: {domain}")
        group_means = [sum(values) / len(values) for values in grouped[domain].values()]
        domain_values[domain] = sum(group_means) / len(group_means)
    return DomainNll(
        domain_values["m3"],
        domain_values["vitaminc"],
        0.5 * domain_values["m3"] + 0.5 * domain_values["vitaminc"],
    )


def fit_group_balanced_temperature(
    rows: Sequence[DevelopmentLogit],
    *,
    lower: float = 0.05,
    upper: float = 20.0,
    iterations: int = 96,
    maximum_domain_increase: float = 0.01,
) -> CalibrationFit:
    """Run the frozen positive log-temperature golden search and acceptance gate."""
    if not rows:
        raise ValidationError("calibration requires development logits")
    if lower <= 0 or upper <= lower or iterations <= 0:
        raise ValidationError("invalid calibration search bounds")
    left, right = math.log(lower), math.log(upper)
    ratio = (math.sqrt(5.0) - 1.0) / 2.0
    first = right - ratio * (right - left)
    second = left + ratio * (right - left)
    first_loss = group_balanced_nll(rows, math.exp(first)).combined
    second_loss = group_balanced_nll(rows, math.exp(second)).combined
    for _ in range(iterations):
        if first_loss <= second_loss:
            right = second
            second = first
            second_loss = first_loss
            first = right - ratio * (right - left)
            first_loss = group_balanced_nll(rows, math.exp(first)).combined
        else:
            left = first
            first = second
            first_loss = second_loss
            second = left + ratio * (right - left)
            second_loss = group_balanced_nll(rows, math.exp(second)).combined
    candidate_temperature = math.exp((left + right) / 2.0)
    before = group_balanced_nll(rows, 1.0)
    candidate = group_balanced_nll(rows, candidate_temperature)
    reasons: list[str] = []
    if candidate.combined >= before.combined:
        reasons.append("combined_development_nll_did_not_decrease")
    if candidate.m3 > before.m3 + maximum_domain_increase:
        reasons.append("m3_development_nll_increased_over_0.01")
    if candidate.vitaminc > before.vitaminc + maximum_domain_increase:
        reasons.append("vitaminc_development_nll_increased_over_0.01")
    accepted = not reasons
    deployed = candidate_temperature if accepted else 1.0
    return CalibrationFit(
        candidate_temperature,
        deployed,
        accepted,
        tuple(reasons),
        before,
        candidate,
        group_balanced_nll(rows, deployed),
    )


def _selection_allows(
    selection: Mapping[str, object],
    *,
    variant: str,
    seed: int,
    checkpoint_tree_sha256: str,
    checkpoint_identity_sha256: str,
    selection_directory: Path,
    development_logits_path: Path,
    development_logits_sha256: str,
    source_alignment_sha256: str,
) -> None:
    if _string(selection, "schema_version") != SELECTION_SCHEMA:
        raise ValidationError("unsupported M4.13 selection schema")
    if selection.get("sealed") is not True:
        raise ValidationError("calibration requires a sealed selection")
    if _string(selection, "selected_variant") != variant:
        raise ValidationError("checkpoint variant was not selected on development")
    if _integer(selection, "primary_seed") != FROZEN_PRIMARY_SEED:
        raise ValidationError("selection changed the predesignated primary seed")
    candidates = selection.get("candidate_checkpoints")
    if not isinstance(candidates, list):
        raise ValidationError("selection checkpoint allow-list is malformed")
    matching = []
    for item in candidates:
        candidate = _mapping(item, "selection checkpoint")
        candidate_tree_sha256 = _sha256_field(candidate, "checkpoint_tree_sha256")
        candidate_identity_sha256 = _sha256_field(
            candidate, "checkpoint_identity_sha256"
        )
        candidate_logits_sha256 = _sha256_field(candidate, "development_logits_sha256")
        candidate_alignment_sha256 = _sha256_field(candidate, "source_alignment_sha256")
        relative_logits = Path(_string(candidate, "development_logits_path"))
        if relative_logits.is_absolute():
            raise ValidationError("selection development-logits path must be relative")
        selected_logits = (selection_directory / relative_logits).resolve()
        selection_root = selection_directory.resolve()
        if selection_root not in selected_logits.parents:
            raise ValidationError("selection development-logits path escapes its root")
        if (
            candidate.get("variant") == variant
            and candidate.get("seed") == seed
            and candidate_tree_sha256 == checkpoint_tree_sha256
            and candidate_identity_sha256 == checkpoint_identity_sha256
            and selected_logits == development_logits_path.resolve()
            and candidate_logits_sha256 == development_logits_sha256
            and candidate_alignment_sha256 == source_alignment_sha256
        ):
            matching.append(candidate)
    if len(matching) != 1:
        raise ValidationError("checkpoint is not uniquely allow-listed by selection")


def _development_source_alignment_sha256(
    *, prepared: Path, dataset_manifest_path: Path
) -> str:
    """Reconstruct Lane C's exact development-source alignment identity."""
    rows: list[dict[str, object]] = []
    for value in _load_jsonl(
        prepared / "development_vitaminc.jsonl", "VitaminC development JSONL"
    ):
        row_id = _string(value, "unique_id")
        case_id = _string(value, "case_id")
        try:
            suffix = int(row_id.rsplit("_", 1)[1])
        except (IndexError, ValueError) as error:
            raise ValidationError(
                "VitaminC development row has invalid suffix"
            ) from error
        rows.append(
            {
                "fixture": "vitaminc_development",
                "row_id": row_id,
                "page_id": _sha256_field(value, "normalized_page_sha256"),
                "case_id": case_id,
                "claim_group_id": None,
                "transition_id": (f"{case_id}:transition-{1 if suffix <= 2 else 2}"),
                "stratum": _string(value, "stratum"),
                "label": normalize_stored_label(_string(value, "label")),
                "claim_sha256": _sha256_field(value, "claim_sha256"),
                "evidence_sha256": _sha256_field(value, "evidence_sha256"),
            }
        )
    for value in _load_jsonl(prepared / "development_m3.jsonl", "M3 development JSONL"):
        claim = _string(value, "claim")
        evidence = _string(value, "evidence")
        rows.append(
            {
                "fixture": "m3_development",
                "row_id": _string(value, "example_id"),
                "page_id": None,
                "case_id": None,
                "claim_group_id": _string(value, "claim_group_id"),
                "transition_id": None,
                "stratum": None,
                "label": normalize_stored_label(_string(value, "label")),
                "claim_sha256": hashlib.sha256(claim.encode()).hexdigest(),
                "evidence_sha256": hashlib.sha256(evidence.encode()).hexdigest(),
            }
        )
    return _canonical_sha256(
        {
            "dataset_manifest_sha256": file_sha256(dataset_manifest_path),
            "vitaminc_manifest_sha256": file_sha256(
                prepared / "development_vitaminc_manifest.json"
            ),
            "m3_development_sha256": file_sha256(prepared / "development_m3.jsonl"),
            "rows": rows,
        }
    )


def _validate_development_dimensions(
    rows: Sequence[DevelopmentLogit], config: Mapping[str, object]
) -> None:
    dataset = _mapping(config.get("dataset"), "dataset configuration")
    samples = _mapping(dataset.get("samples"), "dataset samples")
    vitamin = _mapping(samples.get("development"), "VitaminC development sample")
    m3 = _mapping(config.get("m3"), "M3 configuration")
    m3_development = _mapping(m3.get("development"), "M3 development identity")
    vitamin_rows = [row for row in rows if row.domain == "vitaminc"]
    m3_rows = [row for row in rows if row.domain == "m3"]
    if (len(vitamin_rows), len({row.group_id for row in vitamin_rows})) != (
        _integer(vitamin, "rows"),
        _integer(vitamin, "cases"),
    ):
        raise ValidationError("VitaminC development-logit dimensions drifted")
    if (len(m3_rows), len({row.group_id for row in m3_rows})) != (
        _integer(m3_development, "rows"),
        _integer(m3_development, "claim_groups"),
    ):
        raise ValidationError("M3 development-logit dimensions drifted")


def _validate_development_identities(
    rows: Sequence[DevelopmentLogit], prepared: Path
) -> None:
    """Reject same-cardinality row, group, label, or content substitutions."""
    expected: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    for value in _load_jsonl(
        prepared / "development_vitaminc.jsonl", "VitaminC development JSONL"
    ):
        if _string(value, "split") != "development":
            raise ValidationError("VitaminC calibration source is not development")
        key = ("vitaminc", _string(value, "unique_id"))
        expected[key] = (
            _string(value, "case_id"),
            normalize_stored_label(_string(value, "label")),
            _sha256_field(value, "claim_sha256"),
            _sha256_field(value, "evidence_sha256"),
        )
    for value in _load_jsonl(prepared / "development_m3.jsonl", "M3 development JSONL"):
        if _string(value, "split") != "development":
            raise ValidationError("M3 calibration source is not development")
        claim = _string(value, "claim")
        evidence = _string(value, "evidence")
        key = ("m3", _string(value, "example_id"))
        expected[key] = (
            _string(value, "claim_group_id"),
            normalize_stored_label(_string(value, "label")),
            hashlib.sha256(claim.encode()).hexdigest(),
            hashlib.sha256(evidence.encode()).hexdigest(),
        )
    actual = {
        (row.domain, row.row_id): (
            row.group_id,
            row.stored_label,
            row.claim_sha256,
            row.evidence_sha256,
        )
        for row in rows
    }
    if actual != expected:
        raise ValidationError(
            "development logits do not exactly match the prepared row identities"
        )


def _write_calibration_atomically(
    path: Path,
    payload: Mapping[str, object],
    *,
    expected_invocation_sha256: str,
    failure_stage: CalibrationFailureStage | None,
) -> Mapping[str, object]:
    if (
        _string(payload, "schema_version") != CALIBRATION_SCHEMA
        or _string(payload, "status") != "complete"
        or _string(payload, "invocation_sha256") != expected_invocation_sha256
    ):
        raise ValidationError("requested calibration payload is not self-consistent")
    if path.exists():
        try:
            existing = _load_json(path, "calibration artifact")
            exact = (
                _string(existing, "schema_version") == CALIBRATION_SCHEMA
                and _string(existing, "status") == "complete"
                and _string(existing, "invocation_sha256") == expected_invocation_sha256
                and _canonical_bytes(existing) == _canonical_bytes(payload)
            )
        except (FileNotFoundError, ValidationError):
            exact = False
            existing = {}
        if not exact:
            raise ValidationError(
                "existing calibration is partial or belongs to another run"
            )
        return existing
    path.parent.mkdir(parents=True, exist_ok=True)
    if failure_stage is CalibrationFailureStage.BEFORE_RESULT_WRITE:
        raise RuntimeError("injected failure before calibration result write")
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{path.name}.partial-", dir=path.parent
    )
    temporary = Path(raw_path)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(_canonical_bytes(payload) + b"\n")
            output.flush()
            os.fsync(output.fileno())
        if failure_stage is CalibrationFailureStage.BEFORE_RESULT_SEALING:
            raise RuntimeError("injected failure before calibration result sealing")
        os.replace(temporary, path)
        result = _load_json(path, "calibration artifact")
        if (
            _string(result, "schema_version") != CALIBRATION_SCHEMA
            or _string(result, "status") != "complete"
            or _string(result, "invocation_sha256") != expected_invocation_sha256
            or _canonical_bytes(result) != _canonical_bytes(payload)
        ):
            raise AssertionError("newly written calibration failed its own seal")
        return result
    finally:
        temporary.unlink(missing_ok=True)


def calibrate_selected_checkpoint(
    *,
    artifact_root: Path,
    config_path: Path,
    run_directory: Path,
    selection_path: Path,
    development_logits_path: Path,
    repository_root: Path,
    failure_stage: CalibrationFailureStage | None = None,
) -> Mapping[str, object]:
    """Calibrate one development-selected checkpoint without any test input."""
    repository_provenance = validate_calibration_repository_provenance(
        repository_root
    )
    completed = load_completed_training_run(run_directory)
    manifest = completed.training_manifest
    variant = _string(manifest, "variant")
    seed = _integer(manifest, "seed")
    checkpoint_tree = _string(completed.checkpoint_identity, "checkpoint_tree_sha256")
    checkpoint_identity_path = run_directory / "checkpoint_identity.json"
    checkpoint_identity_sha256 = file_sha256(checkpoint_identity_path)
    selection = _load_json(selection_path, "sealed selection")
    config = _load_json(config_path, "M4.13 configuration")
    if _string(config, "schema_version") != CONFIG_SCHEMA:
        raise ValidationError("unsupported M4.13 configuration schema")
    prepared = artifact_root / "prepared"
    dataset_manifest_path = prepared / "dataset_manifest.json"
    dataset_manifest = _load_json(dataset_manifest_path, "prepared dataset manifest")
    if _string(dataset_manifest, "schema_version") != DATASET_MANIFEST_SCHEMA:
        raise ValidationError("unsupported prepared dataset manifest schema")
    _validate_dataset_manifest(
        dataset_manifest, config_path=config_path, prepared=prepared
    )
    rows = _load_development_logits(development_logits_path)
    if any(
        row.checkpoint_tree_sha256 != checkpoint_tree
        or row.variant != variant
        or row.seed != seed
        for row in rows
    ):
        raise ValidationError("development logits belong to another checkpoint")
    _validate_development_dimensions(rows, config)
    _validate_development_identities(rows, prepared)
    logits_sha256 = file_sha256(development_logits_path)
    source_alignment_sha256 = _development_source_alignment_sha256(
        prepared=prepared, dataset_manifest_path=dataset_manifest_path
    )
    _selection_allows(
        selection,
        variant=variant,
        seed=seed,
        checkpoint_tree_sha256=checkpoint_tree,
        checkpoint_identity_sha256=checkpoint_identity_sha256,
        selection_directory=selection_path.parent,
        development_logits_path=development_logits_path,
        development_logits_sha256=logits_sha256,
        source_alignment_sha256=source_alignment_sha256,
    )
    fit = fit_group_balanced_temperature(rows)
    m3 = _mapping(config.get("m3"), "M3 configuration")
    m3_development = _mapping(m3.get("development"), "M3 development identity")
    dataset = _mapping(config.get("dataset"), "dataset configuration")
    samples = _mapping(dataset.get("samples"), "dataset samples")
    vitamin = _mapping(samples.get("development"), "VitaminC development sample")
    selection_sha256 = file_sha256(selection_path)
    invocation_payload = {
        "schema_version": "groundloop-m4-13-calibration-invocation-v1",
        "selection_sha256": selection_sha256,
        "selected": True,
        "variant": variant,
        "seed": seed,
        "checkpoint_tree_sha256": checkpoint_tree,
        "checkpoint_identity_sha256": checkpoint_identity_sha256,
        "dataset_manifest_sha256": file_sha256(dataset_manifest_path),
        "m3_development_jsonl_sha256": _string(m3_development, "sha256"),
        "vitaminc_development_manifest_sha256": _string(vitamin, "manifest_sha256"),
        "development_logits_sha256": logits_sha256,
        "source_alignment_sha256": source_alignment_sha256,
        "method": CALIBRATION_METHOD,
        "group_weighting": {
            "domain_weights": {"m3": 0.5, "vitaminc": 0.5},
            "m3": "mean row NLL within claim_group_id, then mean groups",
            "vitaminc": "mean row NLL within case_id, then mean cases",
        },
        "search": {
            "domain": "positive log-temperature",
            "lower": 0.05,
            "upper": 20.0,
            "iterations": 96,
            "maximum_domain_nll_increase": 0.01,
        },
        **repository_provenance.to_manifest(),
    }
    invocation_sha256 = _canonical_sha256(invocation_payload)
    semantic = {
        **invocation_payload,
        "candidate_temperature": fit.candidate_temperature,
        "deployed_temperature": fit.deployed_temperature,
        "accepted": fit.accepted,
        "rejection_reasons": list(fit.rejection_reasons),
        "counts": {
            "examples": len(rows),
            "m3_examples": sum(row.domain == "m3" for row in rows),
            "m3_claim_groups": len(
                {row.group_id for row in rows if row.domain == "m3"}
            ),
            "vitaminc_examples": sum(row.domain == "vitaminc" for row in rows),
            "vitaminc_cases": len(
                {row.group_id for row in rows if row.domain == "vitaminc"}
            ),
        },
        "nll": {
            "uncalibrated": asdict(fit.nll_before),
            "old_m3_temperature": {
                "temperature": OLD_M3_TEMPERATURE,
                **asdict(group_balanced_nll(rows, OLD_M3_TEMPERATURE)),
            },
            "candidate": asdict(fit.nll_candidate),
            "deployed": asdict(fit.nll_deployed),
        },
        "base_logit_order": list(BASE_LOGIT_ORDER),
        "stored_probability_order": list(STORED_LABEL_ORDER),
    }
    calibration_version = f"temperature-m4-13-v1:{_canonical_sha256(semantic)}"
    payload = {
        **semantic,
        # Seal fields follow the nested invocation payload so its own
        # schema_version cannot overwrite the artifact schema.
        "schema_version": CALIBRATION_SCHEMA,
        "status": "complete",
        "invocation_sha256": invocation_sha256,
        "calibration_version": calibration_version,
    }
    if (
        validate_calibration_repository_provenance(repository_root)
        != repository_provenance
    ):
        raise ValidationError("calibration repository changed before result write")
    return _write_calibration_atomically(
        run_directory / "calibration.json",
        payload,
        expected_invocation_sha256=invocation_sha256,
        failure_stage=failure_stage,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fit M4.13 calibration on both development domains only."
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--development-logits", type=Path, required=True)
    parser.add_argument(
        "--repository-root",
        type=Path,
        required=True,
        help="exact clean Git worktree root whose calibrator bytes are executed",
    )
    parser.add_argument(
        "--failure-stage", choices=[stage.value for stage in CalibrationFailureStage]
    )
    return parser.parse_args()


def main() -> None:
    arguments = _parse_args()
    result = calibrate_selected_checkpoint(
        artifact_root=arguments.artifact_root,
        config_path=arguments.config,
        run_directory=arguments.run_directory,
        selection_path=arguments.selection,
        development_logits_path=arguments.development_logits,
        repository_root=arguments.repository_root,
        failure_stage=(
            None
            if arguments.failure_stage is None
            else CalibrationFailureStage(arguments.failure_stage)
        ),
    )
    print(json.dumps(dict(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
