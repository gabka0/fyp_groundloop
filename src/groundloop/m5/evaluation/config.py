"""Strict loader for the frozen M5.5 controlled-evaluation configuration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError
from groundloop.m5.evaluation.manifest import (
    WICE_ADAPTER_VERSION,
    load_source_manifest,
)
from groundloop.m5.evaluation.metrics import (
    BOOTSTRAP_CLUSTER_UNIT,
    BOOTSTRAP_CONFIDENCE_LEVEL,
    BOOTSTRAP_METHOD,
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_RNG,
    BOOTSTRAP_SEED,
)
from groundloop.m5.evaluation.records import WiceSourceManifest
from groundloop.m5.evaluation.wice import (
    CONTROLLED_POLICY_VERSION,
    CONTROLLED_REFUTE_THRESHOLD,
    CONTROLLED_SUPPORT_THRESHOLD,
    CONTROLLED_TIE_RULE_VERSION,
    MAX_RENDERED_CHARACTERS,
    RENDER_LIMIT_KIND,
)

CONTROLLED_EVALUATION_CONFIG_SCHEMA = "groundloop-m5-controlled-evaluation-config-v1"
CONFIG_HASH_ALGORITHM = "sha256-canonical-json-v1"
CHECKED_IN_PRIMARY_CONFIG_SHA256 = (
    "4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f"
)
CHECKED_IN_PRIMARY_SOURCE_MANIFEST_SHA256 = (
    "c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2"
)
SOURCE_VERIFICATION_STATEMENT = (
    "Frozen revision metadata enforced; exact consumed bytes verified against "
    "manifest SHA-256 before parsing."
)

FROZEN_SPLIT_ROLES = (
    (
        "train",
        "source-defined training split; not used by this adapter for selection",
    ),
    (
        "dev",
        "source-defined development split; no M5 policy selection",
    ),
    (
        "test",
        "source-defined test split; no model, policy, threshold, prompt, or "
        "implementation selection",
    ),
)
FROZEN_MODEL_CALLS = (
    ("controlled_projection", 0),
    ("source_invalidation", 0),
    ("frozen_citation", 0),
)


class _DuplicateKey(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ProjectionPolicyConfig:
    policy_version: str
    support_threshold: float
    refute_threshold: float
    tie_rule_version: str


@dataclass(frozen=True, slots=True)
class EvidenceUnitConfig:
    adapter_version: str
    render_limit_kind: str
    render_limit_characters: int


@dataclass(frozen=True, slots=True)
class BootstrapConfig:
    cluster_unit: str
    method: str
    rng: str
    seed: int
    resamples: int
    confidence_level: float


@dataclass(frozen=True, slots=True)
class SplitRolesConfig:
    train: str
    dev: str
    test: str


@dataclass(frozen=True, slots=True)
class ModelCallsConfig:
    controlled_projection: int
    source_invalidation: int
    frozen_citation: int


@dataclass(frozen=True, slots=True)
class ControlledEvaluationConfig:
    schema: str
    consumed_path: Path
    canonical_sha256: str
    source_manifest_reference: str
    source_manifest_path: Path
    source_manifest: WiceSourceManifest
    projection_policy: ProjectionPolicyConfig
    evidence_unit: EvidenceUnitConfig
    bootstrap: BootstrapConfig
    split_roles: SplitRolesConfig
    model_calls: ModelCallsConfig


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _invalid_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value!r}")


def _mapping(
    value: object,
    context: str,
    expected_keys: tuple[str, ...],
) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValidationError(f"{context} must be a JSON object")
    result = cast(dict[str, object], value)
    expected = set(expected_keys)
    actual = set(result)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        detail: list[str] = []
        if missing:
            detail.append(f"missing={missing!r}")
        if unknown:
            detail.append(f"unknown={unknown!r}")
        raise ValidationError(f"{context} has invalid fields: {', '.join(detail)}")
    return result


def _string(mapping: dict[str, object], name: str, context: str) -> str:
    value = mapping[name]
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{context}.{name} must be a nonempty string")
    return value


def _integer(mapping: dict[str, object], name: str, context: str) -> int:
    value = mapping[name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{context}.{name} must be an integer")
    return value


def _number(mapping: dict[str, object], name: str, context: str) -> float:
    value = mapping[name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{context}.{name} must be a JSON number")
    return float(value)


def _enforce_frozen(context: str, value: object, expected: object) -> None:
    if value != expected:
        raise ValidationError(
            f"{context} drifted from frozen value {expected!r}: got {value!r}"
        )


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _resolve_manifest_path(config_path: Path, reference: str) -> Path:
    relative = Path(reference)
    if (
        relative.is_absolute()
        or reference != relative.as_posix()
        or reference in ("", ".")
        or ".." in relative.parts
    ):
        raise ValidationError(
            "config.source_manifest must be a normalized relative path"
        )
    config_directory = config_path.parent
    try:
        candidate = (config_directory / relative).resolve(strict=True)
    except OSError as error:
        raise ValidationError(
            f"cannot resolve config.source_manifest {reference!r}: {error}"
        ) from error
    if not candidate.is_relative_to(config_directory) or not candidate.is_file():
        raise ValidationError(
            "config.source_manifest must resolve to a file inside the config directory"
        )
    return candidate


def load_controlled_evaluation_config(path: Path) -> ControlledEvaluationConfig:
    """Load the exact frozen config and its config-local source manifest.

    The loader validates duplicate, missing, unknown, and drifted values before
    returning a typed object. The source-manifest reference cannot be absolute,
    traverse upward, or escape through a symlink.
    """

    try:
        config_path = path.expanduser().resolve(strict=True)
        raw = config_path.read_bytes()
    except OSError as error:
        raise ValidationError(
            f"cannot read controlled evaluation config: {error}"
        ) from error
    if not config_path.is_file():
        raise ValidationError("controlled evaluation config path must be a file")
    try:
        decoded = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_invalid_constant,
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        _DuplicateKey,
        ValueError,
    ) as error:
        raise ValidationError(
            f"invalid controlled evaluation config: {error}"
        ) from error
    root = _mapping(
        decoded,
        "config",
        (
            "schema",
            "source_manifest",
            "projection_policy",
            "evidence_unit",
            "bootstrap",
            "split_roles",
            "model_calls",
        ),
    )
    schema = _string(root, "schema", "config")
    _enforce_frozen("config.schema", schema, CONTROLLED_EVALUATION_CONFIG_SCHEMA)

    policy_json = _mapping(
        root["projection_policy"],
        "config.projection_policy",
        (
            "policy_version",
            "support_threshold",
            "refute_threshold",
            "tie_rule_version",
        ),
    )
    policy = ProjectionPolicyConfig(
        policy_version=_string(
            policy_json, "policy_version", "config.projection_policy"
        ),
        support_threshold=_number(
            policy_json, "support_threshold", "config.projection_policy"
        ),
        refute_threshold=_number(
            policy_json, "refute_threshold", "config.projection_policy"
        ),
        tie_rule_version=_string(
            policy_json, "tie_rule_version", "config.projection_policy"
        ),
    )
    for context, value, expected in (
        (
            "config.projection_policy.policy_version",
            policy.policy_version,
            CONTROLLED_POLICY_VERSION,
        ),
        (
            "config.projection_policy.support_threshold",
            policy.support_threshold,
            CONTROLLED_SUPPORT_THRESHOLD,
        ),
        (
            "config.projection_policy.refute_threshold",
            policy.refute_threshold,
            CONTROLLED_REFUTE_THRESHOLD,
        ),
        (
            "config.projection_policy.tie_rule_version",
            policy.tie_rule_version,
            CONTROLLED_TIE_RULE_VERSION,
        ),
    ):
        _enforce_frozen(context, value, expected)

    evidence_json = _mapping(
        root["evidence_unit"],
        "config.evidence_unit",
        ("adapter_version", "render_limit_kind", "render_limit_characters"),
    )
    evidence = EvidenceUnitConfig(
        adapter_version=_string(
            evidence_json, "adapter_version", "config.evidence_unit"
        ),
        render_limit_kind=_string(
            evidence_json, "render_limit_kind", "config.evidence_unit"
        ),
        render_limit_characters=_integer(
            evidence_json, "render_limit_characters", "config.evidence_unit"
        ),
    )
    for context, value, expected in (
        (
            "config.evidence_unit.adapter_version",
            evidence.adapter_version,
            WICE_ADAPTER_VERSION,
        ),
        (
            "config.evidence_unit.render_limit_kind",
            evidence.render_limit_kind,
            RENDER_LIMIT_KIND,
        ),
        (
            "config.evidence_unit.render_limit_characters",
            evidence.render_limit_characters,
            MAX_RENDERED_CHARACTERS,
        ),
    ):
        _enforce_frozen(context, value, expected)

    bootstrap_json = _mapping(
        root["bootstrap"],
        "config.bootstrap",
        ("cluster_unit", "method", "rng", "seed", "resamples", "confidence_level"),
    )
    bootstrap = BootstrapConfig(
        cluster_unit=_string(bootstrap_json, "cluster_unit", "config.bootstrap"),
        method=_string(bootstrap_json, "method", "config.bootstrap"),
        rng=_string(bootstrap_json, "rng", "config.bootstrap"),
        seed=_integer(bootstrap_json, "seed", "config.bootstrap"),
        resamples=_integer(bootstrap_json, "resamples", "config.bootstrap"),
        confidence_level=_number(
            bootstrap_json, "confidence_level", "config.bootstrap"
        ),
    )
    for context, value, expected in (
        (
            "config.bootstrap.cluster_unit",
            bootstrap.cluster_unit,
            BOOTSTRAP_CLUSTER_UNIT,
        ),
        ("config.bootstrap.method", bootstrap.method, BOOTSTRAP_METHOD),
        ("config.bootstrap.rng", bootstrap.rng, BOOTSTRAP_RNG),
        ("config.bootstrap.seed", bootstrap.seed, BOOTSTRAP_SEED),
        ("config.bootstrap.resamples", bootstrap.resamples, BOOTSTRAP_RESAMPLES),
        (
            "config.bootstrap.confidence_level",
            bootstrap.confidence_level,
            BOOTSTRAP_CONFIDENCE_LEVEL,
        ),
    ):
        _enforce_frozen(context, value, expected)

    split_json = _mapping(
        root["split_roles"],
        "config.split_roles",
        tuple(name for name, _ in FROZEN_SPLIT_ROLES),
    )
    split_values = {
        name: _string(split_json, name, "config.split_roles")
        for name, _ in FROZEN_SPLIT_ROLES
    }
    for name, expected in FROZEN_SPLIT_ROLES:
        _enforce_frozen(f"config.split_roles.{name}", split_values[name], expected)
    split_roles = SplitRolesConfig(
        train=split_values["train"],
        dev=split_values["dev"],
        test=split_values["test"],
    )

    model_json = _mapping(
        root["model_calls"],
        "config.model_calls",
        tuple(name for name, _ in FROZEN_MODEL_CALLS),
    )
    model_values = {
        name: _integer(model_json, name, "config.model_calls")
        for name, _ in FROZEN_MODEL_CALLS
    }
    for name, expected in FROZEN_MODEL_CALLS:
        _enforce_frozen(f"config.model_calls.{name}", model_values[name], expected)
    model_calls = ModelCallsConfig(
        controlled_projection=model_values["controlled_projection"],
        source_invalidation=model_values["source_invalidation"],
        frozen_citation=model_values["frozen_citation"],
    )

    manifest_reference = _string(root, "source_manifest", "config")
    manifest_path = _resolve_manifest_path(config_path, manifest_reference)
    manifest = load_source_manifest(manifest_path)
    if manifest.adapter_version != evidence.adapter_version:
        raise ValidationError(
            "source manifest adapter version disagrees with controlled config"
        )

    config_hash = hashlib.sha256(_canonical_json_bytes(root)).hexdigest()
    return ControlledEvaluationConfig(
        schema=schema,
        consumed_path=config_path,
        canonical_sha256=config_hash,
        source_manifest_reference=manifest_reference,
        source_manifest_path=manifest_path,
        source_manifest=manifest,
        projection_policy=policy,
        evidence_unit=evidence,
        bootstrap=bootstrap,
        split_roles=split_roles,
        model_calls=model_calls,
    )


def evaluation_config_identity_dict(
    config: ControlledEvaluationConfig,
) -> dict[str, object]:
    """Return the path and hash identity of the exact consumed configuration."""

    config_matches = config.canonical_sha256 == CHECKED_IN_PRIMARY_CONFIG_SHA256
    manifest_matches = (
        config.source_manifest.manifest_hash
        == CHECKED_IN_PRIMARY_SOURCE_MANIFEST_SHA256
    )
    primary_eligible = config_matches and manifest_matches
    return {
        "schema": config.schema,
        "consumed_path": str(config.consumed_path),
        "canonical_sha256": config.canonical_sha256,
        "hash_algorithm": CONFIG_HASH_ALGORITHM,
        "source_manifest_reference": config.source_manifest_reference,
        "source_manifest_consumed_path": str(config.source_manifest_path),
        "source_manifest_schema": config.source_manifest.schema,
        "source_manifest_canonical_sha256": config.source_manifest.manifest_hash,
        "source_revision": config.source_manifest.official_commit,
        "primary_gate_identity": {
            "classification": (
                "checked_in_frozen_primary"
                if primary_eligible
                else "non_primary_config_bundle"
            ),
            "eligible": primary_eligible,
            "config_hash_matches": config_matches,
            "source_manifest_hash_matches": manifest_matches,
            "required_config_canonical_sha256": CHECKED_IN_PRIMARY_CONFIG_SHA256,
            "required_source_manifest_canonical_sha256": (
                CHECKED_IN_PRIMARY_SOURCE_MANIFEST_SHA256
            ),
        },
    }


__all__ = [
    "CHECKED_IN_PRIMARY_CONFIG_SHA256",
    "CHECKED_IN_PRIMARY_SOURCE_MANIFEST_SHA256",
    "CONFIG_HASH_ALGORITHM",
    "CONTROLLED_EVALUATION_CONFIG_SCHEMA",
    "ControlledEvaluationConfig",
    "EvidenceUnitConfig",
    "FROZEN_MODEL_CALLS",
    "FROZEN_SPLIT_ROLES",
    "ModelCallsConfig",
    "ProjectionPolicyConfig",
    "SOURCE_VERIFICATION_STATEMENT",
    "SplitRolesConfig",
    "BootstrapConfig",
    "evaluation_config_identity_dict",
    "load_controlled_evaluation_config",
]
