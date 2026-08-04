from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.evaluation.config import (
    CONFIG_HASH_ALGORITHM,
    SOURCE_VERIFICATION_STATEMENT,
    evaluation_config_identity_dict,
    load_controlled_evaluation_config,
)


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _frozen_payload() -> dict[str, object]:
    config_path = _repository_root() / "configs/m5/controlled_evaluation_v1.json"
    return cast(
        dict[str, object],
        json.loads(config_path.read_text(encoding="utf-8")),
    )


def _write_bundle(
    root: Path,
    payload: dict[str, object] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    manifest = _repository_root() / "configs/m5/wice_source_manifest_v1.json"
    (root / "wice_source_manifest_v1.json").write_bytes(manifest.read_bytes())
    config_path = root / "controlled_evaluation_v1.json"
    config_path.write_text(
        json.dumps(payload if payload is not None else _frozen_payload(), indent=2)
        + "\n",
        encoding="utf-8",
    )
    return config_path


def _replace_nested(
    payload: dict[str, object],
    field_path: tuple[str, ...],
    value: object,
) -> None:
    target = payload
    for name in field_path[:-1]:
        target = cast(dict[str, object], target[name])
    target[field_path[-1]] = value


def test_frozen_config_golden_hash_and_consumed_identity() -> None:
    config_path = _repository_root() / "configs/m5/controlled_evaluation_v1.json"
    config = load_controlled_evaluation_config(config_path)
    identity = evaluation_config_identity_dict(config)

    assert config.canonical_sha256 == (
        "4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f"
    )
    assert config.bootstrap.seed == 20260802
    assert config.bootstrap.resamples == 10_000
    assert config.bootstrap.confidence_level == 0.95
    assert identity["hash_algorithm"] == CONFIG_HASH_ALGORITHM
    assert identity["consumed_path"] == str(config_path.resolve())
    assert identity["primary_gate_identity"] == {
        "classification": "checked_in_frozen_primary",
        "eligible": True,
        "config_hash_matches": True,
        "source_manifest_hash_matches": True,
        "required_config_canonical_sha256": (
            "4ae7e27b9ada58965cbb72037739006d4caa728f955c27caf0bcc93bbee7532f"
        ),
        "required_source_manifest_canonical_sha256": (
            "c4a186f42252c280b5cafdc26b182e88c5aec14072866af15dd86d4b4c4722f2"
        ),
    }
    assert SOURCE_VERIFICATION_STATEMENT == (
        "Frozen revision metadata enforced; exact consumed bytes verified against "
        "manifest SHA-256 before parsing."
    )


def test_canonical_hash_ignores_json_whitespace(tmp_path: Path) -> None:
    first = load_controlled_evaluation_config(_write_bundle(tmp_path / "first"))
    second_path = _write_bundle(tmp_path / "second")
    second_path.write_text(
        json.dumps(_frozen_payload(), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    second = load_controlled_evaluation_config(second_path)

    assert first.canonical_sha256 == second.canonical_sha256


@pytest.mark.parametrize(
    ("field_path", "drifted_value"),
    (
        (("schema",), "groundloop-m5-controlled-evaluation-config-v2"),
        (("projection_policy", "support_threshold"), 0.6),
        (("evidence_unit", "render_limit_characters"), 1199),
        (("bootstrap", "seed"), 1),
        (("split_roles", "test"), "used for selection"),
        (("model_calls", "controlled_projection"), 1),
    ),
)
def test_every_frozen_config_section_rejects_drift(
    tmp_path: Path,
    field_path: tuple[str, ...],
    drifted_value: object,
) -> None:
    payload = _frozen_payload()
    _replace_nested(payload, field_path, drifted_value)

    with pytest.raises(ValidationError, match="drifted from frozen value"):
        load_controlled_evaluation_config(_write_bundle(tmp_path, payload))


def test_duplicate_missing_unknown_and_nonfinite_values_fail_closed(
    tmp_path: Path,
) -> None:
    unknown = _frozen_payload()
    unknown["unexpected"] = True
    with pytest.raises(ValidationError, match="unknown"):
        load_controlled_evaluation_config(_write_bundle(tmp_path / "unknown", unknown))

    missing = _frozen_payload()
    del cast(dict[str, object], missing["bootstrap"])["seed"]
    with pytest.raises(ValidationError, match="missing"):
        load_controlled_evaluation_config(_write_bundle(tmp_path / "missing", missing))

    duplicate_path = _write_bundle(tmp_path / "duplicate")
    duplicate_raw = duplicate_path.read_text(encoding="utf-8").replace(
        "{",
        '{"schema":"duplicate",',
        1,
    )
    duplicate_path.write_text(duplicate_raw, encoding="utf-8")
    with pytest.raises(ValidationError, match="invalid controlled evaluation config"):
        load_controlled_evaluation_config(duplicate_path)

    nonfinite_path = _write_bundle(tmp_path / "nonfinite")
    nonfinite_raw = nonfinite_path.read_text(encoding="utf-8").replace("0.95", "NaN", 1)
    nonfinite_path.write_text(nonfinite_raw, encoding="utf-8")
    with pytest.raises(ValidationError, match="non-finite JSON number"):
        load_controlled_evaluation_config(nonfinite_path)


@pytest.mark.parametrize(
    "reference",
    ("../wice_source_manifest_v1.json", "/tmp/wice_source_manifest_v1.json"),
)
def test_manifest_absolute_and_parent_traversal_are_rejected(
    tmp_path: Path,
    reference: str,
) -> None:
    payload = _frozen_payload()
    payload["source_manifest"] = reference

    with pytest.raises(ValidationError, match="normalized relative path"):
        load_controlled_evaluation_config(_write_bundle(tmp_path, payload))


def test_manifest_symlink_escape_is_rejected(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    config_path = _write_bundle(bundle)
    outside = tmp_path / "outside.json"
    source = _repository_root() / "configs/m5/wice_source_manifest_v1.json"
    outside.write_bytes(source.read_bytes())
    link = bundle / "escape.json"
    link.symlink_to(outside)
    payload = _frozen_payload()
    payload["source_manifest"] = link.name
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValidationError, match="inside the config directory"):
        load_controlled_evaluation_config(config_path)


def test_manifest_adapter_drift_is_rejected(tmp_path: Path) -> None:
    config_path = _write_bundle(tmp_path)
    manifest_path = tmp_path / "wice_source_manifest_v1.json"
    manifest = cast(
        dict[str, object],
        json.loads(manifest_path.read_text(encoding="utf-8")),
    )
    manifest["adapter_version"] = "wice-evidence-unit-v2"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValidationError, match="unsupported adapter version"):
        load_controlled_evaluation_config(config_path)
