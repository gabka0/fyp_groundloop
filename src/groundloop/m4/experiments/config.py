"""Strict loader for the frozen controlled M4.6 evaluation configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.experiments.contracts import EvaluationPolicy, PolicyKind
from groundloop.m4.oracles import (
    FROZEN_HISTORY_BOOTSTRAP_V1,
    BootstrapConfig,
    WorkloadSplit,
)

_HEX = frozenset("0123456789abcdef")


def _require_hash(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class ControlledEvaluationConfig:
    schema_version: str
    config_id: str
    workload_builder_id: str
    split: WorkloadSplit
    verifier_model_artifact_id: str
    verifier_execution_spec_hash: str
    decision_policy_id: str
    oracle_policy_id: str
    oracle_policy_hash: str
    policies: tuple[EvaluationPolicy, ...]
    bootstrap: BootstrapConfig

    def __post_init__(self) -> None:
        if self.schema_version != "m4-controlled-evaluation-config-v1":
            raise ValidationError("unsupported controlled evaluation config schema")
        if self.workload_builder_id != "build_controlled_dynamic_workload_v1":
            raise ValidationError("unsupported controlled workload builder")
        if self.split is not WorkloadSplit.TEST:
            raise ValidationError("frozen one-command fixture evaluates the test split")
        if not self.config_id.strip():
            raise ValidationError("config_id must be non-empty")
        for name, value in (
            ("verifier_model_artifact_id", self.verifier_model_artifact_id),
            ("decision_policy_id", self.decision_policy_id),
            ("oracle_policy_id", self.oracle_policy_id),
        ):
            if not value.strip():
                raise ValidationError(f"{name} must be non-empty")
        _require_hash(
            "verifier_execution_spec_hash", self.verifier_execution_spec_hash
        )
        _require_hash("oracle_policy_hash", self.oracle_policy_hash)
        expected_verifier_hash = stable_m4_digest(
            "m4-controlled-table-verifier-v1", "no-model-call"
        )
        expected_oracle_hash = stable_m4_digest(
            "m4-controlled-full-pair-policy-v1", "exact-cartesian"
        )
        if (
            self.verifier_model_artifact_id != "controlled-table-verifier-v1"
            or self.verifier_execution_spec_hash != expected_verifier_hash
        ):
            raise ValidationError("controlled verifier provenance is inconsistent")
        if self.oracle_policy_hash != expected_oracle_hash:
            raise ValidationError("controlled exhaustive oracle hash is inconsistent")
        if len(self.policies) != len(PolicyKind):
            raise ValidationError("config requires every frozen M4.6 policy ablation")
        if self.policies != tuple(
            sorted(self.policies, key=lambda item: item.policy_id)
        ):
            raise ValidationError("policies must use canonical policy-ID order")
        ids = tuple(item.policy_id for item in self.policies)
        kinds = tuple(item.kind for item in self.policies)
        if len(ids) != len(set(ids)) or set(kinds) != set(PolicyKind):
            raise ValidationError("policy IDs and kinds must be unique and complete")
        if self.bootstrap != FROZEN_HISTORY_BOOTSTRAP_V1:
            raise ValidationError("controlled run requires the frozen bootstrap")

    @property
    def manifest_hash(self) -> str:
        return stable_m4_digest(
            "m4-controlled-evaluation-config-v1",
            self.schema_version,
            self.config_id,
            self.workload_builder_id,
            self.split.value,
            self.verifier_model_artifact_id,
            self.verifier_execution_spec_hash,
            self.decision_policy_id,
            self.oracle_policy_id,
            self.oracle_policy_hash,
            *(policy.manifest_hash for policy in self.policies),
            self.bootstrap.manifest_hash,
        )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(dict[str, object], value)


def _text(mapping: dict[str, object], name: str) -> str:
    value = mapping.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")
    return value


def _integer(mapping: dict[str, object], name: str) -> int:
    value = mapping.get(name)
    if type(value) is not int:
        raise ValidationError(f"{name} must be an integer")
    return value


def _floating(mapping: dict[str, object], name: str) -> float:
    value = mapping.get(name)
    if type(value) not in {int, float}:
        raise ValidationError(f"{name} must be numeric")
    return float(cast(int | float, value))


def load_controlled_evaluation_config(
    path: str | Path,
) -> ControlledEvaluationConfig:
    """Load a checked-in config and reject ignored or malformed fields."""
    source = Path(path)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError(
            f"cannot load evaluation config {source}: {error}"
        ) from error
    root = _object(raw, "evaluation config")
    expected_root = {
        "schema_version",
        "config_id",
        "workload_builder_id",
        "split",
        "verifier_model_artifact_id",
        "verifier_execution_spec_hash",
        "decision_policy_id",
        "oracle_policy_id",
        "oracle_policy_hash",
        "policies",
        "bootstrap",
    }
    if set(root) != expected_root:
        raise ValidationError("evaluation config has missing or unknown fields")

    raw_policies = root["policies"]
    if not isinstance(raw_policies, list):
        raise ValidationError("policies must be a JSON array")
    policies: list[EvaluationPolicy] = []
    for raw_policy in raw_policies:
        policy = _object(raw_policy, "policy")
        if set(policy) != {
            "policy_id",
            "kind",
            "approximate_budget_per_inserted_chunk",
            "frontier_budget_per_inserted_chunk",
        }:
            raise ValidationError("policy has missing or unknown fields")
        try:
            kind = PolicyKind(_text(policy, "kind"))
        except ValueError as error:
            raise ValidationError("unknown evaluation policy kind") from error
        policies.append(
            EvaluationPolicy(
                policy_id=_text(policy, "policy_id"),
                kind=kind,
                approximate_budget_per_inserted_chunk=_integer(
                    policy, "approximate_budget_per_inserted_chunk"
                ),
                frontier_budget_per_inserted_chunk=_integer(
                    policy, "frontier_budget_per_inserted_chunk"
                ),
            )
        )

    bootstrap_raw = _object(root["bootstrap"], "bootstrap")
    if set(bootstrap_raw) != {
        "config_id",
        "seed",
        "replicate_count",
        "confidence_level",
        "minimum_cluster_count",
        "interval_method",
        "estimand",
    }:
        raise ValidationError("bootstrap has missing or unknown fields")
    try:
        split = WorkloadSplit(_text(root, "split"))
    except ValueError as error:
        raise ValidationError("unknown workload split") from error
    return ControlledEvaluationConfig(
        schema_version=_text(root, "schema_version"),
        config_id=_text(root, "config_id"),
        workload_builder_id=_text(root, "workload_builder_id"),
        split=split,
        verifier_model_artifact_id=_text(root, "verifier_model_artifact_id"),
        verifier_execution_spec_hash=_text(
            root, "verifier_execution_spec_hash"
        ),
        decision_policy_id=_text(root, "decision_policy_id"),
        oracle_policy_id=_text(root, "oracle_policy_id"),
        oracle_policy_hash=_text(root, "oracle_policy_hash"),
        policies=tuple(policies),
        bootstrap=BootstrapConfig(
            config_id=_text(bootstrap_raw, "config_id"),
            seed=_integer(bootstrap_raw, "seed"),
            replicate_count=_integer(bootstrap_raw, "replicate_count"),
            confidence_level=_floating(bootstrap_raw, "confidence_level"),
            minimum_cluster_count=_integer(
                bootstrap_raw, "minimum_cluster_count"
            ),
            interval_method=_text(bootstrap_raw, "interval_method"),
            estimand=_text(bootstrap_raw, "estimand"),
        ),
    )
