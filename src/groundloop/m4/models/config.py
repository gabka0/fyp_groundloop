"""Strict local-artifact validation and factories for pinned M3 reuse."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from groundloop.ai.embeddings.bge import (
    BGE_MODEL_ARTIFACT,
    BGE_MODEL_ID,
    BGE_QUERY_PREFIX,
    BGE_REVISION,
    BgeSmallEmbedder,
)
from groundloop.ai.embeddings.common import (
    EMBEDDING_DIMENSION,
    ArtifactUnavailableError,
)
from groundloop.ai.verification.adapter import PinnedMiniLMVerifier
from groundloop.ai.verification.artifacts import file_sha256, tree_digest
from groundloop.ai.verification.constants import BASE_MODEL_ID, BASE_MODEL_REVISION
from groundloop.domain import DecisionPolicy
from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.models.contracts import (
    EmbeddingAdapterSpec,
    VerificationAdapterSpec,
)
from groundloop.m4.models.embedding import M4BgeRoleAdapter
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter

M4_MODEL_CONFIG_SCHEMA = "groundloop-m4-m3-reuse-v1"


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(dict[str, object], value)


def _text(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"configuration field {key} must be non-empty text")
    return value


def _integer(values: dict[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"configuration field {key} must be an integer")
    return value


def _number(values: dict[str, object], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"configuration field {key} must be numeric")
    return float(value)


@dataclass(frozen=True, slots=True)
class PinnedM3ReuseConfig:
    embedding_model_id: str
    embedding_revision: str
    embedding_tokenizer_revision: str
    embedding_dimension: int
    embedding_query_prefix: str
    embedding_snapshot_tree_sha256: str
    embedding_cache_relative_path: str
    embedding_snapshot_relative_path: str
    verifier_logical_model_id: str
    verifier_revision: str
    verifier_base_model_id: str
    verifier_base_model_revision: str
    verifier_checkpoint_tree_sha256: str
    verifier_model_file_sha256: str
    verifier_checkpoint_relative_path: str
    verifier_max_length: int
    verifier_batch_size: int
    verifier_prompt_artifact_id: str
    verifier_prompt_template_sha256: str
    calibration_version: str
    calibration_temperature: float
    calibration_file_sha256: str
    calibration_relative_path: str
    decision_policy: DecisionPolicy

    @classmethod
    def load(cls, path: Path) -> PinnedM3ReuseConfig:
        try:
            root_value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValidationError(f"cannot read M4 model config: {path}") from error
        root = _object(root_value, "configuration root")
        if _text(root, "schema_version") != M4_MODEL_CONFIG_SCHEMA:
            raise ValidationError("unknown M4 model configuration schema")
        embedding = _object(root.get("embedding"), "embedding")
        verifier = _object(root.get("verifier"), "verifier")
        calibration = _object(root.get("calibration"), "calibration")
        policy = _object(root.get("decision_policy"), "decision_policy")
        config = cls(
            embedding_model_id=_text(embedding, "model_id"),
            embedding_revision=_text(embedding, "revision"),
            embedding_tokenizer_revision=_text(embedding, "tokenizer_revision"),
            embedding_dimension=_integer(embedding, "dimension"),
            embedding_query_prefix=_text(embedding, "query_prefix"),
            embedding_snapshot_tree_sha256=_text(
                embedding, "snapshot_tree_sha256"
            ),
            embedding_cache_relative_path=_text(embedding, "cache_relative_path"),
            embedding_snapshot_relative_path=_text(
                embedding, "snapshot_relative_path"
            ),
            verifier_logical_model_id=_text(verifier, "logical_model_id"),
            verifier_revision=_text(verifier, "revision"),
            verifier_base_model_id=_text(verifier, "base_model_id"),
            verifier_base_model_revision=_text(verifier, "base_model_revision"),
            verifier_checkpoint_tree_sha256=_text(
                verifier, "checkpoint_tree_sha256"
            ),
            verifier_model_file_sha256=_text(verifier, "model_file_sha256"),
            verifier_checkpoint_relative_path=_text(
                verifier, "checkpoint_relative_path"
            ),
            verifier_max_length=_integer(verifier, "max_length"),
            verifier_batch_size=_integer(verifier, "batch_size"),
            verifier_prompt_artifact_id=_text(verifier, "prompt_artifact_id"),
            verifier_prompt_template_sha256=_text(
                verifier, "prompt_template_sha256"
            ),
            calibration_version=_text(calibration, "version"),
            calibration_temperature=_number(calibration, "temperature"),
            calibration_file_sha256=_text(calibration, "file_sha256"),
            calibration_relative_path=_text(calibration, "relative_path"),
            decision_policy=DecisionPolicy(
                policy_version=_text(policy, "version"),
                support_threshold=_number(policy, "support_threshold"),
                refute_threshold=_number(policy, "refute_threshold"),
                tie_rule_version=_text(policy, "tie_rule_version"),
            ),
        )
        config.validate_frozen_identity()
        return config

    def validate_frozen_identity(self) -> None:
        expected = (
            (self.embedding_model_id, BGE_MODEL_ID, "BGE model"),
            (self.embedding_revision, BGE_REVISION, "BGE revision"),
            (
                self.embedding_tokenizer_revision,
                BGE_REVISION,
                "BGE tokenizer revision",
            ),
            (self.embedding_query_prefix, BGE_QUERY_PREFIX, "BGE query prefix"),
            (self.verifier_base_model_id, BASE_MODEL_ID, "verifier base model"),
            (
                self.verifier_base_model_revision,
                BASE_MODEL_REVISION,
                "verifier base revision",
            ),
        )
        for actual, frozen, label in expected:
            if actual != frozen:
                raise ArtifactConflictError(f"{label} drift")
        if self.embedding_dimension != EMBEDDING_DIMENSION:
            raise ArtifactConflictError("BGE embedding dimension drift")
        if self.verifier_max_length != 256 or self.verifier_batch_size != 8:
            raise ArtifactConflictError("M3 verifier execution configuration drift")


@dataclass(frozen=True, slots=True)
class LocalArtifactAvailability:
    embedding_snapshot: Path
    verifier_checkpoint: Path
    calibration_file: Path
    embedding_valid: bool
    verifier_valid: bool
    calibration_valid: bool
    problems: tuple[str, ...]

    @property
    def available(self) -> bool:
        return self.embedding_valid and self.verifier_valid and self.calibration_valid


def inspect_local_artifacts(
    config: PinnedM3ReuseConfig, *, artifact_root: Path
) -> LocalArtifactAvailability:
    embedding_snapshot = artifact_root / config.embedding_snapshot_relative_path
    verifier_checkpoint = artifact_root / config.verifier_checkpoint_relative_path
    calibration_file = artifact_root / config.calibration_relative_path
    problems: list[str] = []

    embedding_valid = embedding_snapshot.is_dir()
    if embedding_valid:
        embedding_valid = (
            tree_digest(embedding_snapshot)
            == config.embedding_snapshot_tree_sha256
        )
        if not embedding_valid:
            problems.append("BGE snapshot tree hash differs from frozen M3 artifact")
    else:
        problems.append(f"BGE snapshot absent: {embedding_snapshot}")

    verifier_valid = verifier_checkpoint.is_dir()
    if verifier_valid:
        model_file = verifier_checkpoint / "model.safetensors"
        training_manifest = verifier_checkpoint / "groundloop_training_manifest.json"
        verifier_valid = (
            tree_digest(verifier_checkpoint)
            == config.verifier_checkpoint_tree_sha256
            and model_file.is_file()
            and file_sha256(model_file) == config.verifier_model_file_sha256
            and training_manifest.is_file()
        )
        if verifier_valid:
            try:
                manifest_value = json.loads(training_manifest.read_text("utf-8"))
                manifest = _object(manifest_value, "training manifest")
                verifier_valid = (
                    _text(manifest, "base_model_id")
                    == config.verifier_base_model_id
                    and _text(manifest, "base_model_revision")
                    == config.verifier_base_model_revision
                )
            except (OSError, json.JSONDecodeError, ValidationError):
                verifier_valid = False
        if not verifier_valid:
            problems.append("verifier checkpoint identity or hash drift")
    else:
        problems.append(f"verifier checkpoint absent: {verifier_checkpoint}")

    calibration_valid = calibration_file.is_file()
    if calibration_valid:
        calibration_valid = (
            file_sha256(calibration_file) == config.calibration_file_sha256
        )
        if calibration_valid:
            try:
                calibration_value = json.loads(calibration_file.read_text("utf-8"))
                calibration = _object(calibration_value, "calibration")
                calibration_valid = (
                    _text(calibration, "calibration_version")
                    == config.calibration_version
                    and _number(calibration, "temperature")
                    == config.calibration_temperature
                    and _text(calibration, "fit_split") == "development"
                )
            except (OSError, json.JSONDecodeError, ValidationError):
                calibration_valid = False
        if not calibration_valid:
            problems.append("calibration identity, split, temperature or hash drift")
    else:
        problems.append(f"calibration file absent: {calibration_file}")

    return LocalArtifactAvailability(
        embedding_snapshot=embedding_snapshot,
        verifier_checkpoint=verifier_checkpoint,
        calibration_file=calibration_file,
        embedding_valid=embedding_valid,
        verifier_valid=verifier_valid,
        calibration_valid=calibration_valid,
        problems=tuple(problems),
    )


@dataclass(frozen=True, slots=True)
class PinnedM3AdapterBundle:
    embeddings: M4BgeRoleAdapter
    verifier: M4CalibratedVerifierAdapter
    availability: LocalArtifactAvailability


def build_pinned_m3_adapters(
    config: PinnedM3ReuseConfig, *, artifact_root: Path
) -> PinnedM3AdapterBundle:
    availability = inspect_local_artifacts(config, artifact_root=artifact_root)
    if not availability.available:
        raise ArtifactUnavailableError("; ".join(availability.problems))

    embedder = BgeSmallEmbedder(
        cache_dir=artifact_root / config.embedding_cache_relative_path,
        allow_download=False,
    )
    if embedder.model_artifact != BGE_MODEL_ARTIFACT:
        raise ArtifactConflictError("M3 BGE adapter identity drift")
    embedding_spec = EmbeddingAdapterSpec(
        model_artifact=embedder.model_artifact,
        dimension=config.embedding_dimension,
        local_artifact_sha256=config.embedding_snapshot_tree_sha256,
    )
    verifier_backend = PinnedMiniLMVerifier(
        model_path=str(availability.verifier_checkpoint),
        model_revision=config.verifier_revision,
        temperature=config.calibration_temperature,
        max_length=config.verifier_max_length,
        batch_size=config.verifier_batch_size,
        local_files_only=True,
        artifact_sha256=config.verifier_checkpoint_tree_sha256,
        logical_model_id=config.verifier_logical_model_id,
        calibration_version=config.calibration_version,
    )
    if (
        verifier_backend.prompt_artifact.artifact_id
        != config.verifier_prompt_artifact_id
        or verifier_backend.prompt_artifact.template_hash
        != config.verifier_prompt_template_sha256
    ):
        raise ArtifactConflictError("M3 verifier prompt identity drift")
    verifier_spec = VerificationAdapterSpec(
        model_artifact=verifier_backend.model_artifact,
        prompt_artifact=verifier_backend.prompt_artifact,
        calibration_version=config.calibration_version,
        calibration_artifact_sha256=config.calibration_file_sha256,
        temperature=config.calibration_temperature,
        max_length=config.verifier_max_length,
        decision_policy=config.decision_policy,
    )
    return PinnedM3AdapterBundle(
        embeddings=M4BgeRoleAdapter(embedder, embedding_spec),
        verifier=M4CalibratedVerifierAdapter(
            verifier_backend,
            verifier_spec,
            batch_size=config.verifier_batch_size,
        ),
        availability=availability,
    )
