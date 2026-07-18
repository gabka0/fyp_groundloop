"""Command-line entry point for the reproducible static M3 pipeline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

import psycopg
from psycopg import Connection, sql

from groundloop.ai.application import (
    M3Application,
    M3ApplicationConfig,
    StaticEmbeddingProvider,
)
from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.claim_extraction import (
    DeterministicClaimExtractor,
    QwenClaimExtractor,
)
from groundloop.ai.contracts import (
    AnswerGenerator,
    ClaimExtractor,
    EvidenceVerifier,
    ModelArtifact,
    PipelineRunManifest,
    PromptArtifact,
    ScoreTriple,
)
from groundloop.ai.embeddings import (
    BgeSmallEmbedder,
    DeterministicFakeEmbedder,
)
from groundloop.ai.generation import (
    DeterministicAnswerGenerator,
    QwenAnswerGenerator,
    QwenCompletionBackend,
)
from groundloop.ai.manifest import manifest_to_dict
from groundloop.ai.persistence import PostgresArtifactStore
from groundloop.ai.verification import (
    DeterministicFakeVerifier,
    PinnedMiniLMVerifier,
    TemperatureCalibration,
)
from groundloop.ai.verification.artifacts import tree_digest
from groundloop.domain import (
    DecisionPolicy,
    ModelStamp,
    SemanticObservation,
    SubjectKind,
)
from groundloop.errors import GroundLoopError, ValidationError
from groundloop.policy import decide


@dataclass(frozen=True, slots=True)
class CliConfig:
    schema_version: str
    corpus_namespace: str
    chunker_artifact_id: str
    chunk_size: int
    question_top_k: int
    claim_top_k: int
    policy: DecisionPolicy
    verifier_max_length: int
    verifier_batch_size: int
    verifier_logical_model_id: str
    verifier_revision: str
    canonical_json: str

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.canonical_json.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RuntimeComponents:
    embedder: StaticEmbeddingProvider
    generator: AnswerGenerator
    extractor: ClaimExtractor
    verifier: EvidenceVerifier

    @property
    def models(self) -> tuple[ModelArtifact, ...]:
        return (
            self.embedder.model_artifact,
            self.generator.model_artifact,
            self.extractor.model_artifact,
            self.verifier.model_artifact,
        )

    @property
    def prompts(self) -> tuple[PromptArtifact, ...]:
        return (
            self.generator.prompt_artifact,
            self.extractor.prompt_artifact,
            self.verifier.prompt_artifact,
        )


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(dict[str, object], value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonempty string")
    return value.strip()


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValidationError(f"{name} must be a positive integer")
    return value


def _number(value: object, name: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValidationError(f"{name} must be numeric")
    return float(value)


def load_cli_config(path: Path) -> CliConfig:
    """Load and validate the intentionally small coordinator-owned config."""
    if not path.is_file():
        raise FileNotFoundError(f"M3 config is absent: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    root = _mapping(value, "M3 config")
    chunker = _mapping(root.get("chunker"), "chunker")
    retrieval = _mapping(root.get("retrieval"), "retrieval")
    policy_value = _mapping(root.get("policy"), "policy")
    verifier = _mapping(root.get("verifier"), "verifier")
    canonical = json.dumps(root, sort_keys=True, separators=(",", ":"))
    schema_version = _text(root.get("schema_version"), "schema_version")
    if schema_version != "groundloop-m3-cli-config-v1":
        raise ValidationError(
            f"unsupported M3 CLI config schema: {schema_version}"
        )
    policy = DecisionPolicy(
        policy_version=_text(policy_value.get("version"), "policy.version"),
        support_threshold=_number(
            policy_value.get("support_threshold"), "policy.support_threshold"
        ),
        refute_threshold=_number(
            policy_value.get("refute_threshold"), "policy.refute_threshold"
        ),
    )
    return CliConfig(
        schema_version=schema_version,
        corpus_namespace=_text(
            root.get("corpus_namespace"), "corpus_namespace"
        ),
        chunker_artifact_id=_text(
            chunker.get("artifact_id"), "chunker.artifact_id"
        ),
        chunk_size=_integer(chunker.get("max_characters"), "chunker.max_characters"),
        question_top_k=_integer(
            retrieval.get("question_top_k"), "retrieval.question_top_k"
        ),
        claim_top_k=_integer(
            retrieval.get("claim_top_k"), "retrieval.claim_top_k"
        ),
        policy=policy,
        verifier_max_length=_integer(
            verifier.get("max_length"), "verifier.max_length"
        ),
        verifier_batch_size=_integer(
            verifier.get("batch_size"), "verifier.batch_size"
        ),
        verifier_logical_model_id=_text(
            verifier.get("logical_model_id"), "verifier.logical_model_id"
        ),
        verifier_revision=_text(
            verifier.get("revision"), "verifier.revision"
        ),
        canonical_json=canonical,
    )


def _database_url(argument: str | None) -> str:
    value = argument or os.environ.get("GROUNDLOOP_DATABASE_URL")
    if not value:
        raise ValidationError(
            "database URL is required via --database-url or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def _configure_schema(connection: Connection[Any], schema: str) -> None:
    if not schema.strip():
        raise ValidationError("database schema must be nonempty")
    with connection.transaction():
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
        connection.execute(
            sql.SQL("SET LOCAL search_path TO {}, public").format(
                sql.Identifier(schema)
            )
        )
    connection.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
    )
    connection.commit()


def _components(
    *,
    backend: str,
    config: CliConfig,
    checkpoint: Path | None,
    calibration_path: Path | None,
    embedding_cache: Path | None,
    allow_model_download: bool,
) -> RuntimeComponents:
    if backend == "deterministic":
        return RuntimeComponents(
            embedder=DeterministicFakeEmbedder(),
            generator=DeterministicAnswerGenerator(),
            extractor=DeterministicClaimExtractor(),
            verifier=DeterministicFakeVerifier(
                max_length=config.verifier_max_length
            ),
        )
    if checkpoint is None or calibration_path is None:
        raise ValidationError(
            "real backend requires --verifier-checkpoint and "
            "--verifier-calibration"
        )
    calibration = TemperatureCalibration.read_json(calibration_path)
    checkpoint_sha256 = tree_digest(checkpoint)
    qwen = QwenCompletionBackend(allow_download=allow_model_download)
    return RuntimeComponents(
        embedder=BgeSmallEmbedder(
            cache_dir=embedding_cache,
            allow_download=allow_model_download,
        ),
        generator=QwenAnswerGenerator(backend=qwen),
        extractor=QwenClaimExtractor(backend=qwen),
        verifier=PinnedMiniLMVerifier(
            model_path=str(checkpoint),
            model_revision=config.verifier_revision,
            temperature=calibration.temperature,
            max_length=config.verifier_max_length,
            batch_size=config.verifier_batch_size,
            local_files_only=True,
            artifact_sha256=checkpoint_sha256,
            logical_model_id=config.verifier_logical_model_id,
            calibration_version=calibration.calibration_version,
        ),
    )


def _write_manifest(
    path: Path,
    *,
    manifest: PipelineRunManifest,
    components: RuntimeComponents,
    config: CliConfig,
    backend: str,
) -> None:
    payload = {
        "schema_version": "groundloop-m3-cli-output-v1",
        "backend": backend,
        "config_file_hash": config.content_hash,
        "runtime_config_hash": manifest.config_hash,
        "models": [asdict(item) for item in components.models],
        "prompts": [asdict(item) for item in components.prompts],
        "policy": asdict(config.policy),
        "manifest": manifest_to_dict(manifest),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _derived_label(scores: ScoreTriple, policy: DecisionPolicy) -> str:
    observation = SemanticObservation(
        observation_id="cli-display-only",
        subject_kind=SubjectKind.CLAIM,
        subject_id="cli-display-only",
        chunk_version_id="cli-display-only",
        task_type="direct_verification",
        support_score=scores.support,
        refute_score=scores.refute,
        neutral_score=scores.neutral,
        producer=ModelStamp("cli", "cli", "cli"),
        input_hash="cli-display-only",
    )
    return decide(observation, policy).value.upper()


def _display(
    manifest: PipelineRunManifest, policy: DecisionPolicy, output: Path
) -> None:
    reused = bool(manifest.reused_artifact_ids and not manifest.new_artifact_ids)
    print(
        f"Run {manifest.run_id}: {manifest.status.value.upper()} "
        f"({'REUSED' if reused else 'NEW'})"
    )
    if manifest.answer is not None:
        print(f"Answer: {manifest.answer.text}")
        print("Citations: " + ", ".join(manifest.answer.cited_chunk_version_ids))
    print("Claims:")
    for claim in manifest.claims:
        requirement = "required" if claim.required else "optional"
        print(f"  {claim.local_claim_id} [{requirement}]: {claim.text}")
    print("Evidence candidates:")
    for candidate in manifest.retrieval_candidates:
        print(
            f"  {candidate.query_kind.value} rank={candidate.rank} "
            f"score={candidate.score:.6f} chunk={candidate.chunk_version_id}"
        )
    print("Verifier observations:")
    for result in manifest.verifications:
        label = _derived_label(result.scores, policy)
        print(
            f"  claim={result.claim_id} chunk={result.chunk_version_id} "
            f"support={result.scores.support:.6f} "
            f"refute={result.scores.refute:.6f} "
            f"neutral={result.scores.neutral:.6f} label={label}"
        )
    print("Claim states:")
    for claim_state in manifest.claim_states:
        print(f"  {claim_state.claim_id}: {claim_state.status.value}")
    print("Answer states:")
    for answer_state in manifest.answer_states:
        print(f"  {answer_state.answer_version_id}: {answer_state.status.value}")
    print(f"Machine manifest: {output.resolve()}")


def _db_init(args: argparse.Namespace) -> int:
    with psycopg.connect(_database_url(args.database_url)) as connection:
        _configure_schema(connection, str(args.schema))
        initialized = PostgresArtifactStore(connection).initialize_schema()
    print(
        f"GroundLoop schema {args.schema!r} "
        f"{'initialized' if initialized else 'already initialized'}"
    )
    return 0


def _m3_register(args: argparse.Namespace) -> int:
    config = load_cli_config(Path(args.config))
    components = _components(
        backend=str(args.backend),
        config=config,
        checkpoint=(
            None
            if args.verifier_checkpoint is None
            else Path(args.verifier_checkpoint)
        ),
        calibration_path=(
            None
            if args.verifier_calibration is None
            else Path(args.verifier_calibration)
        ),
        embedding_cache=(
            None if args.embedding_cache is None else Path(args.embedding_cache)
        ),
        allow_model_download=bool(args.allow_model_download),
    )
    runtime_hash = hashlib.sha256(
        (
            config.canonical_json
            + "\0"
            + str(args.backend)
            + "\0"
            + components.verifier.calibration_version
            + "\0"
            + repr(components.verifier.temperature)
        ).encode("utf-8")
    ).hexdigest()
    with psycopg.connect(_database_url(args.database_url)) as connection:
        _configure_schema(connection, str(args.schema))
        store = PostgresArtifactStore(connection)
        store.initialize_schema()
        application = M3Application(
            store=store,
            chunker=FixedCharChunker(
                artifact_id=config.chunker_artifact_id,
                max_characters=config.chunk_size,
            ),
            embedder=components.embedder,
            generator=components.generator,
            extractor=components.extractor,
            verifier=components.verifier,
            config=M3ApplicationConfig(
                config_hash=runtime_hash,
                corpus_namespace=config.corpus_namespace,
                question_top_k=config.question_top_k,
                claim_top_k=config.claim_top_k,
                policy=config.policy,
                cold_start=bool(args.cold_start),
            ),
        )
        manifest = application.register(Path(args.corpus), str(args.question))
    output = Path(args.output)
    _write_manifest(
        output,
        manifest=manifest,
        components=components,
        config=config,
        backend=str(args.backend),
    )
    _display(manifest, config.policy, output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="groundloop")
    subcommands = parser.add_subparsers(dest="command", required=True)
    initialize = subcommands.add_parser("db-init", help="initialize one schema")
    initialize.add_argument("--database-url")
    initialize.add_argument("--schema", default="groundloop")
    initialize.set_defaults(handler=_db_init)

    register = subcommands.add_parser(
        "m3-register", help="register one static grounded answer"
    )
    register.add_argument("--corpus", type=Path, required=True)
    register.add_argument("--question", required=True)
    register.add_argument("--config", type=Path, required=True)
    register.add_argument("--database-url")
    register.add_argument("--schema", default="groundloop")
    register.add_argument(
        "--backend", choices=("deterministic", "real"), default="real"
    )
    register.add_argument("--verifier-checkpoint", type=Path)
    register.add_argument("--verifier-calibration", type=Path)
    register.add_argument("--embedding-cache", type=Path)
    register.add_argument("--allow-model-download", action="store_true")
    register.add_argument("--cold-start", action="store_true")
    register.add_argument(
        "--output", type=Path, default=Path("artifacts/m3-last-run.json")
    )
    register.set_defaults(handler=_m3_register)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    handler = cast(Any, arguments.handler)
    try:
        return int(handler(arguments))
    except (GroundLoopError, OSError, psycopg.Error, ValueError) as error:
        print(f"groundloop: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
