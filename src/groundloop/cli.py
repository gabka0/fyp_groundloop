"""Command-line entry point for the reproducible static M3 pipeline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
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
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
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
        raise ValidationError(f"unsupported M3 CLI config schema: {schema_version}")
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
        corpus_namespace=_text(root.get("corpus_namespace"), "corpus_namespace"),
        chunker_artifact_id=_text(chunker.get("artifact_id"), "chunker.artifact_id"),
        chunk_size=_integer(chunker.get("max_characters"), "chunker.max_characters"),
        question_top_k=_integer(
            retrieval.get("question_top_k"), "retrieval.question_top_k"
        ),
        claim_top_k=_integer(retrieval.get("claim_top_k"), "retrieval.claim_top_k"),
        policy=policy,
        verifier_max_length=_integer(verifier.get("max_length"), "verifier.max_length"),
        verifier_batch_size=_integer(verifier.get("batch_size"), "verifier.batch_size"),
        verifier_logical_model_id=_text(
            verifier.get("logical_model_id"), "verifier.logical_model_id"
        ),
        verifier_revision=_text(verifier.get("revision"), "verifier.revision"),
        canonical_json=canonical,
    )


def _database_url(argument: str | None) -> str:
    value = argument or os.environ.get("GROUNDLOOP_DATABASE_URL")
    if not value:
        raise ValidationError(
            "database URL is required via --database-url or GROUNDLOOP_DATABASE_URL"
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
            verifier=DeterministicFakeVerifier(max_length=config.verifier_max_length),
        )
    if checkpoint is None or calibration_path is None:
        raise ValidationError(
            "real backend requires --verifier-checkpoint and --verifier-calibration"
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
            None if args.verifier_checkpoint is None else Path(args.verifier_checkpoint)
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


def _m4_controlled_eval(args: argparse.Namespace) -> int:
    from groundloop.m4.experiments.__main__ import main as run_controlled_eval

    return run_controlled_eval(
        (
            "--config",
            str(args.config),
            "--output-dir",
            str(args.output_dir),
        )
    )


def _m4_activate_m3(args: argparse.Namespace) -> int:
    """Activate one explicit published M3 run as the first M4 baseline."""
    from groundloop.m4.m3_activation import activate_published_m3_run
    from groundloop.m4.models.config import (
        PinnedM3ReuseConfig,
        build_pinned_m3_adapters,
    )

    repo_root = Path(args.repo_root).resolve()
    artifact_root = Path(args.artifact_root).resolve()
    model_config_path = (
        repo_root / "configs/m4/models/m3_reuse_v1.json"
        if args.model_config is None
        else Path(args.model_config).resolve()
    )
    lexical_config_path = (
        repo_root / "configs/m4/impact/lexical_v1.json"
        if args.lexical_config is None
        else Path(args.lexical_config).resolve()
    )
    model_config = PinnedM3ReuseConfig.load(model_config_path)
    bundle = build_pinned_m3_adapters(model_config, artifact_root=artifact_root)
    with psycopg.connect(
        _database_url(args.database_url), autocommit=True
    ) as connection:
        _configure_schema(connection, str(args.schema))
        receipt = activate_published_m3_run(
            connection,
            run_id=str(args.run_id),
            embeddings=bundle.embeddings,
            verifier_spec=bundle.verifier.spec,
            repo_root=repo_root,
            lexical_config_path=lexical_config_path,
            approximate_cap_per_inserted_chunk=(
                None if args.approximate_cap is None else int(args.approximate_cap)
            ),
            frontier_depth=int(args.frontier_depth),
        )
    if args.output is not None:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(receipt.to_json(), encoding="utf-8")
    action = "REPLAYED" if receipt.replayed else "ACTIVATED"
    print(f"M3 run {receipt.run_id}: {action} as M4 epoch {receipt.base_epoch_id}")
    print(
        f"Answer {receipt.answer_version_id}: claims={receipt.claim_count} "
        f"chunks={receipt.chunk_count} observations={receipt.observation_count}"
    )
    print(
        f"Registry {receipt.claim_registry_snapshot_id}; policy "
        f"{receipt.candidate_policy_id} ({receipt.candidate_policy_hash})"
    )
    print("Claims: " + ", ".join(receipt.claim_ids))
    print(
        f"Models: embedding={receipt.embedding_model_artifact_id}; "
        f"verifier={receipt.verifier_model_artifact_id}; "
        f"prompt={receipt.verifier_prompt_artifact_id}; "
        f"calibration={receipt.calibration_version}@"
        f"{receipt.calibration_temperature:.17g}; "
        f"decision={receipt.decision_policy_version}; "
        f"verifier_execution={receipt.verifier_execution_spec_hash}"
    )
    print(
        f"Admission: vector={receipt.vector_method_version}/"
        f"{receipt.vector_index_kind}; "
        f"vector_build={receipt.vector_index_build_config_hash}; "
        f"vector_search={receipt.vector_search_config_hash}; "
        f"lexical={receipt.lexical_method_version}; "
        f"lexical_config={receipt.lexical_config_hash}; "
        f"role_artifacts={receipt.role_artifact_count} "
        f"(created={receipt.created_role_artifact_count}, "
        f"reused={receipt.reused_role_artifact_count}); "
        f"embedding_requests={receipt.activation_embedding_request_count}"
    )
    print(
        "Baseline mismatches: "
        f"python={receipt.baseline_python_mismatch_count}, "
        f"sql_claim={receipt.baseline_sql_claim_mismatch_count}, "
        f"sql_answer={receipt.baseline_sql_answer_mismatch_count}; "
        f"global_closure={str(receipt.global_closure_valid).lower()}"
    )
    return 0


def _m4_real_smoke(args: argparse.Namespace) -> int:
    from groundloop.m4.smoke import (
        M4RealPostgresSmokeConfig,
        run_m4_real_postgres_smoke,
        write_smoke_manifest,
    )

    result = run_m4_real_postgres_smoke(
        M4RealPostgresSmokeConfig(
            database_url=_database_url(args.database_url),
            repo_root=Path(args.repo_root).resolve(),
            artifact_root=Path(args.artifact_root).resolve(),
            model_config_path=(
                None if args.model_config is None else Path(args.model_config).resolve()
            ),
            lexical_config_path=(
                None
                if args.lexical_config is None
                else Path(args.lexical_config).resolve()
            ),
            keep_schema=bool(args.keep_schema),
        )
    )
    if args.output is not None:
        write_smoke_manifest(result, Path(args.output))
    print(result.to_json(), end="")
    return 0


def _m4_real_history(args: argparse.Namespace) -> int:
    from groundloop.m4.real_dynamic_history import (
        M4RealDynamicHistoryConfig,
        run_m4_real_dynamic_history,
        write_dynamic_history_manifest,
    )

    result = run_m4_real_dynamic_history(
        M4RealDynamicHistoryConfig(
            database_url=_database_url(args.database_url),
            repo_root=Path(args.repo_root).resolve(),
            artifact_root=Path(args.artifact_root).resolve(),
            model_config_path=(
                None if args.model_config is None else Path(args.model_config).resolve()
            ),
            lexical_config_path=(
                None
                if args.lexical_config is None
                else Path(args.lexical_config).resolve()
            ),
            keep_schema=bool(args.keep_schema),
        )
    )
    if args.output is not None:
        write_dynamic_history_manifest(result, Path(args.output))
    print(result.to_json(), end="")
    return 0


def _fyp_demo(args: argparse.Namespace) -> int:
    from groundloop.fyp_demo import execute_fyp_demo, format_fyp_demo_summary
    from groundloop.m4.real_dynamic_history import M4RealDynamicHistoryConfig

    output = Path(args.output)
    _, summary = execute_fyp_demo(
        M4RealDynamicHistoryConfig(
            database_url=_database_url(args.database_url),
            repo_root=Path(args.repo_root).resolve(),
            artifact_root=Path(args.artifact_root).resolve(),
            model_config_path=(
                None if args.model_config is None else Path(args.model_config).resolve()
            ),
            lexical_config_path=(
                None
                if args.lexical_config is None
                else Path(args.lexical_config).resolve()
            ),
            keep_schema=bool(args.keep_schema),
        ),
        output=output,
    )
    print(format_fyp_demo_summary(summary, output=output))
    return 0


def _fyp_end_to_end_demo(args: argparse.Namespace) -> int:
    from groundloop.fyp_end_to_end_demo import (
        FypEndToEndDemoConfig,
        format_fyp_end_to_end_summary,
        run_fyp_end_to_end_demo,
        write_fyp_end_to_end_result,
    )

    output = Path(args.output)
    result = run_fyp_end_to_end_demo(
        FypEndToEndDemoConfig(
            database_url=_database_url(args.database_url),
            repo_root=Path(args.repo_root).resolve(),
            artifact_root=Path(args.artifact_root).resolve(),
            backend=str(args.backend),
            model_config_path=(
                None if args.model_config is None else Path(args.model_config).resolve()
            ),
            lexical_config_path=(
                None
                if args.lexical_config is None
                else Path(args.lexical_config).resolve()
            ),
            keep_schema=bool(args.keep_schema),
        )
    )
    write_fyp_end_to_end_result(result, output)
    print(format_fyp_end_to_end_summary(result, output=output))
    return 0


def _fyp_value_benchmark(args: argparse.Namespace) -> int:
    from groundloop.fyp_value_benchmark import (
        execute_fyp_value_benchmark,
        format_fyp_value_benchmark_summary,
    )

    result, _ = execute_fyp_value_benchmark(
        config_path=Path(args.config),
        output_directory=Path(args.output_dir),
    )
    print(
        format_fyp_value_benchmark_summary(
            result,
            output_directory=Path(args.output_dir),
        )
    )
    return 0


def _fyp_impact_selection(args: argparse.Namespace) -> int:
    from groundloop.fyp_impact_selection import (
        execute_impact_selection,
        format_impact_selection_summary,
    )

    result, _ = execute_impact_selection(
        config_path=Path(args.config),
        source_path=Path(args.source),
        output_directory=Path(args.output_dir),
    )
    print(
        format_impact_selection_summary(
            result,
            output_directory=Path(args.output_dir),
        )
    )
    return 0


def _fyp_impact_pareto(args: argparse.Namespace) -> int:
    from groundloop.fyp_impact_pareto import (
        execute_impact_pareto,
        format_impact_pareto_summary,
    )

    result, _ = execute_impact_pareto(
        config_path=Path(args.config),
        source_path=Path(args.source),
        hosted_config_path=Path(args.hosted_config),
        output_directory=Path(args.output_dir),
    )
    print(
        format_impact_pareto_summary(
            result,
            output_directory=Path(args.output_dir),
        )
    )
    return 0


def _openai_client(args: argparse.Namespace) -> object:
    from groundloop.hosted_verifier import OpenAIHTTPClient

    key = os.environ.get(str(args.api_key_env))
    if key is None:
        raise ValueError(f"environment variable {args.api_key_env} is not set")
    return OpenAIHTTPClient(api_key=key)


def _fyp_hosted_smoke(args: argparse.Namespace) -> int:
    from groundloop.fyp_impact_pareto import (
        evaluate_hosted_accuracy,
        load_gold_annotations,
        load_task5_partitions,
    )
    from groundloop.hosted_verifier import (
        OpenAIHTTPClient,
        actual_result_cost,
        build_request_population,
        load_hosted_verifier_config,
        run_sync_smoke,
        select_development_smoke_requests,
        write_hosted_results,
    )

    hosted_config = load_hosted_verifier_config(Path(args.hosted_config))
    development, _ = load_task5_partitions(
        impact_config_path=Path(args.impact_config),
        source_path=Path(args.source),
    )
    requests = build_request_population(
        config=hosted_config, partition=development
    )
    smoke_requests = select_development_smoke_requests(
        requests=requests,
        partition=development,
        limit=hosted_config.smoke_request_limit,
    )
    client = cast(OpenAIHTTPClient, _openai_client(args))
    results = run_sync_smoke(
        config=hosted_config,
        requests=smoke_requests,
        client=client,
    )
    annotations = load_gold_annotations(
        impact_config_path=Path(args.impact_config),
        source_path=Path(args.source),
    )
    accuracy = evaluate_hosted_accuracy(
        requests=smoke_requests,
        results=results,
        annotations=annotations,
    )
    output = Path(args.output_dir)
    result_path = write_hosted_results(
        results=results, output_path=output / "smoke_results.jsonl"
    )
    cost = actual_result_cost(config=hosted_config, results=results, batch=False)
    report = {
        "schema_version": "groundloop-hosted-verifier-smoke-report-v1",
        "request_count": len(results),
        "accuracy": asdict(accuracy),
        "input_tokens": sum(item.input_tokens for item in results),
        "output_tokens": sum(item.output_tokens for item in results),
        "request_elapsed_ms_sum": sum(
            cast(int, item.elapsed_ms) for item in results
        ),
        "actual_cost_usd": str(cost),
        "result_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
    }
    report_path = output / "smoke_report.json"
    report_path.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    print(
        "Hosted verifier development smoke: "
        f"{accuracy.correct_request_count}/{accuracy.evaluated_request_count} "
        f"correct, {report['input_tokens']} input tokens, "
        f"{report['output_tokens']} output tokens, USD {cost}.\n"
        f"Artifacts: {output}"
    )
    return 0


def _evaluation_hosted_requests(args: argparse.Namespace) -> tuple[object, ...]:
    from groundloop.fyp_impact_pareto import load_task5_partitions
    from groundloop.hosted_verifier import (
        build_request_population,
        load_hosted_verifier_config,
    )

    hosted_config = load_hosted_verifier_config(Path(args.hosted_config))
    _, evaluation = load_task5_partitions(
        impact_config_path=Path(args.impact_config),
        source_path=Path(args.source),
    )
    return cast(
        tuple[object, ...],
        build_request_population(config=hosted_config, partition=evaluation),
    )


def _fyp_hosted_batch_init(args: argparse.Namespace) -> int:
    from groundloop.hosted_verifier import (
        HostedVerifierRequest,
        build_request_manifest,
        load_hosted_verifier_config,
        write_batch_ledger,
        write_role_batch_parts,
    )

    hosted_config = load_hosted_verifier_config(Path(args.hosted_config))
    requests = cast(
        tuple[HostedVerifierRequest, ...], _evaluation_hosted_requests(args)
    )
    manifest = build_request_manifest(config=hosted_config, requests=requests)
    output = Path(args.output_dir)
    ledger = write_role_batch_parts(
        requests=requests,
        request_manifest_hash=manifest.manifest_hash,
        output_directory=output / "inputs",
    )
    ledger_path = write_batch_ledger(ledger, output / "batch_ledger.json")
    print(
        f"Initialized {len(ledger.jobs)} sequential Batch jobs at {ledger_path}; "
        f"projected full-program Batch cost USD {manifest.estimated_batch_cost_usd}."
    )
    return 0


def _fyp_hosted_batch_advance(args: argparse.Namespace) -> int:
    from groundloop.hosted_verifier import (
        OpenAIHTTPClient,
        advance_batch_ledger_once,
        load_batch_ledger,
        write_batch_ledger,
    )

    ledger_path = Path(args.ledger)
    ledger = load_batch_ledger(ledger_path)
    client = cast(OpenAIHTTPClient, _openai_client(args))
    advanced = advance_batch_ledger_once(
        ledger=ledger,
        client=client,
        output_directory=Path(args.output_dir),
    )
    write_batch_ledger(advanced, ledger_path)
    completed = sum(
        item.provider_status == "completed" and item.output_path is not None
        for item in advanced.jobs
    )
    active = next(
        (
            f"{item.role}/{item.part_index}:{item.provider_status}"
            for item in advanced.jobs
            if item.provider_status != "completed" or item.output_path is None
        ),
        "none",
    )
    print(f"Batch jobs complete: {completed}/{len(advanced.jobs)}; active: {active}")
    return 0


def _fyp_hosted_batch_collect(args: argparse.Namespace) -> int:
    from groundloop.fyp_impact_pareto import (
        batch_processing_seconds,
        evaluate_hosted_accuracy,
        evaluate_hosted_effect_frontier,
        load_gold_annotations,
        load_task5_partitions,
    )
    from groundloop.fyp_impact_selection import (
        evaluate_policy,
        load_impact_selection_config,
    )
    from groundloop.hosted_verifier import (
        HostedVerifierRequest,
        actual_result_cost,
        collect_batch_results_by_role,
        load_batch_ledger,
        load_hosted_verifier_config,
        write_hosted_results,
    )

    hosted_config = load_hosted_verifier_config(Path(args.hosted_config))
    impact_config = load_impact_selection_config(Path(args.impact_config))
    _, evaluation = load_task5_partitions(
        impact_config_path=Path(args.impact_config),
        source_path=Path(args.source),
    )
    requests = cast(
        tuple[HostedVerifierRequest, ...], _evaluation_hosted_requests(args)
    )
    ledger = load_batch_ledger(Path(args.ledger))
    results_by_role = collect_batch_results_by_role(
        ledger=ledger, requests=requests
    )
    exhaustive_results = tuple(
        sorted(
            results_by_role["baseline_old"]
            + results_by_role["exhaustive_new"],
            key=lambda item: item.request_id,
        )
    )
    annotations = load_gold_annotations(
        impact_config_path=Path(args.impact_config),
        source_path=Path(args.source),
    )
    accuracy = evaluate_hosted_accuracy(
        requests=requests,
        results=exhaustive_results,
        annotations=annotations,
    )
    selected_accuracy = evaluate_hosted_accuracy(
        requests=requests,
        results=results_by_role["selected_new"],
        annotations=annotations,
    )
    selection_started = time.perf_counter_ns()
    evaluate_policy(
        partition=evaluation,
        policy_id="old_new_rarity_coverage",
        budget=8,
        config=impact_config,
    )
    selection_elapsed_ms = (
        time.perf_counter_ns() - selection_started + 999_999
    ) // 1_000_000
    selected_projection_started = time.perf_counter_ns()
    evaluate_hosted_effect_frontier(
        partition=evaluation,
        requests=requests,
        results=exhaustive_results,
        policy_id="old_new_rarity_coverage",
        budgets=(8,),
    )
    selected_projection_elapsed_ms = (
        time.perf_counter_ns() - selected_projection_started + 999_999
    ) // 1_000_000
    exhaustive_projection_started = time.perf_counter_ns()
    evaluate_hosted_effect_frontier(
        partition=evaluation,
        requests=requests,
        results=exhaustive_results,
        policy_id="old_new_rarity_coverage",
        budgets=(256,),
    )
    exhaustive_projection_elapsed_ms = (
        time.perf_counter_ns() - exhaustive_projection_started + 999_999
    ) // 1_000_000
    effects = evaluate_hosted_effect_frontier(
        partition=evaluation,
        requests=requests,
        results=exhaustive_results,
        policy_id="old_new_rarity_coverage",
        budgets=(1, 2, 4, 8, 16, 32, 64, 256),
    )

    def role_usage(role: str) -> dict[str, object]:
        values = results_by_role[role]
        jobs = tuple(item for item in ledger.jobs if item.role == role)
        if any(item.created_at is None or item.completed_at is None for item in jobs):
            raise ValueError("completed Batch job lacks lifecycle coordinates")
        created = tuple(cast(int, item.created_at) for item in jobs)
        completed = tuple(cast(int, item.completed_at) for item in jobs)
        return {
            "request_count": len(values),
            "batch_call_count": len(jobs),
            "input_tokens": sum(item.input_tokens for item in values),
            "output_tokens": sum(item.output_tokens for item in values),
            "actual_batch_cost_usd": str(
                actual_result_cost(
                    config=hosted_config,
                    results=values,
                    batch=True,
                )
            ),
            "provider_processing_seconds": batch_processing_seconds(
                ledger, role
            ),
            "submission_to_completion_seconds": sum(
                end - start for start, end in zip(created, completed, strict=True)
            ),
        }

    role_metrics = {
        role: role_usage(role)
        for role in ("selected_new", "baseline_old", "exhaustive_new")
    }
    selected_provider_wall_ms = (
        cast(int, role_metrics["selected_new"]["submission_to_completion_seconds"])
        * 1000
    )
    exhaustive_provider_wall_ms = (
        cast(
            int,
            role_metrics["exhaustive_new"]["submission_to_completion_seconds"],
        )
        * 1000
    )
    qualifying = tuple(
        point
        for point in effects
        if point.avoided_verifier_pair_count * 100
        >= point.exhaustive_verifier_pair_count * 80
        and all(
            denominator > 0 and numerator * 100 >= denominator * 95
            for numerator, denominator in (
                (point.pair_effect_numerator, point.pair_effect_denominator),
                (point.claim_effect_numerator, point.claim_effect_denominator),
                (point.status_effect_numerator, point.status_effect_denominator),
                (point.answer_effect_numerator, point.answer_effect_denominator),
            )
        )
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    for role, values in results_by_role.items():
        write_hosted_results(
            results=values,
            output_path=output / f"{role}_results.jsonl",
        )
    report: dict[str, object] = {
        "schema_version": "groundloop-hosted-verifier-evaluation-report-v1",
        "result_scope": "constructed_vitaminc_model_relative_effect_diagnostic",
        "request_manifest_hash": ledger.request_manifest_hash,
        "accuracy": asdict(accuracy),
        "selected_new_accuracy": asdict(selected_accuracy),
        "effect_points": [asdict(item) for item in effects],
        "role_metrics": role_metrics,
        "timing_comparison": {
            "selected": {
                "selection_elapsed_ms": selection_elapsed_ms,
                "provider_submission_to_completion_ms": selected_provider_wall_ms,
                "state_projection_elapsed_ms": selected_projection_elapsed_ms,
                "total_update_elapsed_ms": (
                    selection_elapsed_ms
                    + selected_provider_wall_ms
                    + selected_projection_elapsed_ms
                ),
            },
            "exhaustive": {
                "selection_elapsed_ms": 0,
                "provider_submission_to_completion_ms": (
                    exhaustive_provider_wall_ms
                ),
                "state_projection_elapsed_ms": exhaustive_projection_elapsed_ms,
                "total_update_elapsed_ms": (
                    exhaustive_provider_wall_ms
                    + exhaustive_projection_elapsed_ms
                ),
            },
        },
        "gate": {
            "minimum_work_reduction": {"numerator": 80, "denominator": 100},
            "minimum_each_effect_recall": {
                "numerator": 95,
                "denominator": 100,
            },
            "qualifying_budgets": [item.budget for item in qualifying],
            "verdict": "PASS" if qualifying else "NO_GO",
        },
        "timing_boundary": (
            "provider Batch lifecycle time is measured; the API does not expose "
            "pure accelerator inference time"
        ),
        "limitations": [
            "The effect oracle is the exhaustive hosted-model observation set, "
            "not objective truth.",
            "Each source case is a controlled two-required-claim answer; this is "
            "not a natural deployed answer population.",
            "The held-out source was previously used by M4.13 and is not an "
            "untouched external benchmark.",
        ],
    }
    canonical_without_hash = json.dumps(
        report, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    report["report_sha256"] = hashlib.sha256(
        canonical_without_hash.encode()
    ).hexdigest()
    report_path = output / "hosted_evaluation_report.json"
    report_path.write_text(
        json.dumps(
            report,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        "Hosted verifier held-out evaluation: "
        f"accuracy {accuracy.correct_request_count}/"
        f"{accuracy.evaluated_request_count}; gate {report['gate']}; "
        f"artifacts {output}"
    )
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

    controlled = subcommands.add_parser(
        "m4-controlled-eval",
        help="run the frozen deterministic M4 evaluation fixture",
    )
    controlled.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m4/evaluation/controlled_v1.json"),
    )
    controlled.add_argument("--output-dir", type=Path, required=True)
    controlled.set_defaults(handler=_m4_controlled_eval)

    activate = subcommands.add_parser(
        "m4-activate-m3",
        help="activate one explicit published M3 run as the first M4 baseline",
    )
    activate.add_argument("--run-id", required=True)
    activate.add_argument("--database-url")
    activate.add_argument("--schema", default="groundloop")
    activate.add_argument("--repo-root", type=Path, default=Path.cwd())
    activate.add_argument(
        "--artifact-root",
        type=Path,
        default=Path(os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", Path.cwd())),
    )
    activate.add_argument("--model-config", type=Path)
    activate.add_argument("--lexical-config", type=Path)
    activate.add_argument("--approximate-cap", type=int)
    activate.add_argument("--frontier-depth", type=int, default=1)
    activate.add_argument("--output", type=Path)
    activate.set_defaults(handler=_m4_activate_m3)

    smoke = subcommands.add_parser(
        "m4-real-smoke",
        help="run one real-model M4 insert and exact replay",
    )
    smoke.add_argument("--database-url")
    smoke.add_argument("--repo-root", type=Path, default=Path.cwd())
    smoke.add_argument(
        "--artifact-root",
        type=Path,
        default=Path(os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", Path.cwd())),
    )
    smoke.add_argument("--model-config", type=Path)
    smoke.add_argument("--lexical-config", type=Path)
    smoke.add_argument("--output", type=Path)
    smoke.add_argument("--keep-schema", action="store_true")
    smoke.set_defaults(handler=_m4_real_smoke)

    history = subcommands.add_parser(
        "m4-real-history",
        help=(
            "run measured real-model M4 insert/delete/replace events, "
            "dual-oracle audits, and exact replays"
        ),
    )
    history.add_argument("--database-url")
    history.add_argument("--repo-root", type=Path, default=Path.cwd())
    history.add_argument(
        "--artifact-root",
        type=Path,
        default=Path(os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", Path.cwd())),
    )
    history.add_argument("--model-config", type=Path)
    history.add_argument("--lexical-config", type=Path)
    history.add_argument("--output", type=Path)
    history.add_argument("--keep-schema", action="store_true")
    history.set_defaults(handler=_m4_real_history)

    demo = subcommands.add_parser(
        "fyp-demo",
        help="run and explain the bounded real-model FYP dynamic demo",
    )
    demo.add_argument("--database-url")
    demo.add_argument("--repo-root", type=Path, default=Path.cwd())
    demo.add_argument(
        "--artifact-root",
        type=Path,
        default=Path(os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", Path.cwd())),
    )
    demo.add_argument("--model-config", type=Path)
    demo.add_argument("--lexical-config", type=Path)
    demo.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/fyp-demo/m4-dynamic-history.json"),
    )
    demo.add_argument("--keep-schema", action="store_true")
    demo.set_defaults(handler=_fyp_demo)

    end_to_end = subcommands.add_parser(
        "fyp-e2e-demo",
        help="run the bounded same-schema M3-to-M4 FYP demonstration",
    )
    end_to_end.add_argument("--database-url")
    end_to_end.add_argument("--repo-root", type=Path, default=Path.cwd())
    end_to_end.add_argument(
        "--artifact-root",
        type=Path,
        default=Path(os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", Path.cwd())),
    )
    end_to_end.add_argument(
        "--backend", choices=("deterministic", "real"), default="deterministic"
    )
    end_to_end.add_argument("--model-config", type=Path)
    end_to_end.add_argument("--lexical-config", type=Path)
    end_to_end.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/fyp-e2e-demo/result.json"),
    )
    end_to_end.add_argument("--keep-schema", action="store_true")
    end_to_end.set_defaults(handler=_fyp_end_to_end_demo)

    value_benchmark = subcommands.add_parser(
        "fyp-value-benchmark",
        help="compare selective verifier work with the frozen exhaustive baseline",
    )
    value_benchmark.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m4/evaluation/controlled_v1.json"),
    )
    value_benchmark.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/fyp-value-benchmark"),
    )
    value_benchmark.set_defaults(handler=_fyp_value_benchmark)

    impact_selection = subcommands.add_parser(
        "fyp-impact-selection",
        help="run the held-out text-only impact-selection diagnostic",
    )
    impact_selection.add_argument(
        "--config",
        type=Path,
        default=Path("configs/fyp/impact_selection_v1.json"),
    )
    impact_selection.add_argument(
        "--source",
        type=Path,
        required=True,
        help="exact prepared M4.13 VitaminC development JSONL",
    )
    impact_selection.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/fyp-impact-selection"),
    )
    impact_selection.set_defaults(handler=_fyp_impact_selection)

    impact_pareto = subcommands.add_parser(
        "fyp-impact-pareto",
        help="freeze Task 4 misses, the held-out Pareto, and hosted requests",
    )
    impact_pareto.add_argument(
        "--config",
        type=Path,
        default=Path("configs/fyp/impact_pareto_v1.json"),
    )
    impact_pareto.add_argument(
        "--source",
        type=Path,
        required=True,
        help="exact prepared M4.13 VitaminC development JSONL",
    )
    impact_pareto.add_argument(
        "--hosted-config",
        type=Path,
        default=Path("configs/fyp/hosted_verifier_openai_luna_v1.json"),
    )
    impact_pareto.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/fyp-impact-pareto"),
    )
    impact_pareto.set_defaults(handler=_fyp_impact_pareto)

    hosted_smoke = subcommands.add_parser(
        "fyp-hosted-smoke",
        help="run the capped Task 5C development verifier smoke",
    )
    hosted_smoke.add_argument(
        "--impact-config",
        type=Path,
        default=Path("configs/fyp/impact_selection_v1.json"),
    )
    hosted_smoke.add_argument(
        "--hosted-config",
        type=Path,
        default=Path("configs/fyp/hosted_verifier_openai_luna_v1.json"),
    )
    hosted_smoke.add_argument("--source", type=Path, required=True)
    hosted_smoke.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/fyp-hosted-verifier/smoke"),
    )
    hosted_smoke.add_argument("--api-key-env", default="OPENAI_API_KEY")
    hosted_smoke.set_defaults(handler=_fyp_hosted_smoke)

    hosted_batch_init = subcommands.add_parser(
        "fyp-hosted-batch-init",
        help="freeze resumable Task 5C role-separated Batch inputs",
    )
    hosted_batch_init.add_argument(
        "--impact-config",
        type=Path,
        default=Path("configs/fyp/impact_selection_v1.json"),
    )
    hosted_batch_init.add_argument(
        "--hosted-config",
        type=Path,
        default=Path("configs/fyp/hosted_verifier_openai_luna_v1.json"),
    )
    hosted_batch_init.add_argument("--source", type=Path, required=True)
    hosted_batch_init.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/fyp-hosted-verifier/batch"),
    )
    hosted_batch_init.set_defaults(handler=_fyp_hosted_batch_init)

    hosted_batch_advance = subcommands.add_parser(
        "fyp-hosted-batch-advance",
        help="submit or poll exactly one resumable Task 5C Batch job",
    )
    hosted_batch_advance.add_argument("--ledger", type=Path, required=True)
    hosted_batch_advance.add_argument("--output-dir", type=Path, required=True)
    hosted_batch_advance.add_argument("--api-key-env", default="OPENAI_API_KEY")
    hosted_batch_advance.set_defaults(handler=_fyp_hosted_batch_advance)

    hosted_batch_collect = subcommands.add_parser(
        "fyp-hosted-batch-collect",
        help="validate complete Task 5C Batch outputs and compute the final report",
    )
    hosted_batch_collect.add_argument(
        "--impact-config",
        type=Path,
        default=Path("configs/fyp/impact_selection_v1.json"),
    )
    hosted_batch_collect.add_argument(
        "--hosted-config",
        type=Path,
        default=Path("configs/fyp/hosted_verifier_openai_luna_v1.json"),
    )
    hosted_batch_collect.add_argument("--source", type=Path, required=True)
    hosted_batch_collect.add_argument("--ledger", type=Path, required=True)
    hosted_batch_collect.add_argument("--output-dir", type=Path, required=True)
    hosted_batch_collect.set_defaults(handler=_fyp_hosted_batch_collect)
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
