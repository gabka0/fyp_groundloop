from __future__ import annotations

import json
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest

import groundloop.cli as cli
import groundloop.m4.m3_activation as activation_module
import groundloop.m4.models.config as model_config_module


@dataclass(frozen=True, slots=True)
class _Receipt:
    run_id: str = "m3-run-cli"
    base_epoch_id: int = 17
    answer_version_id: str = "answer-cli"
    claim_registry_snapshot_id: str = "registry-cli"
    candidate_policy_id: str = "policy-cli"
    candidate_policy_hash: str = "a" * 64
    claim_count: int = 2
    chunk_count: int = 3
    observation_count: int = 4
    claim_ids: tuple[str, ...] = ("claim-a", "claim-b")
    embedding_model_artifact_id: str = "embedding-model-cli"
    verifier_model_artifact_id: str = "verifier-model-cli"
    verifier_prompt_artifact_id: str = "verifier-prompt-cli"
    calibration_version: str = "calibration-cli"
    calibration_temperature: float = 1.25
    decision_policy_version: str = "decision-cli"
    verifier_execution_spec_hash: str = "b" * 64
    vector_method_version: str = "vector-cli"
    vector_index_kind: str = "exact"
    vector_index_build_config_hash: str = "c" * 64
    vector_search_config_hash: str = "d" * 64
    lexical_method_version: str = "lexical-cli"
    lexical_config_hash: str = "e" * 64
    role_artifact_count: int = 5
    created_role_artifact_count: int = 0
    reused_role_artifact_count: int = 5
    activation_embedding_request_count: int = 0
    baseline_python_mismatch_count: int = 0
    baseline_sql_claim_mismatch_count: int = 0
    baseline_sql_answer_mismatch_count: int = 0
    global_closure_valid: bool = True
    replayed: bool = True

    def to_json(self) -> str:
        return (
            json.dumps(
                {
                    "base_epoch_id": self.base_epoch_id,
                    "replayed": self.replayed,
                    "run_id": self.run_id,
                },
                sort_keys=True,
                indent=2,
            )
            + "\n"
        )


class _NoInferenceEmbeddings:
    def embed_claims(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("CLI adapter construction performed inference")

    def embed_chunks(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("CLI adapter construction performed inference")


@dataclass(frozen=True, slots=True)
class _NoInferenceVerifier:
    spec: object

    def verify_pairs(self, *_args: object, **_kwargs: object) -> None:
        raise AssertionError("CLI adapter construction performed inference")


@dataclass(frozen=True, slots=True)
class _AdapterBundle:
    embeddings: _NoInferenceEmbeddings
    verifier: _NoInferenceVerifier


def test_m4_activate_m3_parser_requires_run_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = cli.build_parser()

    with pytest.raises(SystemExit) as raised:
        parser.parse_args(("m4-activate-m3",))

    assert raised.value.code == 2
    assert "--run-id" in capsys.readouterr().err


def test_m4_activate_m3_always_builds_and_delegates_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = object()
    config = object()
    embedding_adapter = _NoInferenceEmbeddings()
    verifier_spec = object()
    bundle = _AdapterBundle(
        embeddings=embedding_adapter,
        verifier=_NoInferenceVerifier(verifier_spec),
    )
    receipt = _Receipt()
    connection_calls: list[tuple[str, bool]] = []
    schema_calls: list[tuple[object, str]] = []
    config_loads: list[Path] = []
    adapter_builds: list[tuple[object, Path]] = []
    activation_calls: list[
        tuple[object, str, object, object, Path, Path, int | None, int]
    ] = []
    replay_preflight_calls: list[str] = []

    def connect(
        database_url: str, *, autocommit: bool
    ) -> AbstractContextManager[object]:
        connection_calls.append((database_url, autocommit))
        return nullcontext(connection)

    def configure_schema(actual_connection: object, schema: str) -> None:
        schema_calls.append((actual_connection, schema))

    class _ConfigLoader:
        @classmethod
        def load(cls, path: Path) -> object:
            del cls
            config_loads.append(path)
            return config

    def build_adapters(actual_config: object, *, artifact_root: Path) -> _AdapterBundle:
        adapter_builds.append((actual_config, artifact_root))
        return bundle

    def reject_replay_preflight(_connection: object, *, run_id: str) -> _Receipt:
        replay_preflight_calls.append(run_id)
        return receipt

    def activate(
        actual_connection: object,
        *,
        run_id: str,
        embeddings: object,
        verifier_spec: object,
        repo_root: Path,
        lexical_config_path: Path,
        approximate_cap_per_inserted_chunk: int | None,
        frontier_depth: int,
    ) -> _Receipt:
        activation_calls.append(
            (
                actual_connection,
                run_id,
                embeddings,
                verifier_spec,
                repo_root,
                lexical_config_path,
                approximate_cap_per_inserted_chunk,
                frontier_depth,
            )
        )
        return receipt

    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(cli, "_configure_schema", configure_schema)
    monkeypatch.setattr(model_config_module, "PinnedM3ReuseConfig", _ConfigLoader)
    monkeypatch.setattr(
        model_config_module,
        "build_pinned_m3_adapters",
        build_adapters,
    )
    monkeypatch.setattr(
        activation_module,
        "validate_m3_m4_activation_replay",
        reject_replay_preflight,
    )
    monkeypatch.setattr(activation_module, "activate_published_m3_run", activate)

    repo_root = tmp_path / "checkout"
    artifact_root = tmp_path / "artifacts"
    model_config = tmp_path / "config" / "models.json"
    lexical_config = tmp_path / "config" / "lexical.json"
    output = tmp_path / "reports" / "activation.json"
    result = cli.main(
        (
            "m4-activate-m3",
            "--run-id",
            receipt.run_id,
            "--database-url",
            "postgresql+psycopg://groundloop:test@db/groundloop",
            "--schema",
            "groundloop_cli_bridge",
            "--repo-root",
            str(repo_root),
            "--artifact-root",
            str(artifact_root),
            "--model-config",
            str(model_config),
            "--lexical-config",
            str(lexical_config),
            "--approximate-cap",
            "7",
            "--frontier-depth",
            "3",
            "--output",
            str(output),
        )
    )

    assert result == 0
    assert replay_preflight_calls == []
    assert connection_calls == [("postgresql://groundloop:test@db/groundloop", True)]
    assert schema_calls == [(connection, "groundloop_cli_bridge")]
    assert config_loads == [model_config.resolve()]
    assert adapter_builds == [(config, artifact_root.resolve())]
    assert activation_calls == [
        (
            connection,
            receipt.run_id,
            embedding_adapter,
            verifier_spec,
            repo_root.resolve(),
            lexical_config.resolve(),
            7,
            3,
        )
    ]
    assert output.read_text(encoding="utf-8") == receipt.to_json()
    assert capsys.readouterr().out.splitlines() == [
        "M3 run m3-run-cli: REPLAYED as M4 epoch 17",
        "Answer answer-cli: claims=2 chunks=3 observations=4",
        f"Registry registry-cli; policy policy-cli ({'a' * 64})",
        "Claims: claim-a, claim-b",
        "Models: embedding=embedding-model-cli; verifier=verifier-model-cli; "
        "prompt=verifier-prompt-cli; calibration=calibration-cli@1.25; "
        f"decision=decision-cli; verifier_execution={'b' * 64}",
        "Admission: vector=vector-cli/exact; "
        f"vector_build={'c' * 64}; vector_search={'d' * 64}; "
        f"lexical=lexical-cli; lexical_config={'e' * 64}; "
        "role_artifacts=5 (created=0, reused=5); embedding_requests=0",
        "Baseline mismatches: python=0, sql_claim=0, sql_answer=0; global_closure=true",
    ]
