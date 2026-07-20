from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from groundloop.m4.models.config import (
    PinnedM3ReuseConfig,
    inspect_local_artifacts,
)
from groundloop.m4.smoke import (
    M4RealPostgresSmokeConfig,
    run_m4_real_postgres_smoke,
    write_smoke_manifest,
)


def test_real_models_and_postgres_insert_then_fresh_port_replay(
    tmp_path: Path,
) -> None:
    if os.environ.get("GROUNDLOOP_RUN_M4_REAL_POSTGRES_SMOKE") != "1":
        pytest.skip("set GROUNDLOOP_RUN_M4_REAL_POSTGRES_SMOKE=1")
    database_url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not database_url:
        pytest.skip("live PostgreSQL URL is absent")

    repo_root = Path(__file__).resolve().parents[3]
    artifact_root = Path(
        os.environ.get("GROUNDLOOP_ARTIFACT_ROOT", str(repo_root))
    ).resolve()
    model_config_path = repo_root / "configs/m4/models/m3_reuse_v1.json"
    model_config = PinnedM3ReuseConfig.load(model_config_path)
    availability = inspect_local_artifacts(model_config, artifact_root=artifact_root)
    if not availability.available:
        pytest.skip("; ".join(availability.problems))

    result = run_m4_real_postgres_smoke(
        M4RealPostgresSmokeConfig(
            database_url=database_url,
            repo_root=repo_root,
            artifact_root=artifact_root,
            model_config_path=model_config_path,
        )
    )
    manifest = result.manifest
    output = tmp_path / "m4-real-postgres-smoke.json"
    write_smoke_manifest(result, output)

    assert manifest["schema_version"] == "groundloop-m4-real-postgres-smoke-v1"
    assert manifest["scope"] == "bounded-integration-smoke-not-model-quality"
    assert manifest["downloads_allowed"] is False
    assert manifest["database_connection_autocommit"] is True
    assert manifest["reconnected_with_fresh_ports_for_replay"] is True
    first = manifest["first_execution"]
    replay = manifest["fresh_process_replay"]
    surfaces = manifest["exact_surfaces"]
    assert isinstance(first, dict)
    assert isinstance(replay, dict)
    assert isinstance(surfaces, dict)
    assert first["state"] == "sealed"
    assert first["admission_embedding_requests"] == 1
    assert first["verifier_backend_pair_calls"] == 1
    assert replay["state"] == "replayed"
    assert replay["admission_embedding_requests"] == 0
    assert replay["verifier_requests"] == 0
    assert replay["verifier_backend_pair_calls"] == 0
    assert replay["database_projection_unchanged"] is True
    assert surfaces["surface_a_grounding"] is True
    assert surfaces["surface_b_coordination"] is True
    assert surfaces["surface_c_evaluation"] is True
    assert manifest["artifact_counts_unchanged_on_replay"] is True
    assert json.loads(output.read_text(encoding="utf-8")) == manifest
