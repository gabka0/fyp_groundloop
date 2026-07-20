from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from groundloop.m4.models.config import PinnedM3ReuseConfig, inspect_local_artifacts
from groundloop.m4.real_dynamic_history import (
    M4RealDynamicHistoryConfig,
    run_m4_real_dynamic_history,
    write_dynamic_history_manifest,
)


def test_real_measured_insert_delete_replace_history(tmp_path: Path) -> None:
    if os.environ.get("GROUNDLOOP_RUN_M4_REAL_DYNAMIC_HISTORY") != "1":
        pytest.skip("set GROUNDLOOP_RUN_M4_REAL_DYNAMIC_HISTORY=1")
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

    result = run_m4_real_dynamic_history(
        M4RealDynamicHistoryConfig(
            database_url=database_url,
            repo_root=repo_root,
            artifact_root=artifact_root,
            model_config_path=model_config_path,
        )
    )
    manifest = result.manifest
    output = tmp_path / "m4-real-dynamic-history.json"
    write_dynamic_history_manifest(result, output)

    assert manifest["schema_version"] == "groundloop-m4-real-dynamic-history-v1"
    assert manifest["execution_mode"] == "measured"
    assert manifest["all_events_sealed"] is True
    assert manifest["all_replays_zero_model_calls"] is True
    assert manifest["all_replays_database_unchanged"] is True
    assert manifest["all_events_equal_python_and_sql_oracles"] is True
    registry = manifest["claim_registry"]
    assert isinstance(registry, dict)
    assert registry["prebuilt_outside_event_kernel"] is True
    assert registry["events_carry_identity_only"] is True
    history = manifest["history"]
    assert isinstance(history, list)
    assert [event["update_kind"] for event in history] == [
        "insert",
        "delete",
        "replace",
    ]
    assert [event["state"] for event in history] == ["sealed"] * 3
    assert [
        event["model_calls"]["admission_embedding_requests"] for event in history
    ] == [1, 0, 1]
    assert [
        event["model_calls"]["verifier_backend_pair_calls"] for event in history
    ] == [1, 0, 1]
    assert all(event["compact_registry_identity_only"] for event in history)
    assert all(
        event["exact_surfaces"]["python_full_recomputation_equal"]
        and event["exact_surfaces"]["sql_full_recomputation_equal"]
        and event["fresh_connection_exact_replay"]["zero_model_and_discovery_calls"]
        and event["fresh_connection_exact_replay"]["database_projection_unchanged"]
        for event in history
    )
    assert json.loads(output.read_text(encoding="utf-8")) == manifest
