from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from groundloop import cli
from groundloop.errors import ValidationError
from groundloop.fyp_demo import (
    FypDemoSummary,
    format_fyp_demo_summary,
    summarize_fyp_demo,
)
from groundloop.m4.real_dynamic_history import (
    M4RealDynamicHistoryConfig,
    M4RealDynamicHistoryResult,
)


def _event(
    *,
    event_id: str,
    update_kind: str,
    epoch_id: int,
    claim_status: str,
    answer_status: str,
    support_count: int,
    refute_count: int,
    discovery: int,
    embedding: int,
    verifier: int,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "update_kind": update_kind,
        "epoch_id": epoch_id,
        "state": "sealed",
        "publication_id": f"{1000 + epoch_id:064x}",
        "compact_registry_identity_only": True,
        "model_calls": {
            "discovery": discovery,
            "admission_embedding_requests": embedding,
            "verifier_requests": verifier,
            "verifier_backend_pair_calls": verifier,
        },
        "exact_surfaces": {
            "python_full_recomputation_equal": True,
            "sql_full_recomputation_equal": True,
            "claim_mismatch_count": 0,
            "answer_mismatch_count": 0,
            "epoch_revision": 4,
            "open_job_count": 0,
            "open_scope_count": 0,
            "claim_state": {
                "support_count": support_count,
                "refute_count": refute_count,
                "best_support_score": 0.95 if support_count else None,
                "best_refute_score": 0.9,
                "supporting_observation_ids": [
                    f"{100 + item:064x}" for item in range(support_count)
                ],
                "refuting_observation_ids": [
                    f"{200 + item:064x}" for item in range(refute_count)
                ],
                "status": claim_status,
            },
            "answer_state": {
                "required_claim_count": 1,
                "supported_count": 0,
                "unsupported_count": 0,
                "refuted_count": 1 if answer_status == "contradicted" else 0,
                "conflicted_count": 1 if answer_status == "conflicted" else 0,
                "status": answer_status,
            },
        },
        "durable_provenance": {
            "jobs": discovery + verifier,
            "attempts": discovery + verifier,
            "discovery_results": discovery,
            "channel_hits": 2 * verifier,
            "admitted_pairs": verifier,
            "working_observation_deltas": 1,
            "semantic_observations": verifier,
            "verification_executions": verifier,
            "pair_judgments": verifier,
        },
        "fresh_connection_exact_replay": {
            "state": "replayed",
            "zero_model_and_discovery_calls": True,
            "database_projection_unchanged": True,
            "projection_sha256": f"{epoch_id:064x}",
            "table_count": 62,
        },
    }


def _manifest() -> dict[str, object]:
    return {
        "schema_version": "groundloop-m4-real-dynamic-history-v1",
        "scope": "bounded-real-dynamic-integration-not-model-quality",
        "downloads_allowed": False,
        "execution_mode": "measured",
        "postgres_version": "PostgreSQL 16.14",
        "pgvector_version": "0.8.5",
        "claim_registry": {
            "snapshot_id": "1" * 64,
            "claim_count": 1,
            "prebuilt_outside_event_kernel": True,
            "events_carry_identity_only": True,
        },
        "candidate_policy_id": "m4-real-postgres-smoke-policy-v1",
        "candidate_policy_hash": "2" * 64,
        "embedding_model_artifact_id": "3" * 64,
        "embedding_model_tree_sha256": "4" * 64,
        "verifier_model_artifact_id": "5" * 64,
        "verifier_checkpoint_tree_sha256": "6" * 64,
        "verifier_prompt_artifact_id": "groundloop-verifier-pair-v1",
        "verifier_prompt_template_sha256": "7" * 64,
        "calibration_version": "temperature-v1:fixture",
        "calibration_artifact_sha256": "8" * 64,
        "decision_policy_version": "m3-policy-v1",
        "decision_policy_hash": "9" * 64,
        "verifier_execution_spec_hash": "a" * 64,
        "history": [
            _event(
                event_id="m4-real-history-insert",
                update_kind="insert",
                epoch_id=11,
                claim_status="conflicted",
                answer_status="conflicted",
                support_count=1,
                refute_count=1,
                discovery=1,
                embedding=1,
                verifier=1,
            ),
            _event(
                event_id="m4-real-history-delete",
                update_kind="delete",
                epoch_id=12,
                claim_status="refuted",
                answer_status="contradicted",
                support_count=0,
                refute_count=1,
                discovery=1,
                embedding=0,
                verifier=0,
            ),
            _event(
                event_id="m4-real-history-replace",
                update_kind="replace",
                epoch_id=13,
                claim_status="refuted",
                answer_status="contradicted",
                support_count=0,
                refute_count=2,
                discovery=1,
                embedding=1,
                verifier=1,
            ),
        ],
        "history_epoch_ids": [11, 12, 13],
        "reconnected_for_every_event_and_replay": True,
        "all_events_sealed": True,
        "all_replays_zero_model_calls": True,
        "all_replays_database_unchanged": True,
        "all_events_equal_python_and_sql_oracles": True,
    }


def test_summary_exposes_trajectory_work_and_evidence_boundary(
    tmp_path: Path,
) -> None:
    summary = summarize_fyp_demo(_manifest())

    assert [event.update_kind for event in summary.events] == [
        "insert",
        "delete",
        "replace",
    ]
    assert [event.claim_status for event in summary.events] == [
        "conflicted",
        "refuted",
        "refuted",
    ]
    assert [event.answer_status for event in summary.events] == [
        "conflicted",
        "contradicted",
        "contradicted",
    ]
    assert summary.total_discovery_calls == 3
    assert summary.total_embedding_requests == 2
    assert summary.total_verifier_pair_calls == 2

    rendered = format_fyp_demo_summary(summary, output=tmp_path / "result.json")
    assert "GroundLoop FYP demo: PASS" in rendered
    assert "INSERT   CONFLICTED  CONFLICTED    1/1" in rendered
    assert "JOBS  OBS" in rendered
    assert "incremental published state matched" in rendered
    assert "does not establish objective truth" in rendered
    assert str((tmp_path / "result.json").resolve()) in rendered


@pytest.mark.parametrize(
    ("path", "value", "message"),
    (
        (("schema_version",), "other", "schema_version"),
        (("all_events_sealed",), False, "all_events_sealed"),
        (("all_events_equal_python_and_sql_oracles",), 1, "all_events_equal"),
        (("history", 0, "update_kind"), "delete", "update_kind"),
        (("history", 0, "state"), "failed", "state"),
        (("history", 0, "publication_id"), "not-a-hash", "lowercase SHA-256"),
        (
            ("history", 0, "exact_surfaces", "python_full_recomputation_equal"),
            False,
            "python_full_recomputation_equal",
        ),
        (
            ("history", 0, "exact_surfaces", "claim_mismatch_count"),
            1,
            "claim_mismatch_count",
        ),
        (
            ("history", 0, "exact_surfaces", "answer_mismatch_count"),
            False,
            "answer_mismatch_count",
        ),
        (
            (
                "history",
                0,
                "fresh_connection_exact_replay",
                "zero_model_and_discovery_calls",
            ),
            False,
            "replay_zero_model_and_discovery_calls",
        ),
        (
            (
                "history",
                0,
                "fresh_connection_exact_replay",
                "database_projection_unchanged",
            ),
            False,
            "replay_database_projection_unchanged",
        ),
        (
            ("history", 0, "exact_surfaces", "claim_state", "status"),
            "supported",
            "claim_status",
        ),
        (
            (
                "history",
                0,
                "exact_surfaces",
                "claim_state",
                "supporting_observation_ids",
            ),
            [],
            "support witness count drifted",
        ),
        (
            ("history", 0, "exact_surfaces", "claim_state", "best_support_score"),
            None,
            "best_support_score",
        ),
        (
            ("history", 0, "exact_surfaces", "epoch_revision"),
            0,
            "epoch_revision must be positive",
        ),
        (
            ("history", 0, "exact_surfaces", "answer_state", "refuted_count"),
            1,
            "answer_state.refuted_count",
        ),
        (
            ("history", 0, "durable_provenance", "semantic_observations"),
            0,
            "durable_provenance.semantic_observations",
        ),
        (
            ("history", 0, "fresh_connection_exact_replay", "projection_sha256"),
            "not-a-hash",
            "lowercase SHA-256",
        ),
        (
            ("history", 2, "model_calls", "verifier_backend_pair_calls"),
            0,
            "verifier_pair_calls",
        ),
    ),
)
def test_summary_rejects_weakened_or_different_history(
    path: tuple[str | int, ...], value: object, message: str
) -> None:
    manifest: Any = deepcopy(_manifest())
    target = manifest
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value

    with pytest.raises(ValidationError, match=message):
        summarize_fyp_demo(manifest)


def test_summary_requires_strictly_increasing_epochs() -> None:
    manifest: Any = _manifest()
    manifest["history"][1]["epoch_id"] = 11

    with pytest.raises(ValidationError, match="strictly increasing"):
        summarize_fyp_demo(manifest)


def test_fyp_demo_cli_delegates_to_m4_history_and_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import groundloop.fyp_demo as demo_module

    manifest = _manifest()
    summary = summarize_fyp_demo(manifest)
    captured: dict[str, object] = {}

    def fake_execute(
        config: M4RealDynamicHistoryConfig, *, output: Path
    ) -> tuple[M4RealDynamicHistoryResult, FypDemoSummary]:
        captured["config"] = config
        captured["output"] = output
        return M4RealDynamicHistoryResult(manifest), summary

    monkeypatch.setattr(demo_module, "execute_fyp_demo", fake_execute)
    output = tmp_path / "manifest.json"
    result = cli.main(
        (
            "fyp-demo",
            "--database-url",
            "postgresql://fixture",
            "--repo-root",
            str(tmp_path),
            "--artifact-root",
            str(tmp_path),
            "--output",
            str(output),
        )
    )

    assert result == 0
    assert captured["output"] == output
    config = captured["config"]
    assert isinstance(config, M4RealDynamicHistoryConfig)
    assert config.database_url == "postgresql://fixture"
    assert config.repo_root == tmp_path.resolve()
    assert "GroundLoop FYP demo: PASS" in capsys.readouterr().out
