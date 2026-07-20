"""Coordinator CLI configuration tests that never require model downloads."""

from __future__ import annotations

from pathlib import Path

from groundloop.cli import build_parser, load_cli_config


def test_frozen_cli_config_loads_and_hashes_stably() -> None:
    path = Path("configs/m3/pipeline_deterministic.json")
    first = load_cli_config(path)
    second = load_cli_config(path)

    assert first == second
    assert len(first.content_hash) == 64
    assert first.question_top_k == 6
    assert first.claim_top_k == 4
    assert first.policy.policy_version == "m3-policy-v1"


def test_cli_exposes_database_and_static_registration_commands() -> None:
    parser = build_parser()
    initialize = parser.parse_args(("db-init", "--schema", "test"))
    register = parser.parse_args(
        (
            "m3-register",
            "--corpus",
            "corpus",
            "--question",
            "question",
            "--config",
            "configs/m3/pipeline_deterministic.json",
            "--backend",
            "deterministic",
        )
    )

    assert initialize.command == "db-init"
    assert register.command == "m3-register"
    assert register.backend == "deterministic"


def test_cli_exposes_m4_execution_commands(tmp_path: Path) -> None:
    parser = build_parser()
    controlled = parser.parse_args(
        ("m4-controlled-eval", "--output-dir", str(tmp_path / "report"))
    )
    smoke = parser.parse_args(
        ("m4-real-smoke", "--output", str(tmp_path / "smoke.json"))
    )
    history = parser.parse_args(
        ("m4-real-history", "--output", str(tmp_path / "history.json"))
    )

    assert controlled.command == "m4-controlled-eval"
    assert smoke.command == "m4-real-smoke"
    assert history.command == "m4-real-history"
