"""Live one-command M3 registration, publication, and replay gate."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
from psycopg import Connection, sql

from groundloop.cli import main


def test_deterministic_cli_publishes_and_reuses_complete_run(
    live_connection: Connection[tuple[object, ...]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ[
        "GROUNDLOOP_DATABASE_URL"
    ]
    schema = "groundloop_m3_cli_" + uuid.uuid4().hex
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "guide.txt").write_text(
        "GroundLoop maintains structured grounding state incrementally.",
        encoding="utf-8",
    )
    first_output = tmp_path / "first.json"
    replay_output = tmp_path / "replay.json"
    base_args = (
        "m3-register",
        "--corpus",
        str(corpus),
        "--question",
        "What does GroundLoop maintain?",
        "--config",
        "configs/m3/pipeline_deterministic.json",
        "--backend",
        "deterministic",
        "--database-url",
        database_url,
        "--schema",
        schema,
    )
    try:
        assert main(base_args + ("--output", str(first_output))) == 0
        assert main(base_args + ("--output", str(replay_output))) == 0

        first = json.loads(first_output.read_text(encoding="utf-8"))
        replay = json.loads(replay_output.read_text(encoding="utf-8"))
        first_manifest = first["manifest"]
        replay_manifest = replay["manifest"]
        assert first_manifest["status"] == "published"
        assert first_manifest["run_id"] == replay_manifest["run_id"]
        assert first_manifest["new_artifact_ids"]
        assert not replay_manifest["new_artifact_ids"]
        assert replay_manifest["reused_artifact_ids"]
        assert first_manifest["answer"]
        assert first_manifest["claims"]
        assert first_manifest["retrieval_candidates"]
        assert first_manifest["verifications"]
        assert first_manifest["claim_states"]
        assert first_manifest["answer_states"]
        assert len(first["models"]) == 4
        assert len(first["prompts"]) == 3

        live_connection.rollback()
        live_connection.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_pipeline_run"
        ).fetchone() == (1,)
        assert live_connection.execute(
            "SELECT count(*) FROM groundloop_semantic_observation"
        ).fetchone() == (1,)
    finally:
        live_connection.rollback()
        live_connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
        live_connection.commit()

    output = capsys.readouterr().out
    assert "Answer:" in output
    assert "Evidence candidates:" in output
    assert "Verifier observations:" in output
    assert "(REUSED)" in output
