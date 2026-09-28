from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

from groundloop.fyp_end_to_end_demo import (
    FypEndToEndDemoConfig,
    run_fyp_end_to_end_demo,
)
from groundloop.m4.contracts import stable_m4_digest

REPO_ROOT = Path(__file__).resolve().parents[3]


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not value:
        pytest.skip("live PostgreSQL is required for the end-to-end demo")
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def test_deterministic_end_to_end_runner_drops_schema() -> None:
    database_url = _database_url()
    result = run_fyp_end_to_end_demo(
        FypEndToEndDemoConfig(
            database_url=database_url,
            repo_root=REPO_ROOT,
            artifact_root=REPO_ROOT,
        )
    )

    assert result.keep_schema_requested is False
    assert result.schema_removed is True
    assert result.m3.semantic_epoch_id == result.activation.base_epoch_id
    assert result.activation_replay_receipt.replayed is True
    assert result.activation_replay_receipt.run_id == result.m3.run_id
    assert result.activation_replay.epoch_id == result.activation.base_epoch_id
    assert tuple(event.update_kind for event in result.events) == (
        "insert",
        "delete",
        "replace",
    )
    assert tuple(event.event_id for event in result.events) == tuple(
        f"fyp-e2e-{kind}-"
        + stable_m4_digest("fyp-end-to-end-demo-event-id-v1", result.m3.run_id, kind)
        for kind in ("insert", "delete", "replace")
    )
    assert result.m3_provenance_equal is True
    assert result.events[1].embedding_request_count == 0
    assert result.events[1].verifier_call_count == 0
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            "SELECT count(*) FROM pg_namespace WHERE nspname = %s",
            (result.schema_name,),
        ).fetchone()
    assert row == (0,)
