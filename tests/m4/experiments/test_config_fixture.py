"""Frozen configuration and table-driven fixture tests."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.contracts import AdmissionChannel
from groundloop.m4.experiments import (
    PolicyKind,
    build_frozen_controlled_fixture_v1,
    load_controlled_evaluation_config,
)

CONFIG_PATH = Path("configs/m4/evaluation/controlled_v1.json")


def test_config_freezes_all_five_policies_and_history_bootstrap() -> None:
    first = load_controlled_evaluation_config(CONFIG_PATH)
    second = load_controlled_evaluation_config(CONFIG_PATH)

    assert first == second
    assert first.manifest_hash == second.manifest_hash
    assert {policy.kind for policy in first.policies} == set(PolicyKind)
    assert first.bootstrap.seed == 20260719
    assert first.bootstrap.replicate_count == 10_000
    assert first.bootstrap.minimum_cluster_count == 2
    assert all(
        policy.approximate_budget_per_inserted_chunk == 1
        for policy in first.policies
    )
    frontier = next(
        policy
        for policy in first.policies
        if policy.kind is PolicyKind.UNION_LINEAGE_FRONTIER
    )
    assert frontier.frontier_budget_per_inserted_chunk == 1


def test_config_loader_rejects_unknown_fields_and_incomplete_policy_set(
    tmp_path: Path,
) -> None:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["ignored"] = True
    unknown = tmp_path / "unknown.json"
    unknown.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="unknown fields"):
        load_controlled_evaluation_config(unknown)

    del payload["ignored"]
    payload["policies"] = payload["policies"][:-1]
    incomplete = tmp_path / "incomplete.json"
    incomplete.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="every frozen"):
        load_controlled_evaluation_config(incomplete)

    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["bootstrap"]["seed"] += 1
    changed_seed = tmp_path / "changed-seed.json"
    changed_seed.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="frozen bootstrap"):
        load_controlled_evaluation_config(changed_seed)


def test_fixture_is_content_stable_complete_and_has_no_hidden_labels() -> None:
    first = build_frozen_controlled_fixture_v1()
    second = build_frozen_controlled_fixture_v1()

    assert first == second
    assert first.manifest_hash == second.manifest_hash
    assert len(first.histories) == 4
    assert len(first.events) == 12
    insert = next(event for event in first.events if event.event_id == "test-h1-insert")
    assert len(insert.judgments) == 2
    assert {candidate.channel for candidate in insert.candidates} == {
        AdmissionChannel.VECTOR,
        AdmissionChannel.LEXICAL,
        AdmissionChannel.LINEAGE,
        AdmissionChannel.FRONTIER,
    }
    assert all(
        not hasattr(candidate, "derived_label") for candidate in insert.candidates
    )

    changed_judgments = list(insert.judgments)
    changed_judgments[0] = replace(
        changed_judgments[0], support_score=0.06, neutral_score=0.89
    )
    changed_event = replace(insert, judgments=tuple(changed_judgments))
    changed_events = tuple(
        changed_event if event.event_id == insert.event_id else event
        for event in first.events
    )
    changed_fixture = replace(first, events=changed_events)
    assert changed_fixture.manifest_hash != first.manifest_hash


def test_fixture_and_policy_contracts_fail_closed() -> None:
    fixture = build_frozen_controlled_fixture_v1()
    with pytest.raises(ValidationError, match="canonical order"):
        replace(fixture, events=tuple(reversed(fixture.events)))

    config = load_controlled_evaluation_config(CONFIG_PATH)
    frontier = next(
        policy
        for policy in config.policies
        if policy.kind is PolicyKind.UNION_LINEAGE_FRONTIER
    )
    with pytest.raises(ValidationError, match="frontier budget"):
        replace(frontier, frontier_budget_per_inserted_chunk=0)
