"""Isolated fake-provider and financial failure-injection tests; no paid calls."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from groundloop.baselines.engines import SignedDeltaTreatment
from groundloop.events import _mutate
from groundloop.fyp_viability import (
    FakeProvider,
    Source,
    answer_fields_correct,
    cache_key,
    delta_bundle,
    live_diagnostic,
    parallel,
    publish,
    qualified_useful,
    selected_claims,
    stage,
    structured,
    structured_fixture,
    verify,
)
from groundloop.fyp_viability_provider import (
    BudgetLedger,
    LiveProvider,
    ProviderFailure,
    docs_cost,
    parse_response,
)
from groundloop.reference import compute_all_states

ROOT = Path(__file__).resolve().parents[2]


def ledger(tmp_path: Path, cost: str = "0") -> BudgetLedger:
    return BudgetLedger(
        tmp_path / "budget.sqlite3", {"cost_usd": cost, "sha256": "test"}
    )


def response(status: str = "completed", model: str = "gpt-6-sol") -> dict:
    return {
        "status": status,
        "model": model,
        "id": "test",
        "service_tier": "default",
        "usage": {
            "input_tokens": 10,
            "output_tokens": 10,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
        },
        "output": [{"content": [{"type": "output_text", "text": '{"ok":true}'}]}],
    }


@pytest.mark.parametrize("operation", ["insert", "delete", "replace"])
@pytest.mark.parametrize("fraction", [0.25, 1.0])
def test_coalesced_delta_equals_independent_atomic_sequence(operation, fraction):
    before, events = structured_fixture(8, fraction, operation)
    after = stage(before, events)
    batch = SignedDeltaTreatment(before)
    delta_bundle(batch, events, before, after)
    atomic = SignedDeltaTreatment(before)
    repo = deepcopy(before)
    for event in events:
        old = deepcopy(repo)
        _mutate(repo, event, repo.advance_epoch())
        atomic.process(event, old, repo)
        claims, answers = compute_all_states(repo)
        assert atomic.claim_states == claims
        assert atomic.answer_states == answers
    assert batch.claim_states == atomic.claim_states
    assert batch.answer_states == atomic.answer_states
    assert compute_all_states(before) != compute_all_states(after)


def test_all_three_exact_baselines_and_operations():
    result = structured(
        {
            "structured_sizes": [6],
            "affected_fractions": [0.5],
            "structured_repetitions": 2,
        }
    )
    assert result["trials"] == 6
    assert result["full_state_comparisons"] == 18
    assert result["mismatches"] == 0
    assert {r["operation"] for r in result["rows"]} == {"insert", "replace", "delete"}


def test_independent_new_key_bundle_rejects_duplicate_keys():
    repo, events = structured_fixture(4, 0.25, "insert")
    with pytest.raises(ValueError, match="independent"):
        stage(repo, events + events[1:])


def test_identity_selection_falls_back_without_oracle():
    entities = ["A", "B", "C"]
    assert selected_claims("B", entities) == ["B"]
    assert selected_claims(None, entities) == entities
    assert selected_claims("unseen", entities) == entities
    assert selected_claims("B", entities, {"A"}) == ["A", "B"]


def test_cache_identity_changes_with_actual_revision_not_other_entity():
    a = Source("A", "X", "s", "v0")
    b = Source("A", "Y", "s", "v1")
    assert cache_key("A", [a]) != cache_key("A", [b])
    assert cache_key("A", [a]) != cache_key("B", [a])
    assert cache_key("A", [a]) == cache_key("A", [a])


def test_verifier_batches_all_ids_and_does_not_receive_gold():
    class Inspect(FakeProvider):
        def call(self, scope, task, payload, schema):
            assert "gold" not in json.dumps(payload)
            assert len(payload["pairs"]) <= 8
            return super().call(scope, task, payload, schema)

    claims = {
        f"E{i}": f"E{i}'s current primary office is in City{i}." for i in range(12)
    }
    provider = Inspect()
    labels, info = verify(provider, "test", claims, [Source("E0", "New", "s", "v")])
    assert len(labels) == 12 and info == {"pairs": 12, "batches": 2}
    assert labels["E0", "chunk:v"] == "refute"
    assert labels["E1", "chunk:v"] == "neutral"


def test_duplicate_or_missing_verifier_rows_are_failures_not_neutral():
    class Bad(FakeProvider):
        def call(self, scope, task, payload, schema):
            return {"judgments": []}

    with pytest.raises(ProviderFailure, match="incomplete"):
        verify(
            Bad(),
            "test",
            {"A": "A's current primary office is in X."},
            [Source("A", "X", "s", "v")],
        )


def test_fake_complete_workflow_covers_conflict_absence_and_cache(tmp_path):
    config = json.loads((ROOT / "configs/fyp_viability_v1.json").read_bytes())
    result = live_diagnostic(config, FakeProvider(), tmp_path)
    assert result["live_model"] is False
    for gate in (
        "exact_stored_state",
        "cross_arm_complete_state",
        "fixture_status",
        "controlled_effect_selection",
        "answer_fields",
        "r1_pair_reduction_50pct",
        "r4_pair_reduction_50pct",
    ):
        assert result["gates"][gate], gate
    assert result["provider_aggregate"]["api_requests"] == 0
    for reads in (1, 4):
        summary = {r["arm"]: r for r in result["summary"] if r["reads"] == reads}
        assert summary["selective"]["verifier_pairs"] == 2
        assert summary["exhaustive"]["verifier_pairs"] == 24
        assert summary["cached_rag"]["cache_hits"] > 0
    assert result["summary"][0]["source_index_setup_ms"] >= 0
    assert len(list(tmp_path.glob("*-corpus.json"))) == 24


def test_unknown_answer_requires_empty_fields():
    assert answer_fields_correct(
        {"values": [], "source_ids": [], "disposition": "unknown"}, []
    )
    assert not answer_fields_correct(
        {"values": ["invented"], "source_ids": [], "disposition": "unknown"}, []
    )


def test_result_publication_never_overwrites(tmp_path):
    publish(tmp_path / "x.json", {"x": 1})
    with pytest.raises(FileExistsError):
        publish(tmp_path / "x.json", {"x": 2})


def test_cap_prevents_post_and_preserves_seed(tmp_path):
    budget = ledger(tmp_path, "4.999")
    with pytest.raises(ProviderFailure, match="cap"):
        budget.reserve("test", "answer", b"x")
    assert budget.stats()["api_requests"] == 0
    assert budget.stats()["aggregate_with_prior_usd"] == "4.999"


def test_concurrent_reservations_do_not_overspend(tmp_path):
    budget = ledger(tmp_path, "4.95")

    def attempt(_):
        try:
            return budget.reserve("test", "answer", b"x")[0]
        except ProviderFailure:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(8)))
    assert sum(v is not None for v in outcomes) == 1
    assert Decimal(budget.stats()["aggregate_with_prior_usd"]) <= 5


def test_ambiguous_post_has_no_retry_and_retains_full_reservation(tmp_path):
    budget = ledger(tmp_path)
    calls = []

    def fail(request):
        calls.append(request)
        raise httpx.ReadTimeout("injected", request=request)

    provider = LiveProvider(
        "fake-key",
        budget,
        tmp_path,
        lambda: None,
        httpx.Client(transport=httpx.MockTransport(fail)),
    )
    with pytest.raises(ProviderFailure, match="no retry"):
        provider.call("test", "answer", {}, {"type": "object"})
    assert len(calls) == 1
    assert budget.stats()["unresolved"] == 1
    assert Decimal(budget.stats()["conservative_accounted_usd"]) > 0
    assert budget.stats()["provider_time_ms"] is None
    assert budget.stats()["recorded_provider_elapsed_ms"] >= 0
    receipt = json.loads(next(tmp_path.glob("*-receipt.json")).read_bytes())
    assert receipt["raw_response_sha256"] is None
    assert receipt["ambiguity"]


@pytest.mark.parametrize(
    "kind", ["incomplete", "refusal", "bad_json", "model", "pro", "http"]
)
def test_provider_failures_never_become_semantic_labels(tmp_path, kind):
    budget = ledger(tmp_path)
    body = response()
    status = 200
    if kind == "incomplete":
        body["status"] = "incomplete"
    elif kind == "refusal":
        body["output"][0]["content"] = [{"type": "refusal", "refusal": "no"}]
    elif kind == "bad_json":
        body["output"][0]["content"][0]["text"] = "bad"
    elif kind == "model":
        body["model"] = "another-model"
    elif kind == "pro":
        body["model"] = "gpt-6-sol-pro"
    else:
        status = 429
    provider = LiveProvider(
        "fake-key",
        budget,
        tmp_path,
        lambda: None,
        httpx.Client(
            transport=httpx.MockTransport(lambda req: httpx.Response(status, json=body))
        ),
    )
    with pytest.raises(ProviderFailure):
        provider.call("test", "answer", {}, {"type": "object"})
    assert budget.stats()["api_requests"] == 1
    assert budget.stats()["unresolved"] == (
        1 if kind in {"http", "model", "pro"} else 0
    )
    receipts = list(tmp_path.glob("*-receipt.json"))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_bytes())
    assert receipt["raw_response_sha256"]
    assert receipt["elapsed_ms"] >= 0
    assert "fake-key" not in receipts[0].read_text()


def test_usage_bound_violation_retains_reservation(tmp_path):
    budget = ledger(tmp_path)
    request, bound = budget.reserve("test", "answer", b"x")
    body = response()
    body["usage"]["input_tokens"] = bound + 1
    with pytest.raises(ProviderFailure, match="usage"):
        budget.settle(request, body, bound)
    assert budget.stats()["unresolved"] == 1


def test_finite_request_limit_even_with_zero_usage(tmp_path):
    budget = ledger(tmp_path)
    for _ in range(200):
        request, bound = budget.reserve("test", "answer", b"x")
        body = response()
        body["usage"]["input_tokens"] = 0
        body["usage"]["output_tokens"] = 0
        body["elapsed_ms"] = 0
        budget.settle(request, body, bound)
    with pytest.raises(ProviderFailure, match="limit"):
        budget.reserve("test", "answer", b"x")


def test_caching_rates_not_double_counted_or_hidden():
    usage = {
        "input_tokens": 12000,
        "output_tokens": 200,
        "input_tokens_details": {
            "cached_tokens": 4000,
            "cache_write_tokens": 7000,
            "cache_write_tokens_details": {
                "ttl_30m_tokens": 3000,
                "ttl_12h_tokens": 4000,
            },
        },
    }
    assert Decimal(docs_cost(usage)) == Decimal("0.0323")
    del usage["input_tokens_details"]["cache_write_tokens_details"]
    assert docs_cost(usage) is None


def test_no_completed_text_is_not_a_success():
    with pytest.raises(ProviderFailure):
        parse_response({"status": "completed", "output": []})


def test_usefulness_requires_quality_and_not_just_speed():
    gates = {
        k: True
        for k in (
            "exact_stored_state",
            "cross_arm_complete_state",
            "fixture_status",
            "controlled_effect_selection",
            "answer_fields",
            "pair",
        )
    }
    assert qualified_useful(gates, "pair", True, True)
    for key in gates:
        bad = dict(gates)
        bad[key] = False
        assert not qualified_useful(bad, "pair", True, True), key
    assert not qualified_useful(gates, "pair", False, True)
    assert not qualified_useful(gates, "pair", True, False)


def test_billing_tier_mismatch_does_not_release_reservation(tmp_path):
    budget = ledger(tmp_path)
    body = response()
    body["service_tier"] = "priority"
    provider = LiveProvider(
        "fake-key",
        budget,
        tmp_path,
        lambda: None,
        httpx.Client(
            transport=httpx.MockTransport(lambda req: httpx.Response(200, json=body))
        ),
    )
    with pytest.raises(ProviderFailure, match="tier"):
        provider.call("test", "answer", {}, {"type": "object"})
    assert budget.stats()["unresolved"] == 1
    assert budget.stats()["docs_rate_estimate_usd"] is None


def test_bad_http_json_preserves_hash_not_unsafe_error_body(tmp_path):
    budget = ledger(tmp_path)
    provider = LiveProvider(
        "fake-key",
        budget,
        tmp_path,
        lambda: None,
        httpx.Client(
            transport=httpx.MockTransport(
                lambda req: httpx.Response(200, text="fake-key not JSON")
            )
        ),
    )
    with pytest.raises(ProviderFailure):
        provider.call("test", "answer", {}, {"type": "object"})
    receipt = next(tmp_path.glob("*-receipt.json")).read_text()
    assert "raw_response_sha256" in receipt
    assert "fake-key" not in receipt
    assert budget.stats()["unresolved"] == 1


def test_model_snapshot_drift_stops_and_retains_reservation(tmp_path):
    budget = ledger(tmp_path)
    bodies = iter(
        [response(model="gpt-6-sol-2026-01-01"), response(model="gpt-6-sol-2026-01-02")]
    )
    provider = LiveProvider(
        "fake-key",
        budget,
        tmp_path,
        lambda: None,
        httpx.Client(
            transport=httpx.MockTransport(
                lambda req: httpx.Response(200, json=next(bodies))
            )
        ),
    )
    provider.call("test", "answer", {}, {"type": "object"})
    with pytest.raises(ProviderFailure, match="snapshot"):
        provider.call("test", "answer", {}, {"type": "object"})
    assert budget.stats()["api_requests"] == 2
    assert budget.stats()["unresolved"] == 1


def test_queued_paid_jobs_stop_after_failure(tmp_path):
    budget = ledger(tmp_path)
    provider = LiveProvider(
        "fake-key",
        budget,
        tmp_path,
        lambda: None,
        httpx.Client(
            transport=httpx.MockTransport(
                lambda req: httpx.Response(500, text="failed")
            )
        ),
    )
    with pytest.raises(ProviderFailure):
        parallel(
            provider,
            lambda i: provider.call("test", "answer", {"i": i}, {"type": "object"}),
            range(12),
        )
    assert 1 <= budget.stats()["api_requests"] <= 4
    previous = budget.stats()["api_requests"]
    with pytest.raises(ProviderFailure, match="stopped"):
        provider.call("test", "answer", {}, {"type": "object"})
    assert budget.stats()["api_requests"] == previous


def test_existing_ledger_cannot_be_reset_with_new_authority(tmp_path):
    budget = ledger(tmp_path)
    budget.reserve("test", "answer", b"x")
    reopened = ledger(tmp_path)
    assert reopened.stats()["unresolved"] == 1
    with pytest.raises(ProviderFailure, match="authority"):
        ledger(tmp_path, "0.1")
    assert reopened.stats()["unresolved"] == 1


def test_stop_is_admission_gate_not_physical_cancellation(tmp_path):
    """Already reserved calls may still POST; queued admissions must not."""
    budget = ledger(tmp_path)
    admitted, release = threading.Event(), threading.Event()
    real = httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=response()))
    )

    class DelayedHandoff:
        @property
        def post(self):
            # Attribute lookup happens AFTER admission lock release but BEFORE
            # client.post is invoked: reproduce the precise scheduling window.
            admitted.set()
            assert release.wait(5)
            return real.post

    provider = LiveProvider(
        "fake-key", budget, tmp_path, lambda: None, DelayedHandoff()
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(provider.call, "test", "answer", {}, {"type": "object"})
        assert admitted.wait(5)
        provider.stop()
        release.set()
        assert future.result(timeout=5) == {"ok": True}
    assert budget.stats()["api_requests"] == 1
    assert budget.stats()["unresolved"] == 0
    with pytest.raises(ProviderFailure, match="stopped"):
        provider.call("test", "answer", {}, {"type": "object"})
    assert budget.stats()["api_requests"] == 1
    real.close()
