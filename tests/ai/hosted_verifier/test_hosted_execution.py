from __future__ import annotations

import json
from pathlib import Path

from groundloop.fyp_impact_pareto import (
    GoldAnnotation,
    evaluate_hosted_accuracy,
    evaluate_hosted_effect_frontier,
)
from groundloop.fyp_impact_selection import ClaimCandidate, Partition, RevisionCase
from groundloop.hosted_verifier import (
    RESULT_SCHEMA_VERSION,
    HostedVerifierResult,
    advance_batch_ledger_once,
    build_request_population,
    load_hosted_verifier_config,
    parse_openai_response,
    write_role_batch_parts,
)
from groundloop.m4.contracts import sha256_text

ROOT = Path(__file__).resolve().parents[3]


def _partition() -> Partition:
    claims = tuple(
        ClaimCandidate(sha256_text(text), text)
        for text in (
            "Amber status is old.",
            "Amber status is new.",
            "Birch status is old.",
            "Birch status is new.",
        )
    )
    cases = (
        RevisionCase(
            case_id="amber",
            selection_key="0" * 64,
            stratum="a",
            old_evidence="Amber status is old.",
            new_evidence="Amber status is new.",
            affected_claims=(claims[0], claims[1]),
        ),
        RevisionCase(
            case_id="birch",
            selection_key="1" * 64,
            stratum="b",
            old_evidence="Birch status is old.",
            new_evidence="Birch status is new.",
            affected_claims=(claims[2], claims[3]),
        ),
    )
    return Partition(
        cases=cases,
        registry=tuple(sorted(claims, key=lambda item: item.claim_sha256)),
    )


def _payload(label: str, response_id: str = "resp_test") -> dict[str, object]:
    return {
        "id": response_id,
        "model": "gpt-6-luna-2026-10-02",
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [
                    {"type": "output_text", "text": json.dumps({"label": label})}
                ],
            }
        ],
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    }


def _result(request_id: str, label: str) -> HostedVerifierResult:
    return HostedVerifierResult(
        schema_version=RESULT_SCHEMA_VERSION,
        request_id=request_id,
        provider_response_id="resp_test",
        model="gpt-6-luna-2026-10-02",
        label=label,
        input_tokens=10,
        output_tokens=5,
        total_tokens=15,
        elapsed_ms=None,
        raw_response_sha256="0" * 64,
    )


def test_strict_response_parser_and_gold_join() -> None:
    config = load_hosted_verifier_config(
        ROOT / "configs/fyp/hosted_verifier_openai_luna_v1.json"
    )
    partition = _partition()
    requests = build_request_population(config=config, partition=partition)
    first = requests[0]
    payload = _payload("neutral")
    raw = json.dumps(payload).encode()
    parsed = parse_openai_response(
        request=first,
        payload=payload,
        raw_response_bytes=raw,
        elapsed_ms=7,
    )
    assert parsed.request_id == first.request_id
    assert parsed.label == "neutral"
    assert parsed.total_tokens == 15
    annotations = (
        GoldAnnotation(
            case_id="test",
            stratum="a",
            claim_sha256=first.claim_sha256,
            evidence_sha256=first.current_evidence_sha256,
            label="neutral",
        ),
    )
    accuracy = evaluate_hosted_accuracy(
        requests=requests,
        results=(parsed,),
        annotations=annotations,
    )
    assert accuracy.evaluated_request_count == 1
    assert accuracy.correct_request_count == 1


def test_effect_frontier_uses_exhaustive_model_observations() -> None:
    config = load_hosted_verifier_config(
        ROOT / "configs/fyp/hosted_verifier_openai_luna_v1.json"
    )
    partition = _partition()
    requests = build_request_population(config=config, partition=partition)
    affected = {
        (event_index, claim.claim_sha256)
        for event_index, case in enumerate(partition.cases)
        for claim in case.affected_claims
    }
    results = tuple(
        _result(
            request.request_id,
            "support"
            if request.judged_side == "new"
            and (request.event_index, request.claim_sha256) in affected
            else "neutral",
        )
        for request in requests
    )
    points = evaluate_hosted_effect_frontier(
        partition=partition,
        requests=requests,
        results=results,
        policy_id="old_new_rarity_coverage",
        budgets=(1, 4),
    )
    assert points[0].pair_effect_denominator == 4
    assert points[1].pair_effect_numerator == 4
    assert points[1].status_effect_numerator == 4
    assert points[1].answer_effect_numerator == points[1].answer_effect_denominator
    assert points[1].avoided_verifier_pair_count == 0


class _CompletedBatchClient:
    def __init__(self, output: bytes) -> None:
        self.output = output

    def upload_batch_file(self, path: str | Path) -> dict[str, object]:
        assert Path(path).is_file()
        return {"id": "file_input"}

    def json_request(
        self, *, method: str, path: str, payload: object | None = None
    ) -> tuple[dict[str, object], bytes]:
        assert method == "POST"
        assert path == "/v1/batches"
        assert payload is not None
        value: dict[str, object] = {
            "id": "batch_test",
            "status": "completed",
            "output_file_id": "file_output",
            "error_file_id": None,
            "created_at": 1,
            "in_progress_at": 2,
            "finalizing_at": 3,
            "completed_at": 4,
        }
        return value, json.dumps(value).encode()

    def download_file(self, file_id: str) -> bytes:
        assert file_id == "file_output"
        return self.output


def test_batch_ledger_advances_only_one_job(tmp_path: Path) -> None:
    config = load_hosted_verifier_config(
        ROOT / "configs/fyp/hosted_verifier_openai_luna_v1.json"
    )
    requests = build_request_population(config=config, partition=_partition())
    ledger = write_role_batch_parts(
        requests=requests,
        request_manifest_hash="1" * 64,
        output_directory=tmp_path / "inputs",
    )
    first_line = Path(ledger.jobs[0].input_path).read_text().splitlines()[0]
    first_request_id = json.loads(first_line)["custom_id"]
    body = _payload("neutral")
    output = (
        json.dumps(
            {
                "custom_id": first_request_id,
                "response": {"status_code": 200, "body": body},
                "error": None,
            }
        )
        + "\n"
    ).encode()
    advanced = advance_batch_ledger_once(
        ledger=ledger,
        client=_CompletedBatchClient(output),  # type: ignore[arg-type]
        output_directory=tmp_path / "outputs",
    )
    assert advanced.jobs[0].provider_status == "completed"
    assert advanced.jobs[0].output_path is not None
    assert advanced.jobs[1].provider_status == "not_submitted"
