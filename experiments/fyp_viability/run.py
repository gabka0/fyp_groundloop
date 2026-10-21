"""Run the frozen bounded experiment; outputs must be new external directories."""

from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path

from groundloop.fyp_viability import (
    FakeProvider,
    live_diagnostic,
    publish,
    retained,
    structured,
)
from groundloop.fyp_viability_provider import (
    BudgetLedger,
    LiveProvider,
    ProviderFailure,
    digest,
    legacy_snapshot,
)

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = Path("/home/kassym/Desktop/groundloop-viability-v1.GulqnrZv")
PRIMARY = Path("/home/kassym/Desktop/groundloop")
LEGACY = (
    PRIMARY
    / "artifacts/fyp_model_effect_selector/task14_context_v1/aggregate_budget.sqlite3"
)
EXECUTABLES = (
    "src/groundloop/fyp_viability.py",
    "src/groundloop/fyp_viability_provider.py",
    "experiments/fyp_viability/run.py",
    "configs/fyp_viability_v1.json",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("structured", "retained", "fake", "live"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(EVIDENCE) or output == EVIDENCE:
        parser.error("output must be a NEW directory below the external evidence root")
    accepted = EVIDENCE / "revision3/DOC_ACCEPTED.json"
    if not accepted.is_file():
        parser.error("source execution lacks accepted documentation authority")
    activation = ROOT / "docs/workstreams/fyp_viability/ACTIVATION.md"
    if (
        digest(activation.read_bytes())
        != "c7b2102dbc099a95e631d71b285532c850ae20be84a57104be96480664e77d49"
    ):
        parser.error("frozen protocol bytes changed")
    config = json.loads((ROOT / "configs/fyp_viability_v1.json").read_bytes())
    output.mkdir(parents=False, exist_ok=False)
    publish(
        output / "MANIFEST.json",
        {
            "mode": args.mode,
            "python": sys.version,
            "platform": platform.platform(),
            "executable_sha256": {
                p: digest((ROOT / p).read_bytes()) for p in EXECUTABLES
            },
            "protocol_sha256": digest(activation.read_bytes()),
            "doc_acceptance_sha256": digest(accepted.read_bytes()),
            "config": config,
            "scope": (
                "bounded experiment; not full repository/SQL/M5/deployment acceptance"
            ),
        },
    )
    provider = None
    try:
        if args.mode == "structured":
            report = structured(config)
            publish(output / "report.json", report)
        elif args.mode == "retained":
            report = retained(PRIMARY)
            publish(output / "report.json", report)
        elif args.mode == "fake":
            report = live_diagnostic(config, FakeProvider(), output)
        else:
            from dotenv import dotenv_values

            seed = legacy_snapshot(LEGACY)
            publish(output / "LEGACY_SEED.json", seed)
            key = dotenv_values(PRIMARY / ".env").get("OPENAI_API_KEY")
            if not key:
                raise ProviderFailure("existing environment has no OpenAI API key")
            ledger = BudgetLedger(EVIDENCE / "aggregate_budget.sqlite3", seed)
            responses = output / "responses"
            responses.mkdir()
            provider = LiveProvider(
                key, ledger, responses, lambda: legacy_snapshot(LEGACY)
            )
            report = live_diagnostic(config, provider, output)
        print(
            json.dumps(
                {
                    "output": str(output),
                    "mode": args.mode,
                    "mismatches": report.get("mismatches"),
                    "gates": report.get("gates"),
                    "verdict": report.get("verdict"),
                }
            )
        )
        return 0
    except Exception as error:
        # Unknown failures retain their real type and artifacts, not fake success.
        record = {
            "status": "INCOMPLETE_INCONCLUSIVE",
            "error_type": type(error).__name__,
            "provider": provider.stats() if provider is not None else None,
        }
        if isinstance(error, ProviderFailure):
            record["safe_error"] = str(error)
        publish(output / "FAILURE.json", record)
        print(json.dumps(record))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
