#!/usr/bin/env python3
"""Reproduce the hash-checked M5.5 controlled/retrospective report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from groundloop.m5.evaluation.baselines import run_baselines
from groundloop.m5.evaluation.config import (
    SOURCE_VERIFICATION_STATEMENT,
    evaluation_config_identity_dict,
    load_controlled_evaluation_config,
)
from groundloop.m5.evaluation.histories import (
    build_authored_controlled_histories,
    build_wice_primary_histories,
)
from groundloop.m5.evaluation.metrics import build_metric_report
from groundloop.m5.evaluation.reporting import (
    adapter_audit_dict,
    evaluation_report_dict,
    write_json_report,
)
from groundloop.m5.evaluation.wice import adapt_wice

DEFAULT_CONFIG_PATH = Path("configs/m5/controlled_evaluation_v1.json")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the frozen, zero-model-call M5.5 WiCE and authored-control "
            "evaluation. Downloaded data stays outside Git."
        )
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        required=True,
        help="detached WiCE checkout at the frozen official commit",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="strict frozen evaluation config (source manifest is resolved from it)",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="verify and report adapter aggregates without dynamic baselines",
    )
    parser.add_argument(
        "--measure-python-comparators",
        action="store_true",
        help=(
            "measure only baselines 6/7 in their labelled pure-Python backends; "
            "the functional baseline-5 scaffold remains timing-disabled"
        ),
    )
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    config = load_controlled_evaluation_config(arguments.config)
    adapter = adapt_wice(arguments.source_root, config.source_manifest)
    config_identity = evaluation_config_identity_dict(config)
    primary_gate_identity = config_identity["primary_gate_identity"]
    if arguments.audit_only:
        report: dict[str, object] = {
            "schema": "groundloop-m5-controlled-evaluation-audit-only-v1",
            "evaluation_config": config_identity,
            "result_classification": primary_gate_identity,
            "source_verification": SOURCE_VERIFICATION_STATEMENT,
            "adapter_audit": adapter_audit_dict(adapter, config=config),
        }
    else:
        wice_histories = build_wice_primary_histories(adapter)
        wice_run = run_baselines(
            wice_histories,
            measure_latency=arguments.measure_python_comparators,
        )
        wice_metrics = build_metric_report(
            wice_run,
            seed=config.bootstrap.seed,
            resamples=config.bootstrap.resamples,
            confidence_level=config.bootstrap.confidence_level,
        )
        authored_histories = build_authored_controlled_histories()
        authored_run = run_baselines(
            authored_histories,
            measure_latency=arguments.measure_python_comparators,
        )
        authored_metrics = build_metric_report(
            authored_run,
            seed=config.bootstrap.seed,
            resamples=config.bootstrap.resamples,
            confidence_level=config.bootstrap.confidence_level,
        )
        report = {
            "schema": "groundloop-m5-controlled-evaluation-suite-v1",
            "evaluation_config": config_identity,
            "result_classification": primary_gate_identity,
            "source_verification": SOURCE_VERIFICATION_STATEMENT,
            "wice_primary_retrospective": evaluation_report_dict(
                adapter=adapter,
                histories=wice_histories,
                run=wice_run,
                metrics=wice_metrics,
                config=config,
            ),
            "authored_controlled_diagnostics": evaluation_report_dict(
                adapter=adapter,
                histories=authored_histories,
                run=authored_run,
                metrics=authored_metrics,
                config=config,
            ),
        }
    write_json_report(arguments.output, report)
    summary = {
        "output": str(arguments.output),
        "config_path": str(config.consumed_path),
        "config_schema": config.schema,
        "config_canonical_sha256": config.canonical_sha256,
        "result_classification": primary_gate_identity,
        "source_manifest_path": str(config.source_manifest_path),
        "manifest_hash": adapter.manifest.manifest_hash,
        "source_revision": adapter.manifest.official_commit,
        "source_verification": SOURCE_VERIFICATION_STATEMENT,
        "bootstrap_seed": config.bootstrap.seed,
        "bootstrap_resamples": config.bootstrap.resamples,
        "bootstrap_confidence_level": config.bootstrap.confidence_level,
        "primary_claims": len(adapter.source_claims),
        "controlled_requirement_observations": len(adapter.controlled_projections),
        "direct_claim_observations": 0,
        "model_calls": 0,
        "audit_only": arguments.audit_only,
    }
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
