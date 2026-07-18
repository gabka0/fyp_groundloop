#!/usr/bin/env python3
"""Run reproducible structured GroundLoop baselines from a JSON config."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from groundloop.baselines.analysis import (
    summarize,
    write_kernel_svg,
    write_summary_csv,
    write_summary_markdown,
)
from groundloop.baselines.models import Locality, WorkloadParameters
from groundloop.baselines.runner import run_paired_benchmark, write_jsonl
from groundloop.baselines.workload import generate_workload


def _parameters(raw: dict[str, Any]) -> WorkloadParameters:
    return WorkloadParameters(
        E=int(raw["E"]),
        C=int(raw["C"]),
        A=int(raw["A"]),
        k=int(raw["k"]),
        f=int(raw["f"]),
        duplicate_content_ratio=float(raw["duplicate_content_ratio"]),
        skew=float(raw["skew"]),
        locality=Locality(str(raw["locality"])),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", default="configs/baselines/structured_smoke.json", type=Path
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    config: dict[str, Any] = json.loads(args.config.read_text())
    run_id = str(config["run_id"])
    warmup = int(config["warmup"])
    repetitions = int(config["repetitions"])
    records = []
    for scenario in config["scenarios"]:
        workload = generate_workload(
            str(scenario["name"]),
            _parameters(scenario["parameters"]),
            int(scenario["seed"]),
        )
        records.extend(
            run_paired_benchmark(
                workload,
                run_id=run_id,
                warmup=warmup,
                repetitions=repetitions,
            )
        )

    rows = summarize(records)
    write_jsonl(args.output_dir / "raw_metrics.jsonl", records)
    write_summary_csv(args.output_dir / "summary.csv", rows)
    write_summary_markdown(args.output_dir / "summary.md", rows)
    write_kernel_svg(args.output_dir / "kernel_median.svg", rows)
    print(
        json.dumps(
            {
                "run_id": run_id,
                "records": len(records),
                "summary_rows": len(rows),
                "output_dir": str(args.output_dir),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
