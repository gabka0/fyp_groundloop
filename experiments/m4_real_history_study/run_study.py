#!/usr/bin/env python3
"""Run the local-only M4.10 Git-history pilot in one command."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from groundloop.m4.real_history_study import (
    RealHistoryStudyRunConfig,
    run_real_history_study,
)


def _database_url(argument: str | None) -> str:
    value = (
        argument
        or os.environ.get("GROUNDLOOP_TEST_DATABASE_URL")
        or os.environ.get("GROUNDLOOP_DATABASE_URL")
    )
    if not value:
        raise SystemExit(
            "PostgreSQL URL absent: pass --database-url or set GROUNDLOOP_DATABASE_URL"
        )
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description=("Run the pinned, local-only M4.10 naturally-versioned Git study")
    )
    parser.add_argument("--database-url")
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument(
        "--artifact-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument(
        "--mp-spdz-root", type=Path, default=Path("/home/kassym/mp-spdz")
    )
    parser.add_argument(
        "--bustub-root", type=Path, default=Path("/home/kassym/bustub-private")
    )
    parser.add_argument(
        "--dynagox-root", type=Path, default=Path("/home/kassym/dynagox")
    )
    parser.add_argument(
        "--definition", type=Path, help="override the frozen history definition"
    )
    parser.add_argument(
        "--model-config", type=Path, help="override the pinned M3 reuse config"
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("/tmp/groundloop-m4-10-real-history"),
    )
    parser.add_argument("--bootstrap-replicates", type=int, default=10_000)
    args = parser.parse_args()
    result = run_real_history_study(
        RealHistoryStudyRunConfig(
            repo_root=args.repo_root.resolve(),
            artifact_root=args.artifact_root.resolve(),
            database_url=_database_url(args.database_url),
            source_roots={
                "mp-spdz-readme-december-2025": args.mp_spdz_root,
                "bustub-readme-ubuntu-22-to-24": args.bustub_root,
                "dynagox-protected-oram-design-insert": args.dynagox_root,
            },
            definition_path=args.definition,
            model_config_path=args.model_config,
            output_directory=args.output_directory,
            bootstrap_replicates=args.bootstrap_replicates,
        )
    )
    print(f"structural_hash={result.structural_hash}")
    print(f"timing_hash={result.timing_hash}")
    print(f"study_manifest_hash={result.bundle.study_manifest_hash}")
    print(f"report_hash={result.bundle.report_hash}")
    print(f"bundle_manifest_hash={result.bundle.bundle_manifest_hash}")
    print(f"output_directory={args.output_directory.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
