#!/usr/bin/env python3
"""Run the bounded local-only M4.5 PostgreSQL/model integration smoke."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from groundloop.m4.smoke import (
    M4RealPostgresSmokeConfig,
    SmokeUnavailableError,
    run_m4_real_postgres_smoke,
    write_smoke_manifest,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one real-model M4 insertion and a fresh-port exact replay in "
            "a disposable PostgreSQL schema. This is not a quality benchmark."
        )
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("GROUNDLOOP_DATABASE_URL", ""),
        help="PostgreSQL DSN; defaults to GROUNDLOOP_DATABASE_URL",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path(
            os.environ.get(
                "GROUNDLOOP_M3_ARTIFACT_ROOT",
                str(Path(__file__).resolve().parents[1]),
            )
        ),
    )
    parser.add_argument("--model-config", type=Path)
    parser.add_argument("--lexical-config", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep-schema", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_m4_real_postgres_smoke(
            M4RealPostgresSmokeConfig(
                database_url=args.database_url,
                repo_root=args.repo_root.resolve(),
                artifact_root=args.artifact_root.resolve(),
                model_config_path=(
                    None if args.model_config is None else args.model_config.resolve()
                ),
                lexical_config_path=(
                    None
                    if args.lexical_config is None
                    else args.lexical_config.resolve()
                ),
                keep_schema=args.keep_schema,
            )
        )
    except SmokeUnavailableError as error:
        print(f"M4.5 smoke unavailable: {error}", file=sys.stderr)
        return 2
    if args.output is not None:
        write_smoke_manifest(result, args.output)
    print(result.to_json(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
