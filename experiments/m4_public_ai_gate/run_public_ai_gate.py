#!/usr/bin/env python3
"""Run the explicit local-only M4.12 public AI-quality gate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from groundloop.m4.public_ai_gate import run_public_ai_gate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--official-repository", type=Path, required=True)
    parser.add_argument("--vitaminc-archive", type=Path, required=True)
    parser.add_argument("--vitaminc-root", type=Path, required=True)
    parser.add_argument("--m3-artifact-root", type=Path, required=True)
    parser.add_argument("--embedding-cache", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m4/public_ai/vitaminc_revision_gate_v1.json"),
    )
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    result = run_public_ai_gate(
        official_repository=args.official_repository,
        vitaminc_archive=args.vitaminc_archive,
        vitaminc_root=args.vitaminc_root,
        m3_artifact_root=args.m3_artifact_root,
        embedding_cache=args.embedding_cache,
        config_path=args.config,
        output_directory=args.output_directory,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
