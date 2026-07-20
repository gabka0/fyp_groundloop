#!/usr/bin/env python3
"""Run the sealed M4.13 terminal evaluation after development selection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .scoring import PinnedMiniLMTerminalScorer
from .terminal import (
    TerminalArtifactPaths,
    run_terminal_from_artifacts,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--candidate-artifact-root", type=Path, required=True)
    parser.add_argument("--m4-12-artifact-root", type=Path, required=True)
    parser.add_argument("--corrected-m4-10-root", type=Path, required=True)
    parser.add_argument("--v0-checkpoint", type=Path, required=True)
    parser.add_argument("--v0-calibration", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument(
        "--final-test-manifest-sha256",
        required=True,
        help="Must equal the pre-frozen 3dcfc... terminal reserve manifest.",
    )
    parser.add_argument("--bootstrap-seed", type=int, default=20260720)
    parser.add_argument("--bootstrap-resamples", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    arguments = _parse_args()
    result = run_terminal_from_artifacts(
        paths=TerminalArtifactPaths(
            selection=arguments.selection,
            data_root=arguments.data_root,
            candidate_artifact_root=arguments.candidate_artifact_root,
            m4_12_artifact_root=arguments.m4_12_artifact_root,
            corrected_m4_10_root=arguments.corrected_m4_10_root,
            v0_checkpoint=arguments.v0_checkpoint,
            v0_calibration=arguments.v0_calibration,
            final_test_manifest_sha256=arguments.final_test_manifest_sha256,
        ),
        scorer=PinnedMiniLMTerminalScorer(),
        output_directory=arguments.output_directory,
        bootstrap_seed=arguments.bootstrap_seed,
        bootstrap_resamples=arguments.bootstrap_resamples,
    )
    print(
        json.dumps(
            {"disposition": result.disposition, **dict(result.result_manifest)},
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
