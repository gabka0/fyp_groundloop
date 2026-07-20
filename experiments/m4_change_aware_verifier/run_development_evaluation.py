#!/usr/bin/env python3
"""Score all frozen development variants and seal selection.json."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from .artifacts import (
    build_development_models_manifest,
    load_development_models_manifest,
)
from .development_runner import DevelopmentInputs, run_development_evaluation
from .provenance import load_development_rows
from .scoring import PinnedMiniLMTerminalScorer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--models-manifest", type=Path)
    parser.add_argument("--training-artifact-root", type=Path)
    parser.add_argument("--v0-checkpoint", type=Path)
    parser.add_argument("--output-directory", type=Path, required=True)
    arguments = parser.parse_args()
    manifest = arguments.models_manifest
    if manifest is None:
        if arguments.training_artifact_root is None or arguments.v0_checkpoint is None:
            parser.error(
                "provide --models-manifest or both --training-artifact-root "
                "and --v0-checkpoint"
            )
        temporary = tempfile.TemporaryDirectory(
            prefix="groundloop-m4-13-development-models-"
        )
        manifest = Path(temporary.name) / "development_models.json"
        build_development_models_manifest(
            manifest,
            artifact_root=arguments.training_artifact_root,
            v0_checkpoint=arguments.v0_checkpoint,
        )
    else:
        temporary = None
    if arguments.models_manifest is not None and (
        arguments.training_artifact_root is not None
        or arguments.v0_checkpoint is not None
    ):
        parser.error("--models-manifest cannot be combined with builder arguments")
    vitamin, m3, identities = load_development_rows(arguments.data_root)
    selection, report = run_development_evaluation(
        inputs=DevelopmentInputs(
            models=load_development_models_manifest(manifest),
            vitaminc_rows=vitamin,
            m3_rows=m3,
            dataset_manifest_sha256=identities["dataset_manifest_sha256"],
            vitaminc_manifest_sha256=identities["vitaminc_manifest_sha256"],
            m3_development_sha256=identities["m3_development_sha256"],
        ),
        scorer=PinnedMiniLMTerminalScorer(),
        output_directory=arguments.output_directory,
    )
    print(
        json.dumps(
            {
                "selection_sha256": selection.file_sha256,
                "selected_variant": selection.selected_variant,
                "development_report": report,
            },
            indent=2,
            sort_keys=True,
        )
    )
    if temporary is not None:
        temporary.cleanup()


if __name__ == "__main__":
    main()
