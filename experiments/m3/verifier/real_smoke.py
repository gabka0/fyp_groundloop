#!/usr/bin/env python3
"""Explicit real-checkpoint smoke; ordinary pytest never invokes this."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from groundloop.ai.contracts import AtomicClaim, ChunkDraft, stable_digest
from groundloop.ai.verification.adapter import PinnedMiniLMVerifier
from groundloop.ai.verification.artifacts import tree_digest
from groundloop.ai.verification.calibration import TemperatureCalibration
from groundloop.ai.verification.runtime import load_examples
from groundloop.domain import normalized_text_hash


def main(artifact_root: Path, fixture: Path) -> None:
    checkpoint = artifact_root / "checkpoints" / "minilm2-m3-bounded-v1"
    if not checkpoint.is_dir():
        raise FileNotFoundError(
            f"fine-tuned checkpoint is absent: {checkpoint}; run train.py first"
        )
    calibration = TemperatureCalibration.read_json(
        artifact_root / "reports" / "temperature_calibration.json"
    )
    example = load_examples(fixture)[0]
    chunk_id = stable_digest("real-smoke-chunk-v1", example.example_id)
    chunk = ChunkDraft(
        chunk_version_id=chunk_id,
        document_version_id=stable_digest("real-smoke-document-v1"),
        chunk_index=0,
        text=example.evidence,
        text_hash=normalized_text_hash(example.evidence),
        chunker_artifact_id="software-docs-transfer-v1",
    )
    claim = AtomicClaim(example.claim_group_id, example.claim, True, (chunk_id,))
    verifier = PinnedMiniLMVerifier(
        model_path=str(checkpoint),
        model_revision="groundloop-m3-bounded-v1",
        temperature=calibration.temperature,
        local_files_only=True,
        artifact_sha256=tree_digest(checkpoint),
        logical_model_id="groundloop/minilm2-m3-bounded-v1",
        calibration_version=calibration.calibration_version,
    )
    result = verifier.verify(claim, chunk)
    print(
        json.dumps(
            {
                "checkpoint_sha256": tree_digest(checkpoint),
                "temperature": calibration.temperature,
                "calibration_version": calibration.calibration_version,
                "input_hash": result.input_hash,
                "raw_output_hash": result.raw_output_hash,
                "raw_logits_base_order": result.raw_logits,
                "scores_stored_order": {
                    "support": result.scores.support,
                    "refute": result.scores.refute,
                    "neutral": result.scores.neutral,
                },
                "normalized": sum(
                    (
                        result.scores.support,
                        result.scores.refute,
                        result.scores.neutral,
                    )
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path("experiments/m3/verifier/fixtures/software_docs_transfer.jsonl"),
    )
    arguments = parser.parse_args()
    main(arguments.artifact_root, arguments.fixture)
