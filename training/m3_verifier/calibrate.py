#!/usr/bin/env python3
"""Fit scalar temperature on development data only."""

from __future__ import annotations

import argparse
import json
import os
import resource
import time
from pathlib import Path

from groundloop.ai.contracts import AtomicClaim, ChunkDraft, stable_digest
from groundloop.ai.verification.adapter import PinnedMiniLMVerifier
from groundloop.ai.verification.artifacts import tree_digest, write_jsonl
from groundloop.ai.verification.calibration import TemperatureCalibration
from groundloop.ai.verification.data import VerificationExample
from groundloop.ai.verification.runtime import group_bounded_sample, load_examples
from groundloop.domain import normalized_text_hash


def _pair(example: VerificationExample) -> tuple[AtomicClaim, ChunkDraft]:
    chunk_id = stable_digest(
        "calibration-chunk-v1", example.example_id, example.evidence
    )
    return (
        AtomicClaim(example.claim_group_id, example.claim, True, (chunk_id,)),
        ChunkDraft(
            chunk_version_id=chunk_id,
            document_version_id=stable_digest(
                "calibration-document-v1", example.example_id
            ),
            chunk_index=0,
            text=example.evidence,
            text_hash=normalized_text_hash(example.evidence),
            chunker_artifact_id="prepared-verifier-example-v1",
        ),
    )


def calibrate(*, artifact_root: Path, config_path: Path) -> None:
    started = time.perf_counter()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    threads = min(int(config["max_threads"]), 8)
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["MKL_NUM_THREADS"] = str(threads)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    try:
        import torch
    except ImportError as error:
        raise RuntimeError("calibration requires PyTorch") from error
    torch.set_num_threads(threads)
    checkpoint = artifact_root / "checkpoints" / "minilm2-m3-bounded-v1"
    if not checkpoint.is_dir():
        raise FileNotFoundError(
            f"fine-tuned checkpoint is absent: {checkpoint}; run train.py first"
        )
    examples = group_bounded_sample(
        load_examples(artifact_root / "prepared" / "development.jsonl"),
        maximum=int(config["development_examples_max"]),
        seed=int(config["seed"]),
    )
    verifier = PinnedMiniLMVerifier(
        model_path=str(checkpoint),
        model_revision="groundloop-m3-bounded-v1",
        temperature=1.0,
        max_length=int(config["max_length"]),
        batch_size=int(config["batch_size"]),
        local_files_only=True,
        artifact_sha256=tree_digest(checkpoint),
        logical_model_id="groundloop/minilm2-m3-bounded-v1",
    )
    results = verifier.verify_batch(tuple(_pair(example) for example in examples))
    logits = tuple(result.raw_logits for result in results)
    if any(row is None for row in logits):
        raise RuntimeError("real verifier did not preserve raw logits")
    concrete_logits = tuple(row for row in logits if row is not None)
    calibration = TemperatureCalibration.fit(
        concrete_logits,
        tuple(example.label for example in examples),
        split="development",
    )
    calibration_path = artifact_root / "reports" / "temperature_calibration.json"
    calibration.write_json(calibration_path)
    write_jsonl(
        artifact_root / "reports" / "development_logits.jsonl",
        (
            {
                "example_id": example.example_id,
                "claim_group_id": example.claim_group_id,
                "label": example.label.name.casefold(),
                "base_logit_order": ["contradiction", "entailment", "neutral"],
                "logits": list(row),
                "input_hash": result.input_hash,
                "raw_output_hash": result.raw_output_hash,
            }
            for example, row, result in zip(
                examples, concrete_logits, results, strict=True
            )
        ),
    )
    run = {
        "calibration": json.loads(calibration_path.read_text(encoding="utf-8")),
        "calibration_version": calibration.calibration_version,
        "examples": len(examples),
        "claim_groups": len({example.claim_group_id for example in examples}),
        "adapter_diagnostics": {
            "examples": verifier.last_diagnostics.examples,
            "truncated_examples": verifier.last_diagnostics.truncated_examples,
            "batches": verifier.last_diagnostics.batches,
            "max_length": verifier.last_diagnostics.max_length,
        },
        "checkpoint_sha256": tree_digest(checkpoint),
        "wall_seconds": time.perf_counter() - started,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }
    (artifact_root / "reports" / "calibration_run.json").write_text(
        json.dumps(run, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(run, indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m3/verifier/bounded_cpu.json"),
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    calibrate(artifact_root=arguments.artifact_root, config_path=arguments.config)
