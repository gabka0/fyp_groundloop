#!/usr/bin/env python3
"""Evaluate zero-shot and fine-tuned verifier variants on frozen fixtures."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import resource
import time
from collections.abc import Sequence
from pathlib import Path

from groundloop.ai.contracts import AtomicClaim, ChunkDraft, stable_digest
from groundloop.ai.verification.adapter import (
    AdapterDiagnostics,
    PinnedMiniLMVerifier,
    logits_to_score_triple,
)
from groundloop.ai.verification.artifacts import file_sha256, tree_digest, write_jsonl
from groundloop.ai.verification.calibration import TemperatureCalibration
from groundloop.ai.verification.constants import BASE_MODEL_ID, BASE_MODEL_REVISION
from groundloop.ai.verification.data import VerificationExample
from groundloop.ai.verification.metrics import evaluate_probabilities
from groundloop.ai.verification.runtime import load_examples
from groundloop.domain import normalized_text_hash


def _pair(example: VerificationExample) -> tuple[AtomicClaim, ChunkDraft]:
    chunk_id = stable_digest(
        "evaluation-chunk-v1", example.example_id, example.evidence
    )
    return (
        AtomicClaim(example.claim_group_id, example.claim, True, (chunk_id,)),
        ChunkDraft(
            chunk_version_id=chunk_id,
            document_version_id=stable_digest(
                "evaluation-document-v1", example.example_id
            ),
            chunk_index=0,
            text=example.evidence,
            text_hash=normalized_text_hash(example.evidence),
            chunker_artifact_id="prepared-verifier-example-v1",
        ),
    )


def _predict(
    verifier: PinnedMiniLMVerifier,
    examples: Sequence[VerificationExample],
) -> tuple[tuple[float, float, float], ...]:
    results = verifier.verify_batch(tuple(_pair(example) for example in examples))
    if any(result.raw_logits is None for result in results):
        raise RuntimeError("verifier failed to preserve auditable logits")
    return tuple(
        result.raw_logits for result in results if result.raw_logits is not None
    )


def _diagnostics(value: AdapterDiagnostics) -> dict[str, int]:
    return {
        "examples": value.examples,
        "truncated_examples": value.truncated_examples,
        "batches": value.batches,
        "max_length": value.max_length,
    }


def _variant_metrics(
    logits: Sequence[tuple[float, float, float]],
    examples: Sequence[VerificationExample],
    *,
    temperature: float,
    ece_bins: int,
    bootstrap_resamples: int,
    seed: int,
) -> dict[str, object]:
    triples = tuple(logits_to_score_triple(row, temperature) for row in logits)
    probabilities = tuple(
        (triple.support, triple.refute, triple.neutral) for triple in triples
    )
    metrics = evaluate_probabilities(
        probabilities,
        tuple(example.label for example in examples),
        ece_bins=ece_bins,
        bootstrap_resamples=bootstrap_resamples,
        seed=seed,
    )
    payload = metrics.to_json()
    payload["temperature"] = temperature
    payload["stored_probability_order"] = ["support", "refute", "neutral"]
    payload["macro_f1_semantics"] = (
        "unweighted mean over classes with nonzero ground-truth support; absent "
        "classes have null recall/F1"
    )
    return payload


def evaluate(
    *, artifact_root: Path, config_path: Path, transfer_path: Path, allow_download: bool
) -> None:
    started = time.perf_counter()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    threads = min(int(config["max_threads"]), 8)
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["MKL_NUM_THREADS"] = str(threads)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    try:
        import torch
    except ImportError as error:
        raise RuntimeError("evaluation requires PyTorch") from error
    torch.set_num_threads(threads)
    test_examples = load_examples(artifact_root / "prepared" / "test.jsonl")
    transfer_examples = load_examples(transfer_path)
    calibration = TemperatureCalibration.read_json(
        artifact_root / "reports" / "temperature_calibration.json"
    )
    checkpoint = artifact_root / "checkpoints" / "minilm2-m3-bounded-v1"
    if not checkpoint.is_dir():
        raise FileNotFoundError(
            f"fine-tuned checkpoint is absent: {checkpoint}; run train.py first"
        )
    common = {
        "max_length": int(config["max_length"]),
        "batch_size": int(config["batch_size"]),
    }
    zero_shot = PinnedMiniLMVerifier(
        temperature=1.0,
        local_files_only=not allow_download,
        **common,
    )
    fine_tuned = PinnedMiniLMVerifier(
        model_path=str(checkpoint),
        model_revision="groundloop-m3-bounded-v1",
        temperature=1.0,
        local_files_only=True,
        artifact_sha256=tree_digest(checkpoint),
        logical_model_id="groundloop/minilm2-m3-bounded-v1",
        **common,
    )
    base_test_logits = _predict(zero_shot, test_examples)
    base_test_diagnostics = _diagnostics(zero_shot.last_diagnostics)
    base_transfer_logits = _predict(zero_shot, transfer_examples)
    base_transfer_diagnostics = _diagnostics(zero_shot.last_diagnostics)
    tuned_test_logits = _predict(fine_tuned, test_examples)
    tuned_test_diagnostics = _diagnostics(fine_tuned.last_diagnostics)
    tuned_transfer_logits = _predict(fine_tuned, transfer_examples)
    tuned_transfer_diagnostics = _diagnostics(fine_tuned.last_diagnostics)

    ece_bins = int(config["ece_bins"])
    resamples = int(config["bootstrap_resamples"])
    seed = int(config["seed"])
    report: dict[str, object] = {
        "schema_version": "groundloop-verifier-evaluation-v1",
        "public_test_limitation": (
            "SciFact official test labels are unreleased and WiCE has no "
            "contradiction class; REFUTE recall/F1 on public test are not estimable."
        ),
        "transfer_role": (
            "independent authored software/API documentation transfer evaluation; "
            "not a substitute for the public test"
        ),
        "model": {
            "base_model_id": BASE_MODEL_ID,
            "base_model_revision": BASE_MODEL_REVISION,
            "checkpoint_sha256": tree_digest(checkpoint),
        },
        "calibration": {
            "calibration_version": calibration.calibration_version,
            "temperature": calibration.temperature,
            "fit_split": calibration.fit_split,
            "example_count": calibration.example_count,
            "nll_before": calibration.nll_before,
            "nll_after": calibration.nll_after,
            "method": calibration.method,
        },
        "fixtures": {
            "prepared_manifest_sha256": file_sha256(
                artifact_root / "prepared" / "manifest.json"
            ),
            "public_test_sha256": file_sha256(
                artifact_root / "prepared" / "test.jsonl"
            ),
            "transfer_sha256": file_sha256(transfer_path),
        },
        "public_test": {
            "zero_shot": _variant_metrics(
                base_test_logits,
                test_examples,
                temperature=1.0,
                ece_bins=ece_bins,
                bootstrap_resamples=resamples,
                seed=seed,
            ),
            "fine_tuned_uncalibrated": _variant_metrics(
                tuned_test_logits,
                test_examples,
                temperature=1.0,
                ece_bins=ece_bins,
                bootstrap_resamples=resamples,
                seed=seed + 10,
            ),
            "fine_tuned_calibrated": _variant_metrics(
                tuned_test_logits,
                test_examples,
                temperature=calibration.temperature,
                ece_bins=ece_bins,
                bootstrap_resamples=resamples,
                seed=seed + 20,
            ),
        },
        "software_docs_transfer": {
            "zero_shot": _variant_metrics(
                base_transfer_logits,
                transfer_examples,
                temperature=1.0,
                ece_bins=ece_bins,
                bootstrap_resamples=resamples,
                seed=seed + 30,
            ),
            "fine_tuned_uncalibrated": _variant_metrics(
                tuned_transfer_logits,
                transfer_examples,
                temperature=1.0,
                ece_bins=ece_bins,
                bootstrap_resamples=resamples,
                seed=seed + 40,
            ),
            "fine_tuned_calibrated": _variant_metrics(
                tuned_transfer_logits,
                transfer_examples,
                temperature=calibration.temperature,
                ece_bins=ece_bins,
                bootstrap_resamples=resamples,
                seed=seed + 50,
            ),
        },
        "adapter_diagnostics": {
            "zero_shot_public_test": base_test_diagnostics,
            "zero_shot_transfer": base_transfer_diagnostics,
            "fine_tuned_public_test": tuned_test_diagnostics,
            "fine_tuned_transfer": tuned_transfer_diagnostics,
        },
        "resource": {
            "wall_seconds": time.perf_counter() - started,
            "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "threads": threads,
            "platform": platform.platform(),
        },
        "dependencies": {
            package: importlib.metadata.version(package)
            for package in ("torch", "transformers", "scikit-learn")
        },
    }
    reports = artifact_root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "evaluation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    write_jsonl(
        reports / "evaluation_logits.jsonl",
        (
            {
                "fixture": fixture,
                "model": model,
                "example_id": example.example_id,
                "label": example.label.name.casefold(),
                "base_logit_order": ["contradiction", "entailment", "neutral"],
                "logits": list(logits[index]),
            }
            for fixture, model, examples, logits in (
                ("public_test", "zero_shot", test_examples, base_test_logits),
                ("public_test", "fine_tuned", test_examples, tuned_test_logits),
                (
                    "software_docs_transfer",
                    "zero_shot",
                    transfer_examples,
                    base_transfer_logits,
                ),
                (
                    "software_docs_transfer",
                    "fine_tuned",
                    transfer_examples,
                    tuned_transfer_logits,
                ),
            )
            for index, example in enumerate(examples)
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m3/verifier/bounded_cpu.json"),
    )
    parser.add_argument(
        "--transfer-fixture",
        type=Path,
        default=Path("experiments/m3/verifier/fixtures/software_docs_transfer.jsonl"),
    )
    parser.add_argument("--allow-download", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    evaluate(
        artifact_root=arguments.artifact_root,
        config_path=arguments.config,
        transfer_path=arguments.transfer_fixture,
        allow_download=arguments.allow_download,
    )
