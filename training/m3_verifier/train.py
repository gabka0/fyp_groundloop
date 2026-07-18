#!/usr/bin/env python3
"""Run one bounded, seeded CPU fine-tuning experiment."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import os
import platform
import random
import resource
import time
from collections import Counter
from pathlib import Path
from typing import Any

from groundloop.ai.verification.artifacts import tree_digest, tree_manifest
from groundloop.ai.verification.constants import (
    BASE_MODEL_ID,
    BASE_MODEL_REVISION,
    Label,
)
from groundloop.ai.verification.runtime import group_bounded_sample, load_examples
from groundloop.errors import ValidationError


def _base_label(label: Label) -> int:
    return {Label.SUPPORT: 1, Label.REFUTE: 0, Label.NEUTRAL: 2}[label]


def _load_config(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"training config is absent: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValidationError("training config must be a JSON object")
    if payload.get("base_model_id") != BASE_MODEL_ID:
        raise ValidationError("training config changed the frozen base model")
    if payload.get("base_model_revision") != BASE_MODEL_REVISION:
        raise ValidationError("training config changed the frozen base revision")
    return payload


def train(*, artifact_root: Path, config_path: Path, allow_download: bool) -> None:
    started = time.perf_counter()
    config = _load_config(config_path)
    threads = min(int(config["max_threads"]), 8)
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["MKL_NUM_THREADS"] = str(threads)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    seed = int(config["seed"])
    random.seed(seed)
    try:
        import torch
        from torch.utils.data import DataLoader, Dataset
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
            get_linear_schedule_with_warmup,
        )
    except ImportError as error:
        raise RuntimeError("training requires the optional ML dependencies") from error
    torch.set_num_threads(threads)
    torch.set_num_interop_threads(1)
    torch.manual_seed(seed)
    torch.use_deterministic_algorithms(True)

    train_examples = group_bounded_sample(
        load_examples(artifact_root / "prepared" / "train.jsonl"),
        maximum=int(config["train_examples_max"]),
        seed=seed,
    )
    source = BASE_MODEL_ID
    try:
        tokenizer = AutoTokenizer.from_pretrained(
            source,
            revision=BASE_MODEL_REVISION,
            local_files_only=not allow_download,
        )
        model = AutoModelForSequenceClassification.from_pretrained(
            source,
            revision=BASE_MODEL_REVISION,
            local_files_only=not allow_download,
        )
    except OSError as error:
        raise FileNotFoundError(
            "pinned base model is absent; rerun this explicit command with "
            "--allow-download"
        ) from error
    if int(model.config.num_labels) != 3:
        raise ValidationError("pinned base model does not expose three labels")

    max_length = int(config["max_length"])

    class PairDataset(Dataset[dict[str, Any]]):
        def __len__(self) -> int:
            return len(train_examples)

        def __getitem__(self, index: int) -> dict[str, Any]:
            example = train_examples[index]
            encoded = tokenizer(
                example.evidence,
                example.claim,
                truncation=True,
                max_length=max_length,
            )
            encoded["labels"] = _base_label(example.label)
            return encoded

    def collate(rows: list[dict[str, Any]]) -> dict[str, Any]:
        labels = torch.tensor(
            [int(row.pop("labels")) for row in rows], dtype=torch.long
        )
        batch = tokenizer.pad(rows, padding=True, return_tensors="pt")
        batch["labels"] = labels
        return batch

    generator = torch.Generator().manual_seed(seed)
    loader = DataLoader(
        PairDataset(),
        batch_size=int(config["batch_size"]),
        shuffle=True,
        generator=generator,
        num_workers=0,
        collate_fn=collate,
    )
    accumulation = int(config["gradient_accumulation_steps"])
    epochs = int(config["epochs"])
    optimizer_steps_per_epoch = math.ceil(len(loader) / accumulation)
    total_optimizer_steps = optimizer_steps_per_epoch * epochs
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=float(config["learning_rate"]),
        weight_decay=float(config["weight_decay"]),
    )
    scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=math.floor(
            total_optimizer_steps * float(config["warmup_ratio"])
        ),
        num_training_steps=total_optimizer_steps,
    )
    stored_counts = Counter(example.label for example in train_examples)
    base_counts = [
        stored_counts[Label.REFUTE],
        stored_counts[Label.SUPPORT],
        stored_counts[Label.NEUTRAL],
    ]
    weights = torch.tensor(
        [len(train_examples) / (3.0 * count) for count in base_counts],
        dtype=torch.float32,
    )
    loss_function = torch.nn.CrossEntropyLoss(weight=weights)
    model.train()
    optimizer.zero_grad(set_to_none=True)
    loss_sum = 0.0
    microbatches = 0
    optimizer_steps = 0
    for _epoch in range(epochs):
        for batch_index, batch in enumerate(loader):
            labels = batch.pop("labels")
            logits = model(**batch).logits
            loss = loss_function(logits, labels) / accumulation
            loss.backward()
            loss_sum += float(loss.detach()) * accumulation
            microbatches += 1
            final_batch = batch_index + 1 == len(loader)
            if (batch_index + 1) % accumulation == 0 or final_batch:
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                optimizer.step()
                scheduler.step()
                optimizer.zero_grad(set_to_none=True)
                optimizer_steps += 1

    checkpoint = artifact_root / "checkpoints" / "minilm2-m3-bounded-v1"
    checkpoint.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(checkpoint, safe_serialization=True)
    tokenizer.save_pretrained(checkpoint)
    wall_seconds = time.perf_counter() - started
    peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    manifest: dict[str, object] = {
        "schema_version": "groundloop-verifier-training-run-v1",
        "base_model_id": BASE_MODEL_ID,
        "base_model_revision": BASE_MODEL_REVISION,
        "config": config,
        "examples": len(train_examples),
        "claim_groups": len({item.claim_group_id for item in train_examples}),
        "stored_label_counts": {
            label.name.casefold(): stored_counts[label] for label in Label
        },
        "base_order_class_weights": weights.tolist(),
        "microbatches": microbatches,
        "optimizer_steps": optimizer_steps,
        "mean_training_loss": loss_sum / microbatches,
        "wall_seconds": wall_seconds,
        "peak_rss_kib": peak_rss_kib,
        "hardware": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "torch_threads": threads,
            "cuda_available": torch.cuda.is_available(),
        },
        "dependencies": {
            package: importlib.metadata.version(package)
            for package in (
                "torch",
                "transformers",
                "datasets",
                "accelerate",
                "scikit-learn",
            )
        },
        "checkpoint_sha256": tree_digest(checkpoint),
        "checkpoint_files": tree_manifest(checkpoint),
    }
    (checkpoint / "groundloop_training_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    # Recompute after adding the manifest; record the distributable tree identity
    # in a sibling file to avoid a self-referential checksum.
    final_identity = {
        "checkpoint_sha256_including_training_manifest": tree_digest(checkpoint),
        "files": tree_manifest(checkpoint),
    }
    (artifact_root / "reports" / "checkpoint_identity.json").write_text(
        json.dumps(final_identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/m3/verifier/bounded_cpu.json"),
    )
    parser.add_argument("--allow-download", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    train(
        artifact_root=arguments.artifact_root,
        config_path=arguments.config,
        allow_download=arguments.allow_download,
    )
