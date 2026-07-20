"""Terminal scorer protocol and explicit local MiniLM implementation."""

from __future__ import annotations

import importlib
import math
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from groundloop.ai.verification.artifacts import tree_digest
from groundloop.errors import ValidationError

from .contracts import BASE_LOGIT_ORDER, EvaluationRow, ModelSpec, RawLogitRow


@dataclass(frozen=True, slots=True)
class ScoringResult:
    rows: tuple[RawLogitRow, ...]
    batches: int
    wall_seconds_including_model_load: float
    peak_rss_kib: int | None
    failures: int = 0
    timeouts: int = 0


class TerminalScorer(Protocol):
    def score(
        self, model: ModelSpec, rows: Sequence[EvaluationRow]
    ) -> ScoringResult: ...


def stored_probabilities(
    base_logits: Sequence[float], *, temperature: float
) -> tuple[float, float, float]:
    if len(base_logits) != 3 or temperature <= 0.0 or not math.isfinite(temperature):
        raise ValidationError("softmax requires three logits and positive temperature")
    scaled = tuple(float(value) / temperature for value in base_logits)
    if any(not math.isfinite(value) for value in scaled):
        raise ValidationError("softmax logits must be finite")
    maximum = max(scaled)
    exponentials = tuple(math.exp(value - maximum) for value in scaled)
    denominator = sum(exponentials)
    contradiction, entailment, neutral = (value / denominator for value in exponentials)
    return entailment, contradiction, neutral


def operational_label(
    probabilities: tuple[float, float, float],
    *,
    support_threshold: float = 0.8,
    refute_threshold: float = 0.8,
) -> str:
    support, refute, neutral = probabilities
    if refute >= refute_threshold and refute >= support and refute >= neutral:
        return "refute"
    if support >= support_threshold and support > refute and support > neutral:
        return "support"
    return "neutral"


class PinnedMiniLMTerminalScorer:
    """Score one frozen checkpoint locally; this class never downloads models."""

    def score(self, model: ModelSpec, rows: Sequence[EvaluationRow]) -> ScoringResult:
        if not rows:
            raise ValidationError("terminal scorer requires at least one row")
        if tree_digest(Path(model.checkpoint_path)) != model.checkpoint_tree_sha256:
            raise ValidationError("terminal checkpoint tree identity drifted")
        os.environ["OMP_NUM_THREADS"] = "8"
        os.environ["MKL_NUM_THREADS"] = "8"
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        try:
            resource = importlib.import_module("resource")
            torch = importlib.import_module("torch")
            transformers = importlib.import_module("transformers")
        except ImportError as error:
            raise RuntimeError(
                "terminal scoring requires local ML dependencies"
            ) from error
        started = time.perf_counter()
        torch.set_num_threads(8)
        if int(torch.get_num_interop_threads()) != 1:
            try:
                torch.set_num_interop_threads(1)
            except RuntimeError as error:
                raise ValidationError(
                    "cannot freeze torch inter-op threads before inference"
                ) from error
        torch.use_deterministic_algorithms(True)
        if (
            int(torch.get_num_threads()) != 8
            or int(torch.get_num_interop_threads()) != 1
            or not bool(torch.are_deterministic_algorithms_enabled())
        ):
            raise ValidationError("torch inference determinism settings drifted")
        tokenizer: Any = transformers.AutoTokenizer.from_pretrained(
            model.checkpoint_path,
            local_files_only=True,
        )
        classifier: Any = (
            transformers.AutoModelForSequenceClassification.from_pretrained(
                model.checkpoint_path,
                local_files_only=True,
            )
        )
        classifier.eval()
        if int(classifier.config.num_labels) != 3:
            raise ValidationError("terminal checkpoint must expose three labels")
        id2label = classifier.config.id2label
        observed_label_order = tuple(
            str(id2label.get(index, id2label.get(str(index), ""))).lower()
            for index in range(3)
        )
        if observed_label_order != BASE_LOGIT_ORDER:
            raise ValidationError("terminal checkpoint base-logit label order drifted")
        output: list[RawLogitRow] = []
        batches = 0
        for start in range(0, len(rows), model.batch_size):
            batch = rows[start : start + model.batch_size]
            premises = [row.evidence for row in batch]
            hypotheses = [row.claim for row in batch]
            full = tokenizer(
                premises,
                hypotheses,
                add_special_tokens=True,
                truncation=False,
            )
            truncations = tuple(
                len(token_ids) > model.max_length for token_ids in full["input_ids"]
            )
            encoded = tokenizer(
                premises,
                hypotheses,
                padding=True,
                truncation=True,
                max_length=model.max_length,
                return_tensors="pt",
            )
            with torch.inference_mode():
                logits_batch = classifier(**encoded).logits.detach().cpu().tolist()
            for source, logits_value, truncated in zip(
                batch, logits_batch, truncations, strict=True
            ):
                logits = cast(
                    tuple[float, float, float],
                    tuple(float(value) for value in logits_value),
                )
                calibrated = stored_probabilities(logits, temperature=model.temperature)
                output.append(
                    RawLogitRow(
                        fixture=source.fixture,
                        split=source.split,
                        stratum=source.stratum,
                        page_id=source.page_id,
                        case_id=source.case_id,
                        claim_group_id=source.claim_group_id,
                        transition_id=source.transition_id,
                        row_id=source.row_id,
                        claim_sha256=source.claim_sha256,
                        evidence_sha256=source.evidence_sha256,
                        mapped_label=source.label,
                        input_sha256=source.input_sha256,
                        base_logits=logits,
                        uncalibrated_probabilities=stored_probabilities(
                            logits, temperature=1.0
                        ),
                        old_m3_temperature_probabilities=stored_probabilities(
                            logits, temperature=1.1037657679769346
                        ),
                        calibrated_probabilities=calibrated,
                        operational_label=operational_label(calibrated),
                        truncated=truncated,
                        model_key=model.key,
                        model_identity=model.model_identity,
                        calibration_identity=model.calibration_identity,
                        temperature=model.temperature,
                    )
                )
            batches += 1
        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return ScoringResult(
            rows=tuple(output),
            batches=batches,
            wall_seconds_including_model_load=time.perf_counter() - started,
            peak_rss_kib=peak,
        )
