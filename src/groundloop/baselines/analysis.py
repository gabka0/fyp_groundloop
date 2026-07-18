"""Automated summaries and lightweight dependency-free benchmark plots."""

from __future__ import annotations

import csv
import math
import statistics
from dataclasses import dataclass
from pathlib import Path

from groundloop.baselines.models import MetricsRecord


@dataclass(frozen=True, slots=True)
class SummaryRow:
    scenario: str
    engine: str
    timing_scope: str
    semantic_equivalent: bool
    samples: int
    median_ns: float
    p95_ns: int
    p99_ns: int
    speedup_vs_global_full: float | None
    median_touched_claims: float
    median_candidates: float
    median_maintained_bytes: float
    false_invalidations: int
    stale_state_exposures: int


def _nearest_rank(values: list[int], percentile: float) -> int:
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def summarize(records: list[MetricsRecord]) -> list[SummaryRow]:
    grouped: dict[tuple[str, str, str], list[MetricsRecord]] = {}
    for record in records:
        key = (record.scenario, record.engine, record.timing_scope.value)
        grouped.setdefault(key, []).append(record)
    medians = {
        key: float(statistics.median(item.wall_time_ns for item in items))
        for key, items in grouped.items()
    }
    rows: list[SummaryRow] = []
    for key in sorted(grouped):
        scenario, engine, timing_scope = key
        items = grouped[key]
        latencies = [item.wall_time_ns for item in items]
        equivalent = items[0].semantic_equivalent
        full_median = medians[(scenario, "global_full_recompute", timing_scope)]
        speedup = full_median / medians[key] if equivalent else None
        rows.append(
            SummaryRow(
                scenario=scenario,
                engine=engine,
                timing_scope=timing_scope,
                semantic_equivalent=equivalent,
                samples=len(items),
                median_ns=medians[key],
                p95_ns=_nearest_rank(latencies, 0.95),
                p99_ns=_nearest_rank(latencies, 0.99),
                speedup_vs_global_full=speedup,
                median_touched_claims=float(
                    statistics.median(item.touched_objects.claims for item in items)
                ),
                median_candidates=float(
                    statistics.median(item.candidate_count for item in items)
                ),
                median_maintained_bytes=float(
                    statistics.median(item.maintained_bytes for item in items)
                ),
                false_invalidations=sum(
                    item.false_invalidation_count for item in items
                ),
                stale_state_exposures=sum(
                    item.stale_state_exposure_count for item in items
                ),
            )
        )
    return rows


def write_summary_csv(path: Path, rows: list[SummaryRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SummaryRow.__annotations__))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {field: getattr(row, field) for field in row.__annotations__}
            )


def write_summary_markdown(path: Path, rows: list[SummaryRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Scenario | Engine | Scope | Equivalent | Median ms | p95 ms | "
        "p99 ms | Exact speedup | False invalidations | Stale exposures |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        speedup = (
            f"{row.speedup_vs_global_full:.3f}x"
            if row.speedup_vs_global_full is not None
            else "n/a"
        )
        lines.append(
            f"| {row.scenario} | {row.engine} | {row.timing_scope} | "
            f"{str(row.semantic_equivalent).lower()} | {row.median_ns / 1e6:.6f} | "
            f"{row.p95_ns / 1e6:.6f} | {row.p99_ns / 1e6:.6f} | {speedup} | "
            f"{row.false_invalidations} | {row.stale_state_exposures} |"
        )
    path.write_text("\n".join(lines) + "\n")


def write_kernel_svg(path: Path, rows: list[SummaryRow]) -> None:
    """Write an automated median-latency bar plot for exact kernel paths."""
    selected = [
        row
        for row in rows
        if row.timing_scope == "kernel_only" and row.semantic_equivalent
    ]
    width = 960
    bar_height = 24
    gap = 12
    top = 52
    height = top + len(selected) * (bar_height + gap) + 30
    maximum = max((row.median_ns for row in selected), default=1.0)
    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="20" y="28" font-family="sans-serif" font-size="18">'
        "Exact structured kernel median latency</text>",
    ]
    for index, row in enumerate(selected):
        y = top + index * (bar_height + gap)
        bar_width = max(1.0, 520.0 * row.median_ns / maximum)
        label = f"{row.scenario} / {row.engine}"
        elements.append(
            f'<text x="20" y="{y + 17}" font-family="monospace" '
            f'font-size="12">{label}</text>'
        )
        elements.append(
            f'<rect x="390" y="{y}" width="{bar_width:.2f}" '
            f'height="{bar_height}" fill="#3b82f6"/>'
        )
        elements.append(
            f'<text x="{400 + bar_width:.2f}" y="{y + 17}" '
            f'font-family="sans-serif" font-size="12">'
            f"{row.median_ns / 1e6:.4f} ms</text>"
        )
    elements.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(elements) + "\n")
