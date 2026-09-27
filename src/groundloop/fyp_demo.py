"""Submission-oriented presentation of the accepted M4.8 dynamic history.

The module intentionally adds no new maintenance semantics.  It validates and
summarizes the canonical M4.8 evidence manifest so a weakened or different
history cannot be presented as the frozen GroundLoop FYP demo.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError
from groundloop.m4.real_dynamic_history import (
    M4RealDynamicHistoryConfig,
    M4RealDynamicHistoryResult,
    run_m4_real_dynamic_history,
    write_dynamic_history_manifest,
)

_SCHEMA_VERSION = "groundloop-m4-real-dynamic-history-v1"
_SCOPE = "bounded-real-dynamic-integration-not-model-quality"
_EXPECTED_EVENT_IDS = (
    "m4-real-history-insert",
    "m4-real-history-delete",
    "m4-real-history-replace",
)
_EXPECTED_UPDATE_KINDS = ("insert", "delete", "replace")
_EXPECTED_STATES = (
    ("conflicted", "conflicted"),
    ("refuted", "contradicted"),
    ("refuted", "contradicted"),
)
_EXPECTED_MODEL_WORK = (
    (1, 1, 1),
    (1, 0, 0),
    (1, 1, 1),
)
_EXPECTED_CLAIM_COUNTS = ((1, 1), (0, 1), (0, 2))
_EXPECTED_ANSWER_COUNTS = (
    (1, 0, 0, 0, 1),
    (1, 0, 0, 1, 0),
    (1, 0, 0, 1, 0),
)
_EXPECTED_CHANNEL_HITS = (2, 0, 2)


@dataclass(frozen=True, slots=True)
class FypDemoEventSummary:
    """One validated event in the bounded registered-claim demo."""

    event_id: str
    update_kind: str
    epoch_id: int
    claim_status: str
    answer_status: str
    support_count: int
    refute_count: int
    discovery_calls: int
    embedding_requests: int
    verifier_pair_calls: int
    persisted_jobs: int
    persisted_observations: int
    projection_sha256: str


@dataclass(frozen=True, slots=True)
class FypDemoSummary:
    """Validated, presentation-safe summary of the canonical M4.8 result."""

    postgres_version: str
    pgvector_version: str
    decision_policy_version: str
    events: tuple[FypDemoEventSummary, ...]

    @property
    def total_discovery_calls(self) -> int:
        return sum(event.discovery_calls for event in self.events)

    @property
    def total_embedding_requests(self) -> int:
        return sum(event.embedding_requests for event in self.events)

    @property
    def total_verifier_pair_calls(self) -> int:
        return sum(event.verifier_pair_calls for event in self.events)


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValidationError(f"FYP demo manifest {name} must be an object")
    return cast(dict[str, object], value)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValidationError(f"FYP demo manifest {name} must be a list")
    return cast(list[object], value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"FYP demo manifest {name} must be nonempty text")
    return value


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValidationError(f"FYP demo manifest {name} must be a nonnegative integer")
    return value


def _probability(value: object, name: str) -> float:
    if (
        not isinstance(value, int | float)
        or isinstance(value, bool)
        or not isfinite(value)
        or not 0.0 <= value <= 1.0
    ):
        raise ValidationError(f"FYP demo manifest {name} must be in [0, 1]")
    return float(value)


def _require(value: object, expected: object, name: str) -> None:
    if type(value) is not type(expected) or value != expected:
        raise ValidationError(
            f"FYP demo manifest {name} must be {expected!r}, got {value!r}"
        )


def _sha256(value: object, name: str) -> str:
    text = _text(value, name)
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValidationError(f"FYP demo manifest {name} must be lowercase SHA-256")
    return text


def _observation_ids(value: object, name: str) -> tuple[str, ...]:
    raw = _list(value, name)
    result = tuple(_sha256(item, f"{name}[{index}]") for index, item in enumerate(raw))
    if len(set(result)) != len(result):
        raise ValidationError(f"FYP demo manifest {name} contains duplicates")
    return result


def _event_summary(
    value: object,
    *,
    index: int,
) -> FypDemoEventSummary:
    event = _mapping(value, f"history[{index}]")
    expected_event_id = _EXPECTED_EVENT_IDS[index]
    expected_kind = _EXPECTED_UPDATE_KINDS[index]
    expected_claim, expected_answer = _EXPECTED_STATES[index]
    expected_discovery, expected_embedding, expected_verifier = _EXPECTED_MODEL_WORK[
        index
    ]
    expected_support, expected_refute = _EXPECTED_CLAIM_COUNTS[index]
    expected_answer_counts = _EXPECTED_ANSWER_COUNTS[index]

    _require(event.get("event_id"), expected_event_id, f"history[{index}].event_id")
    _require(
        event.get("update_kind"),
        expected_kind,
        f"history[{index}].update_kind",
    )
    _require(event.get("state"), "sealed", f"history[{index}].state")
    _sha256(event.get("publication_id"), f"history[{index}].publication_id")
    _require(
        event.get("compact_registry_identity_only"),
        True,
        f"history[{index}].compact_registry_identity_only",
    )

    exact = _mapping(event.get("exact_surfaces"), f"history[{index}].exact_surfaces")
    _require(
        exact.get("python_full_recomputation_equal"),
        True,
        f"history[{index}].python_full_recomputation_equal",
    )
    _require(
        exact.get("sql_full_recomputation_equal"),
        True,
        f"history[{index}].sql_full_recomputation_equal",
    )
    _require(
        exact.get("claim_mismatch_count"),
        0,
        f"history[{index}].claim_mismatch_count",
    )
    _require(
        exact.get("answer_mismatch_count"),
        0,
        f"history[{index}].answer_mismatch_count",
    )
    _require(exact.get("open_job_count"), 0, f"history[{index}].open_job_count")
    _require(exact.get("open_scope_count"), 0, f"history[{index}].open_scope_count")
    epoch_revision = _integer(
        exact.get("epoch_revision"), f"history[{index}].epoch_revision"
    )
    if epoch_revision == 0:
        raise ValidationError(
            f"FYP demo manifest history[{index}].epoch_revision must be positive"
        )

    claim = _mapping(exact.get("claim_state"), f"history[{index}].claim_state")
    answer = _mapping(exact.get("answer_state"), f"history[{index}].answer_state")
    _require(claim.get("status"), expected_claim, f"history[{index}].claim_status")
    _require(answer.get("status"), expected_answer, f"history[{index}].answer_status")
    _require(
        claim.get("support_count"),
        expected_support,
        f"history[{index}].support_count",
    )
    _require(
        claim.get("refute_count"),
        expected_refute,
        f"history[{index}].refute_count",
    )
    supporting_ids = _observation_ids(
        claim.get("supporting_observation_ids"),
        f"history[{index}].supporting_observation_ids",
    )
    refuting_ids = _observation_ids(
        claim.get("refuting_observation_ids"),
        f"history[{index}].refuting_observation_ids",
    )
    if len(supporting_ids) != expected_support:
        raise ValidationError(
            f"FYP demo manifest history[{index}] support witness count drifted"
        )
    if len(refuting_ids) != expected_refute:
        raise ValidationError(
            f"FYP demo manifest history[{index}] refute witness count drifted"
        )
    if set(supporting_ids) & set(refuting_ids):
        raise ValidationError(
            f"FYP demo manifest history[{index}] support/refute witnesses overlap"
        )
    if expected_support:
        _probability(
            claim.get("best_support_score"),
            f"history[{index}].best_support_score",
        )
    else:
        _require(
            claim.get("best_support_score"),
            None,
            f"history[{index}].best_support_score",
        )
    _probability(
        claim.get("best_refute_score"),
        f"history[{index}].best_refute_score",
    )
    for field, expected in zip(
        (
            "required_claim_count",
            "supported_count",
            "unsupported_count",
            "refuted_count",
            "conflicted_count",
        ),
        expected_answer_counts,
        strict=True,
    ):
        _require(answer.get(field), expected, f"history[{index}].answer_state.{field}")

    model_calls = _mapping(event.get("model_calls"), f"history[{index}].model_calls")
    _require(
        model_calls.get("discovery"),
        expected_discovery,
        f"history[{index}].discovery_calls",
    )
    _require(
        model_calls.get("admission_embedding_requests"),
        expected_embedding,
        f"history[{index}].embedding_requests",
    )
    _require(
        model_calls.get("verifier_backend_pair_calls"),
        expected_verifier,
        f"history[{index}].verifier_pair_calls",
    )
    _require(
        model_calls.get("verifier_requests"),
        expected_verifier,
        f"history[{index}].verifier_requests",
    )

    provenance = _mapping(
        event.get("durable_provenance"), f"history[{index}].durable_provenance"
    )
    expected_jobs = expected_discovery + expected_verifier
    for field, expected in (
        ("jobs", expected_jobs),
        ("attempts", expected_jobs),
        ("discovery_results", expected_discovery),
        ("channel_hits", _EXPECTED_CHANNEL_HITS[index]),
        ("admitted_pairs", expected_verifier),
        ("working_observation_deltas", 1),
        ("semantic_observations", expected_verifier),
        ("verification_executions", expected_verifier),
        ("pair_judgments", expected_verifier),
    ):
        _require(
            provenance.get(field),
            expected,
            f"history[{index}].durable_provenance.{field}",
        )
    replay = _mapping(
        event.get("fresh_connection_exact_replay"),
        f"history[{index}].fresh_connection_exact_replay",
    )
    _require(replay.get("state"), "replayed", f"history[{index}].replay_state")
    _require(
        replay.get("zero_model_and_discovery_calls"),
        True,
        f"history[{index}].replay_zero_model_and_discovery_calls",
    )
    _require(
        replay.get("database_projection_unchanged"),
        True,
        f"history[{index}].replay_database_projection_unchanged",
    )
    table_count = _integer(
        replay.get("table_count"), f"history[{index}].replay_table_count"
    )
    if table_count == 0:
        raise ValidationError(
            f"FYP demo manifest history[{index}].replay_table_count must be positive"
        )

    epoch_id = _integer(event.get("epoch_id"), f"history[{index}].epoch_id")
    if epoch_id == 0:
        raise ValidationError(
            f"FYP demo manifest history[{index}].epoch_id must be positive"
        )
    return FypDemoEventSummary(
        event_id=expected_event_id,
        update_kind=expected_kind,
        epoch_id=epoch_id,
        claim_status=expected_claim,
        answer_status=expected_answer,
        support_count=expected_support,
        refute_count=expected_refute,
        discovery_calls=expected_discovery,
        embedding_requests=expected_embedding,
        verifier_pair_calls=expected_verifier,
        persisted_jobs=expected_jobs,
        persisted_observations=expected_verifier,
        projection_sha256=_sha256(
            replay.get("projection_sha256"),
            f"history[{index}].replay_projection_sha256",
        ),
    )


def summarize_fyp_demo(manifest: object) -> FypDemoSummary:
    """Validate and summarize only the exact bounded M4.8 demo contract."""
    root = _mapping(manifest, "root")
    _require(root.get("schema_version"), _SCHEMA_VERSION, "schema_version")
    _require(root.get("scope"), _SCOPE, "scope")
    _require(root.get("execution_mode"), "measured", "execution_mode")
    _require(root.get("downloads_allowed"), False, "downloads_allowed")
    for field in (
        "reconnected_for_every_event_and_replay",
        "all_events_sealed",
        "all_replays_zero_model_calls",
        "all_replays_database_unchanged",
        "all_events_equal_python_and_sql_oracles",
    ):
        _require(root.get(field), True, field)

    registry = _mapping(root.get("claim_registry"), "claim_registry")
    _require(registry.get("claim_count"), 1, "claim_registry.claim_count")
    _require(
        registry.get("prebuilt_outside_event_kernel"),
        True,
        "claim_registry.prebuilt_outside_event_kernel",
    )
    _require(
        registry.get("events_carry_identity_only"),
        True,
        "claim_registry.events_carry_identity_only",
    )
    _sha256(registry.get("snapshot_id"), "claim_registry.snapshot_id")

    for field in (
        "candidate_policy_id",
        "verifier_prompt_artifact_id",
        "calibration_version",
        "decision_policy_version",
    ):
        _text(root.get(field), field)
    for field in (
        "candidate_policy_hash",
        "embedding_model_artifact_id",
        "embedding_model_tree_sha256",
        "verifier_model_artifact_id",
        "verifier_checkpoint_tree_sha256",
        "verifier_prompt_template_sha256",
        "calibration_artifact_sha256",
        "decision_policy_hash",
        "verifier_execution_spec_hash",
    ):
        _sha256(root.get(field), field)

    history = _list(root.get("history"), "history")
    if len(history) != len(_EXPECTED_UPDATE_KINDS):
        raise ValidationError(
            "FYP demo manifest history must contain exactly insert, delete, replace"
        )
    events = tuple(
        _event_summary(event, index=index) for index, event in enumerate(history)
    )
    epoch_ids = tuple(event.epoch_id for event in events)
    if tuple(sorted(epoch_ids)) != epoch_ids or len(set(epoch_ids)) != len(epoch_ids):
        raise ValidationError("FYP demo event epochs must be strictly increasing")
    raw_epoch_ids = _list(root.get("history_epoch_ids"), "history_epoch_ids")
    manifest_epoch_ids = tuple(
        _integer(value, f"history_epoch_ids[{index}]")
        for index, value in enumerate(raw_epoch_ids)
    )
    _require(manifest_epoch_ids, epoch_ids, "history_epoch_ids")

    return FypDemoSummary(
        postgres_version=_text(root.get("postgres_version"), "postgres_version"),
        pgvector_version=_text(root.get("pgvector_version"), "pgvector_version"),
        decision_policy_version=_text(
            root.get("decision_policy_version"), "decision_policy_version"
        ),
        events=events,
    )


def format_fyp_demo_summary(summary: FypDemoSummary, *, output: Path) -> str:
    """Render a concise report without inflating the underlying evidence."""
    rows = [
        "GroundLoop FYP demo: PASS",
        "Scope: bounded registered-claim M4 dynamic slice",
        "Seeded baseline: SUPPORTED claim / VALID answer",
        "",
        (
            "UPDATE   CLAIM       ANSWER        S/R  DISC  EMBED  VERIFY  "
            "JOBS  OBS  ORACLES  REPLAY"
        ),
    ]
    for event in summary.events:
        rows.append(
            f"{event.update_kind.upper():<8} "
            f"{event.claim_status.upper():<11} "
            f"{event.answer_status.upper():<13} "
            f"{event.support_count}/{event.refute_count:<2} "
            f"{event.discovery_calls:^4}  "
            f"{event.embedding_requests:^5}  "
            f"{event.verifier_pair_calls:^6}  "
            f"{event.persisted_jobs:^4}  "
            f"{event.persisted_observations:^3}  PASS     PASS"
        )
    rows.extend(
        (
            "",
            (
                "Totals: "
                f"discovery={summary.total_discovery_calls}, "
                f"embedding={summary.total_embedding_requests}, "
                f"verifier_pairs={summary.total_verifier_pair_calls}"
            ),
            (
                "Exact result: incremental published state matched independent "
                "Python and SQL recomputation after every event."
            ),
            (
                "Replay result: every fresh-connection replay made zero discovery, "
                "embedding, verifier-request, or verifier-backend calls and left "
                "the database projection unchanged."
            ),
            (
                "Boundary: this does not establish objective truth, semantic "
                "completeness, model quality, or population-level savings."
            ),
            (
                f"PostgreSQL: {summary.postgres_version}; "
                f"pgvector: {summary.pgvector_version}"
            ),
            f"Decision policy: {summary.decision_policy_version}",
            f"Machine manifest: {output.resolve()}",
        )
    )
    return "\n".join(rows)


def execute_fyp_demo(
    config: M4RealDynamicHistoryConfig, *, output: Path
) -> tuple[M4RealDynamicHistoryResult, FypDemoSummary]:
    """Run the unchanged M4.8 route, validate it, then persist its manifest."""
    result = run_m4_real_dynamic_history(config)
    summary = summarize_fyp_demo(result.manifest)
    write_dynamic_history_manifest(result, output)
    return result, summary
