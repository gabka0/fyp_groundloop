"""Persisted, event-linked M4 exhaustive audit and snapshot refresh.

This module is intentionally independent of selective admission and the M4
pipeline.  Its caller freezes the event inputs explicitly.  The module then
uses only the independent oracle package to build the exhaustive additive
counterfactual and the policy-relative snapshot refresh.

The existing ``groundloop_impact_evaluation_run`` table has a generic JSONB
manifest rather than typed result columns.  We therefore bind the complete
input and result to canonical SHA-256 digests, validate the sealed M4 event at
both ends of the computation, and reject any non-exact replay.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from psycopg import Connection
from psycopg.types.json import Jsonb

from groundloop.domain import AnswerState, ClaimState
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AffectedSets,
    PairJudgment,
    PairKey,
    SnapshotRefreshResult,
    stable_m4_digest,
)
from groundloop.m4.oracles.affected import compute_affected_sets
from groundloop.m4.oracles.exhaustive_delta import (
    ExhaustiveAdditiveDelta,
    compute_exhaustive_additive_delta,
)
from groundloop.m4.oracles.full_pair import run_full_pair_audit
from groundloop.m4.oracles.grounding import recompute_grounding_states
from groundloop.m4.oracles.identity import judgment_digest
from groundloop.m4.oracles.refresh import (
    RefreshChunk,
    RefreshClaim,
    run_snapshot_refresh,
)
from groundloop.m4.oracles.testing import PairJudge

_SCHEMA_VERSION = "m4-event-audit-v1"
_HEX = frozenset("0123456789abcdef")


class EventAuditConflictError(RuntimeError):
    """A logical evaluation run was replayed with different frozen content."""


class EventAuditPersistenceError(RuntimeError):
    """A stored event-audit row is malformed or inconsistent with its event."""


class EventAuditDisposition(StrEnum):
    CREATED = "created"
    REPLAYED = "replayed"


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _float(value: float | None) -> str | None:
    return None if value is None else float(value).hex()


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _json_digest(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _pair_payload(pair: PairKey) -> dict[str, str]:
    return {
        "claim_id": pair.claim_id,
        "chunk_version_id": pair.chunk_version_id,
    }


def _judgment_payload(judgment: PairJudgment) -> dict[str, object]:
    return {
        "pair": _pair_payload(judgment.pair),
        "source_kind": judgment.source_kind.value,
        "source_artifact_id": judgment.source_artifact_id,
        "decision_policy_or_guideline_id": (
            judgment.decision_policy_or_guideline_id
        ),
        "derived_label": judgment.derived_label.value,
        "input_hash": judgment.input_hash,
        "split_id": judgment.split_id,
        "support_score_hex": _float(judgment.support_score),
        "refute_score_hex": _float(judgment.refute_score),
        "neutral_score_hex": _float(judgment.neutral_score),
        "judgment_digest": judgment_digest(judgment),
    }


def _claim_state_payload(state: ClaimState) -> dict[str, object]:
    return {
        "claim_id": state.claim_id,
        "support_count": state.support_count,
        "refute_count": state.refute_count,
        "best_support_score_hex": _float(state.best_support_score),
        "best_refute_score_hex": _float(state.best_refute_score),
        "supporting_observation_ids": list(state.supporting_observation_ids),
        "refuting_observation_ids": list(state.refuting_observation_ids),
        "status": state.status.value,
    }


def _answer_state_payload(state: AnswerState) -> dict[str, object]:
    return {
        "answer_version_id": state.answer_version_id,
        "required_claim_count": state.required_claim_count,
        "supported_count": state.supported_count,
        "unsupported_count": state.unsupported_count,
        "refuted_count": state.refuted_count,
        "conflicted_count": state.conflicted_count,
        "status": state.status.value,
    }


def _affected_payload(affected: AffectedSets) -> dict[str, object]:
    return {
        "baseline_id": affected.baseline_id,
        "materialized_state_claim_ids": list(
            affected.materialized_state_claim_ids
        ),
        "decision_summary_claim_ids": list(
            affected.decision_summary_claim_ids
        ),
        "status_claim_ids": list(affected.status_claim_ids),
        "answer_status_ids": list(affected.answer_status_ids),
    }


def _claim_payload(claim: RefreshClaim) -> dict[str, object]:
    return {
        "claim_id": claim.claim_id,
        "answer_version_id": claim.answer_version_id,
        "required": claim.required,
        "vector_hex": [float(value).hex() for value in claim.vector],
    }


def _chunk_payload(chunk: RefreshChunk) -> dict[str, object]:
    return {
        "chunk_version_id": chunk.chunk_version_id,
        "text_hash": chunk.text_hash,
        "vector_hex": [float(value).hex() for value in chunk.vector],
    }


def _sorted_pair_payload(pairs: tuple[PairKey, ...]) -> list[dict[str, str]]:
    return [_pair_payload(pair) for pair in pairs]


def _canonical_states(
    claim_states: tuple[ClaimState, ...],
    answer_states: tuple[AnswerState, ...],
) -> None:
    claim_ids = tuple(state.claim_id for state in claim_states)
    answer_ids = tuple(state.answer_version_id for state in answer_states)
    if claim_ids != tuple(sorted(set(claim_ids))):
        raise ValidationError("claim states must be sorted and unique")
    if answer_ids != tuple(sorted(set(answer_ids))):
        raise ValidationError("answer states must be sorted and unique")


def _canonical_judgments(
    name: str, judgments: tuple[PairJudgment, ...]
) -> None:
    pairs = tuple(judgment.pair for judgment in judgments)
    if pairs != tuple(sorted(set(pairs))):
        raise ValidationError(f"{name} must be sorted and pair-unique")


@dataclass(frozen=True, slots=True)
class EventAuditSpec:
    """All immutable inputs required to audit one already-sealed M4 event."""

    event_id: str
    treatment_manifest_id: str
    split_id: str
    audit_judge_identity: str
    refresh_judge_identity: str
    corpus_snapshot_hash: str
    refresh_policy_id: str
    refresh_depth_k: int
    claims: tuple[RefreshClaim, ...]
    active_chunks_after: tuple[RefreshChunk, ...]
    inserted_active_chunk_ids: tuple[str, ...]
    surviving_judgments: tuple[PairJudgment, ...]
    working_claim_states: tuple[ClaimState, ...]
    working_answer_states: tuple[AnswerState, ...]
    selective_admitted_pairs: tuple[PairKey, ...]
    selective_judgments: tuple[PairJudgment, ...]
    selective_claim_states: tuple[ClaimState, ...]
    selective_answer_states: tuple[AnswerState, ...]
    deliberate_miss_pairs: tuple[PairKey, ...] = ()

    def __post_init__(self) -> None:
        for name, value in (
            ("event_id", self.event_id),
            ("treatment_manifest_id", self.treatment_manifest_id),
            ("split_id", self.split_id),
            ("audit_judge_identity", self.audit_judge_identity),
            ("refresh_judge_identity", self.refresh_judge_identity),
            ("refresh_policy_id", self.refresh_policy_id),
        ):
            _require_text(name, value)
        _require_sha256("corpus_snapshot_hash", self.corpus_snapshot_hash)
        if self.refresh_depth_k <= 0:
            raise ValidationError("refresh_depth_k must be positive")

        claim_ids = tuple(claim.claim_id for claim in self.claims)
        chunk_ids = tuple(
            chunk.chunk_version_id for chunk in self.active_chunks_after
        )
        if claim_ids != tuple(sorted(set(claim_ids))) or not claim_ids:
            raise ValidationError("claims must be non-empty, sorted, and unique")
        if chunk_ids != tuple(sorted(set(chunk_ids))):
            raise ValidationError(
                "active_chunks_after must be sorted and unique"
            )
        if self.inserted_active_chunk_ids != tuple(
            sorted(set(self.inserted_active_chunk_ids))
        ):
            raise ValidationError(
                "inserted_active_chunk_ids must be sorted and unique"
            )
        if not set(self.inserted_active_chunk_ids) <= set(chunk_ids):
            raise ValidationError("inserted chunks must be active after the event")

        _canonical_judgments("surviving_judgments", self.surviving_judgments)
        _canonical_judgments("selective_judgments", self.selective_judgments)
        _canonical_states(self.working_claim_states, self.working_answer_states)
        _canonical_states(self.selective_claim_states, self.selective_answer_states)

        admitted = self.selective_admitted_pairs
        if admitted != tuple(sorted(set(admitted))):
            raise ValidationError(
                "selective_admitted_pairs must be sorted and unique"
            )
        judgment_pairs = tuple(judgment.pair for judgment in self.selective_judgments)
        if judgment_pairs != admitted:
            raise ValidationError(
                "selective judgments must cover admitted pairs exactly"
            )
        inserted_domain = {
            PairKey(claim_id, chunk_id)
            for claim_id in claim_ids
            for chunk_id in self.inserted_active_chunk_ids
        }
        if not set(admitted) <= inserted_domain:
            raise ValidationError("selective pairs lie outside the inserted domain")
        if self.deliberate_miss_pairs != tuple(
            sorted(set(self.deliberate_miss_pairs))
        ):
            raise ValidationError("deliberate_miss_pairs must be sorted and unique")
        if not set(self.deliberate_miss_pairs) <= inserted_domain:
            raise ValidationError("deliberate miss pair lies outside inserted domain")

        all_judgments = (*self.surviving_judgments, *self.selective_judgments)
        if any(judgment.split_id != self.split_id for judgment in all_judgments):
            raise ValidationError("all frozen judgments must use the audit split")


@dataclass(frozen=True, slots=True)
class PersistedEventAudit:
    evaluation_run_id: str
    event_id: str
    epoch_id: int
    disposition: EventAuditDisposition
    treatment_manifest_id: str
    baseline_manifest_id: str
    audit_manifest_id: str
    refresh_manifest_id: str
    input_hash: str
    result_hash: str
    expected_pair_count: int
    positive_pair_count: int
    missed_positive_pairs: tuple[PairKey, ...]
    detected_deliberate_miss_pairs: tuple[PairKey, ...]
    selective_exhaustive_status_claim_ids: tuple[str, ...]
    selective_exhaustive_answer_status_ids: tuple[str, ...]
    selective_refresh_status_claim_ids: tuple[str, ...]
    selective_refresh_answer_status_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _SealedEvent:
    epoch_id: int
    event_id: str
    payload_hash: str
    revision: int
    candidate_policy_id: str
    registry_snapshot_id: str


def _validate_spec_semantics(spec: EventAuditSpec) -> None:
    claim_to_answer = {
        claim.claim_id: claim.answer_version_id for claim in spec.claims
    }
    required = frozenset(claim.claim_id for claim in spec.claims if claim.required)
    chunk_hashes = {
        chunk.chunk_version_id: chunk.text_hash
        for chunk in spec.active_chunks_after
    }
    working = recompute_grounding_states(
        claim_to_answer=claim_to_answer,
        required_claim_ids=required,
        chunk_text_hashes=chunk_hashes,
        judgments=spec.surviving_judgments,
    )
    if working != (spec.working_claim_states, spec.working_answer_states):
        raise ValidationError(
            "working states do not equal independent recomputation of Bw"
        )
    if set(judgment.pair for judgment in spec.surviving_judgments) & set(
        spec.selective_admitted_pairs
    ):
        raise ValidationError("surviving and selective judgments overlap")
    selective = recompute_grounding_states(
        claim_to_answer=claim_to_answer,
        required_claim_ids=required,
        chunk_text_hashes=chunk_hashes,
        judgments=(*spec.surviving_judgments, *spec.selective_judgments),
    )
    if selective != (spec.selective_claim_states, spec.selective_answer_states):
        raise ValidationError(
            "selective states do not equal independent recomputation of Bs"
        )


def _config_payload(spec: EventAuditSpec) -> dict[str, object]:
    return {
        "protocol_version": _SCHEMA_VERSION,
        "split_id": spec.split_id,
        "audit_judge_identity": spec.audit_judge_identity,
        "refresh_judge_identity": spec.refresh_judge_identity,
        "refresh_policy_id": spec.refresh_policy_id,
        "refresh_depth_k": spec.refresh_depth_k,
    }


def _input_payload(spec: EventAuditSpec) -> dict[str, object]:
    return {
        "event_id": spec.event_id,
        "treatment_manifest_id": spec.treatment_manifest_id,
        "corpus_snapshot_hash": spec.corpus_snapshot_hash,
        "config": _config_payload(spec),
        "claims": [_claim_payload(claim) for claim in spec.claims],
        "active_chunks_after": [
            _chunk_payload(chunk) for chunk in spec.active_chunks_after
        ],
        "inserted_active_chunk_ids": list(spec.inserted_active_chunk_ids),
        "surviving_judgments": [
            _judgment_payload(judgment)
            for judgment in spec.surviving_judgments
        ],
        "working_claim_states": [
            _claim_state_payload(state) for state in spec.working_claim_states
        ],
        "working_answer_states": [
            _answer_state_payload(state) for state in spec.working_answer_states
        ],
        "selective_admitted_pairs": _sorted_pair_payload(
            spec.selective_admitted_pairs
        ),
        "selective_judgments": [
            _judgment_payload(judgment) for judgment in spec.selective_judgments
        ],
        "selective_claim_states": [
            _claim_state_payload(state) for state in spec.selective_claim_states
        ],
        "selective_answer_states": [
            _answer_state_payload(state) for state in spec.selective_answer_states
        ],
        "deliberate_miss_pairs": _sorted_pair_payload(
            spec.deliberate_miss_pairs
        ),
    }


def _config_hash(spec: EventAuditSpec) -> str:
    return _json_digest(_config_payload(spec))


def _evaluation_run_id(spec: EventAuditSpec) -> str:
    return "m4-event-audit-" + stable_m4_digest(
        _SCHEMA_VERSION,
        spec.event_id,
        spec.treatment_manifest_id,
        spec.split_id,
        _config_hash(spec),
    )


def _load_sealed_event(
    connection: Connection[Any], event_id: str
) -> _SealedEvent:
    row = connection.execute(
        """
        SELECT e.epoch_id, e.event_id, e.payload_hash, e.revision,
               e.structural_status, e.semantic_status, e.evaluation_state,
               e.sealed_at, u.candidate_policy_id, u.registry_snapshot_id
        FROM groundloop_epoch AS e
        JOIN groundloop_m4_update AS u ON u.epoch_id = e.epoch_id
        WHERE e.event_id = %s
        """,
        (event_id,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"event {event_id} is not a persisted M4 event")
    if row[4:7] != ("committed", "sealed", "complete") or row[7] is None:
        raise ValidationError(f"M4 event {event_id} is not completely sealed")
    return _SealedEvent(
        epoch_id=int(row[0]),
        event_id=str(row[1]),
        payload_hash=str(row[2]).strip(),
        revision=int(row[3]),
        candidate_policy_id=str(row[8]),
        registry_snapshot_id=str(row[9]),
    )


def _state_projection(state: ClaimState) -> tuple[object, ...]:
    return (
        state.claim_id,
        state.support_count,
        state.refute_count,
        state.best_support_score,
        state.best_refute_score,
        state.status.value,
    )


def _answer_projection(state: AnswerState) -> tuple[object, ...]:
    return (
        state.answer_version_id,
        state.required_claim_count,
        state.supported_count,
        state.unsupported_count,
        state.refuted_count,
        state.conflicted_count,
        state.status.value,
    )


def _validate_event_inputs(
    connection: Connection[Any], *, spec: EventAuditSpec, event: _SealedEvent
) -> None:
    registry_rows = connection.execute(
        """
        SELECT claim.claim_id, claim.answer_version_id, claim.required
        FROM groundloop_m4_claim_registry_member AS member
        JOIN groundloop_claim AS claim USING (claim_id)
        WHERE member.claim_registry_snapshot_id = %s
        ORDER BY claim.claim_id
        """,
        (event.registry_snapshot_id,),
    ).fetchall()
    expected_registry = tuple(
        (claim.claim_id, claim.answer_version_id, claim.required)
        for claim in spec.claims
    )
    if tuple(registry_rows) != expected_registry:
        raise ValidationError(
            "audit claims differ from the event claim-registry snapshot"
        )

    chunk_rows = connection.execute(
        """
        SELECT chunk_version_id, text_hash, valid_from_epoch
        FROM groundloop_m4_effective_chunk_version
        WHERE epoch_id = %s
        ORDER BY chunk_version_id
        """,
        (event.epoch_id,),
    ).fetchall()
    expected_chunks = tuple(
        (chunk.chunk_version_id, chunk.text_hash)
        for chunk in spec.active_chunks_after
    )
    actual_chunks = tuple(
        (str(row[0]), str(row[1]).strip()) for row in chunk_rows
    )
    if actual_chunks != expected_chunks:
        raise ValidationError(
            "audit active chunks differ from the sealed event snapshot"
        )
    inserted = tuple(
        str(row[0]) for row in chunk_rows if int(row[2]) == event.epoch_id
    )
    if inserted != spec.inserted_active_chunk_ids:
        raise ValidationError(
            "audit inserted chunks differ from the sealed event structure"
        )

    published_claim_rows = connection.execute(
        """
        SELECT state.claim_id, state.support_count, state.refute_count,
               state.best_support_score, state.best_refute_score, state.status::text
        FROM groundloop_published_claim_state AS state
        JOIN groundloop_m4_claim_registry_member AS member
          ON member.claim_id = state.claim_id
         AND member.claim_registry_snapshot_id = %s
        WHERE state.valid_from_epoch <= %s
          AND (state.valid_to_epoch IS NULL OR %s < state.valid_to_epoch)
        ORDER BY state.claim_id
        """,
        (event.registry_snapshot_id, event.epoch_id, event.epoch_id),
    ).fetchall()
    if tuple(published_claim_rows) != tuple(
        _state_projection(state) for state in spec.selective_claim_states
    ):
        raise ValidationError(
            "selective claim states differ from the sealed publication"
        )

    answer_ids = tuple(
        sorted({claim.answer_version_id for claim in spec.claims})
    )
    published_answer_rows = connection.execute(
        """
        SELECT answer_version_id, required_claim_count, supported_count,
               unsupported_count, refuted_count, conflicted_count, status::text
        FROM groundloop_published_answer_state
        WHERE answer_version_id = ANY(%s)
          AND valid_from_epoch <= %s
          AND (valid_to_epoch IS NULL OR %s < valid_to_epoch)
        ORDER BY answer_version_id
        """,
        (list(answer_ids), event.epoch_id, event.epoch_id),
    ).fetchall()
    if tuple(published_answer_rows) != tuple(
        _answer_projection(state) for state in spec.selective_answer_states
    ):
        raise ValidationError(
            "selective answer states differ from the sealed publication"
        )


def _result_payload(
    *,
    audit: ExhaustiveAdditiveDelta,
    refresh: SnapshotRefreshResult,
    selective_exhaustive: AffectedSets,
    selective_refresh: AffectedSets,
    missed_positive_pairs: tuple[PairKey, ...],
    detected_deliberate_miss_pairs: tuple[PairKey, ...],
) -> dict[str, object]:
    return {
        "full_pair_audit": {
            "manifest_id": audit.audit.manifest_id,
            "expected_pair_count": audit.audit.expected_pair_count,
            "positive_pairs": _sorted_pair_payload(audit.audit.positive_pairs),
            "judgments": [
                _judgment_payload(judgment)
                for judgment in audit.audit.judgments
            ],
        },
        "exhaustive_additive": {
            "claim_states": [
                _claim_state_payload(state) for state in audit.claim_states
            ],
            "answer_states": [
                _answer_state_payload(state) for state in audit.answer_states
            ],
            "affected_from_working": _affected_payload(audit.affected_sets),
        },
        "snapshot_refresh": {
            "manifest_id": refresh.manifest_id,
            "refresh_policy_id": refresh.refresh_policy_id,
            "depth_k": refresh.depth_k,
            "corpus_snapshot_hash": refresh.corpus_snapshot_hash,
            "retrieved_pairs": _sorted_pair_payload(refresh.retrieved_pairs),
            "judgments": [
                _judgment_payload(judgment) for judgment in refresh.judgments
            ],
            "claim_states": [
                _claim_state_payload(state) for state in refresh.claim_states
            ],
            "answer_states": [
                _answer_state_payload(state) for state in refresh.answer_states
            ],
        },
        "selective_comparisons": {
            "versus_exhaustive": _affected_payload(selective_exhaustive),
            "versus_snapshot_refresh": _affected_payload(selective_refresh),
        },
        "missed_positive_pairs": _sorted_pair_payload(missed_positive_pairs),
        "detected_deliberate_miss_pairs": _sorted_pair_payload(
            detected_deliberate_miss_pairs
        ),
    }


def _build_manifest(
    *,
    spec: EventAuditSpec,
    event: _SealedEvent,
    audit: ExhaustiveAdditiveDelta,
    refresh: SnapshotRefreshResult,
    selective_exhaustive: AffectedSets,
    selective_refresh: AffectedSets,
    missed_positive_pairs: tuple[PairKey, ...],
    detected_deliberate_miss_pairs: tuple[PairKey, ...],
) -> tuple[dict[str, object], str, str, str]:
    input_payload = _input_payload(spec)
    input_hash = _json_digest(input_payload)
    result_payload = _result_payload(
        audit=audit,
        refresh=refresh,
        selective_exhaustive=selective_exhaustive,
        selective_refresh=selective_refresh,
        missed_positive_pairs=missed_positive_pairs,
        detected_deliberate_miss_pairs=detected_deliberate_miss_pairs,
    )
    result_hash = _json_digest(result_payload)
    baseline_manifest_id = "event-baselines-" + stable_m4_digest(
        "m4-event-baselines-v1",
        audit.audit.manifest_id,
        refresh.manifest_id,
    )
    manifest: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "event": {
            "event_id": event.event_id,
            "epoch_id": event.epoch_id,
            "payload_hash": event.payload_hash,
            "sealed_revision": event.revision,
            "candidate_policy_id": event.candidate_policy_id,
            "registry_snapshot_id": event.registry_snapshot_id,
        },
        "input": input_payload,
        "input_hash": input_hash,
        "result": result_payload,
        "result_hash": result_hash,
        "baseline_manifest_id": baseline_manifest_id,
    }
    manifest["manifest_hash"] = _json_digest(manifest)
    return manifest, input_hash, result_hash, baseline_manifest_id


def _pair_tuple(value: object, *, field: str) -> tuple[PairKey, ...]:
    if not isinstance(value, list):
        raise EventAuditPersistenceError(f"{field} must be a JSON array")
    pairs: list[PairKey] = []
    for item in value:
        if not isinstance(item, dict):
            raise EventAuditPersistenceError(f"{field} contains a non-object")
        if set(item) != {"claim_id", "chunk_version_id"}:
            raise EventAuditPersistenceError(f"{field} pair has invalid fields")
        claim_id = item["claim_id"]
        chunk_id = item["chunk_version_id"]
        if not isinstance(claim_id, str) or not isinstance(chunk_id, str):
            raise EventAuditPersistenceError(f"{field} pair IDs must be strings")
        pairs.append(PairKey(claim_id, chunk_id))
    result = tuple(pairs)
    if result != tuple(sorted(set(result))):
        raise EventAuditPersistenceError(f"{field} is not sorted and unique")
    return result


def _string_tuple(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise EventAuditPersistenceError(f"{field} must be a string array")
    result = tuple(cast(list[str], value))
    if result != tuple(sorted(set(result))):
        raise EventAuditPersistenceError(f"{field} is not sorted and unique")
    return result


def _object(value: object, *, field: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EventAuditPersistenceError(f"{field} must be a JSON object")
    return cast(dict[str, object], value)


def _validate_stored_manifest(
    *,
    manifest: dict[str, object],
    expected_input: dict[str, object],
    event: _SealedEvent,
    table_baseline_manifest_id: str,
) -> None:
    if manifest.get("schema_version") != _SCHEMA_VERSION:
        raise EventAuditPersistenceError("unsupported event-audit schema version")
    stored_manifest_hash = manifest.get("manifest_hash")
    if not isinstance(stored_manifest_hash, str):
        raise EventAuditPersistenceError("stored manifest hash is absent")
    unsigned = dict(manifest)
    del unsigned["manifest_hash"]
    if _json_digest(unsigned) != stored_manifest_hash:
        raise EventAuditPersistenceError("stored event-audit manifest hash mismatch")

    stored_input = _object(manifest.get("input"), field="input")
    if stored_input != expected_input:
        raise EventAuditConflictError(
            "evaluation run identity was reused with different frozen inputs"
        )
    if manifest.get("input_hash") != _json_digest(stored_input):
        raise EventAuditPersistenceError("stored event-audit input hash mismatch")
    result = _object(manifest.get("result"), field="result")
    if manifest.get("result_hash") != _json_digest(result):
        raise EventAuditPersistenceError("stored event-audit result hash mismatch")

    event_payload = _object(manifest.get("event"), field="event")
    expected_event: dict[str, object] = {
        "event_id": event.event_id,
        "epoch_id": event.epoch_id,
        "payload_hash": event.payload_hash,
        "sealed_revision": event.revision,
        "candidate_policy_id": event.candidate_policy_id,
        "registry_snapshot_id": event.registry_snapshot_id,
    }
    if event_payload != expected_event:
        raise EventAuditPersistenceError(
            "stored evaluation is not linked to the current sealed event"
        )

    full_pair = _object(result.get("full_pair_audit"), field="full_pair_audit")
    refresh = _object(result.get("snapshot_refresh"), field="snapshot_refresh")
    audit_manifest_id = full_pair.get("manifest_id")
    refresh_manifest_id = refresh.get("manifest_id")
    if not isinstance(audit_manifest_id, str) or not isinstance(
        refresh_manifest_id, str
    ):
        raise EventAuditPersistenceError("baseline manifest identities are absent")
    expected_baseline = "event-baselines-" + stable_m4_digest(
        "m4-event-baselines-v1", audit_manifest_id, refresh_manifest_id
    )
    if (
        manifest.get("baseline_manifest_id") != expected_baseline
        or table_baseline_manifest_id != expected_baseline
    ):
        raise EventAuditPersistenceError("combined baseline manifest mismatch")

    positive = _pair_tuple(full_pair.get("positive_pairs"), field="positive_pairs")
    missed = _pair_tuple(
        result.get("missed_positive_pairs"), field="missed_positive_pairs"
    )
    deliberate = _pair_tuple(
        result.get("detected_deliberate_miss_pairs"),
        field="detected_deliberate_miss_pairs",
    )
    if not set(deliberate) <= set(missed) <= set(positive):
        raise EventAuditPersistenceError("stored miss-set containment is invalid")
    claims = expected_input.get("claims")
    inserted_chunks = expected_input.get("inserted_active_chunk_ids")
    if not isinstance(claims, list) or not isinstance(inserted_chunks, list):
        raise EventAuditPersistenceError("stored Cartesian-domain inputs are invalid")
    expected_pair_count = len(claims) * len(inserted_chunks)
    if full_pair.get("expected_pair_count") != expected_pair_count:
        raise EventAuditPersistenceError("stored exhaustive pair count is invalid")
    admitted = _pair_tuple(
        expected_input.get("selective_admitted_pairs"),
        field="selective_admitted_pairs",
    )
    if missed != tuple(sorted(set(positive) - set(admitted))):
        raise EventAuditPersistenceError("stored positive miss set is not exact")
    designated = _pair_tuple(
        expected_input.get("deliberate_miss_pairs"),
        field="deliberate_miss_pairs",
    )
    if deliberate != tuple(sorted(set(designated) & set(missed))):
        raise EventAuditPersistenceError("stored deliberate miss set is not exact")


def _stored_outcome(
    *,
    row: tuple[object, ...],
    spec: EventAuditSpec,
    event: _SealedEvent,
    disposition: EventAuditDisposition,
) -> PersistedEventAudit:
    (
        evaluation_run_id,
        treatment_manifest_id,
        baseline_manifest_id,
        corpus_snapshot_hash,
        split_id,
        config_hash,
        status,
        raw_manifest,
    ) = row
    expected_run_id = _evaluation_run_id(spec)
    expected_columns = (
        expected_run_id,
        spec.treatment_manifest_id,
        spec.corpus_snapshot_hash,
        spec.split_id,
        _config_hash(spec),
        "completed",
    )
    actual_columns = (
        str(evaluation_run_id),
        str(treatment_manifest_id),
        str(corpus_snapshot_hash).strip(),
        str(split_id),
        str(config_hash).strip(),
        str(status),
    )
    if actual_columns != expected_columns:
        raise EventAuditConflictError(
            "evaluation run identity conflicts with persisted scalar columns"
        )
    if not isinstance(raw_manifest, dict):
        raise EventAuditPersistenceError("stored event-audit manifest is not an object")
    manifest = cast(dict[str, object], raw_manifest)
    _validate_stored_manifest(
        manifest=manifest,
        expected_input=_input_payload(spec),
        event=event,
        table_baseline_manifest_id=str(baseline_manifest_id),
    )

    result = _object(manifest["result"], field="result")
    full_pair = _object(result["full_pair_audit"], field="full_pair_audit")
    refresh = _object(result["snapshot_refresh"], field="snapshot_refresh")
    comparisons = _object(
        result["selective_comparisons"], field="selective_comparisons"
    )
    exhaustive_comparison = _object(
        comparisons["versus_exhaustive"], field="versus_exhaustive"
    )
    refresh_comparison = _object(
        comparisons["versus_snapshot_refresh"], field="versus_snapshot_refresh"
    )
    positive_pairs = _pair_tuple(
        full_pair["positive_pairs"], field="positive_pairs"
    )
    expected_pair_count = full_pair.get("expected_pair_count")
    if not isinstance(expected_pair_count, int) or expected_pair_count < 0:
        raise EventAuditPersistenceError("expected_pair_count is invalid")
    return PersistedEventAudit(
        evaluation_run_id=expected_run_id,
        event_id=event.event_id,
        epoch_id=event.epoch_id,
        disposition=disposition,
        treatment_manifest_id=spec.treatment_manifest_id,
        baseline_manifest_id=str(baseline_manifest_id),
        audit_manifest_id=str(full_pair["manifest_id"]),
        refresh_manifest_id=str(refresh["manifest_id"]),
        input_hash=str(manifest["input_hash"]),
        result_hash=str(manifest["result_hash"]),
        expected_pair_count=expected_pair_count,
        positive_pair_count=len(positive_pairs),
        missed_positive_pairs=_pair_tuple(
            result["missed_positive_pairs"], field="missed_positive_pairs"
        ),
        detected_deliberate_miss_pairs=_pair_tuple(
            result["detected_deliberate_miss_pairs"],
            field="detected_deliberate_miss_pairs",
        ),
        selective_exhaustive_status_claim_ids=_string_tuple(
            exhaustive_comparison["status_claim_ids"],
            field="selective_exhaustive.status_claim_ids",
        ),
        selective_exhaustive_answer_status_ids=_string_tuple(
            exhaustive_comparison["answer_status_ids"],
            field="selective_exhaustive.answer_status_ids",
        ),
        selective_refresh_status_claim_ids=_string_tuple(
            refresh_comparison["status_claim_ids"],
            field="selective_refresh.status_claim_ids",
        ),
        selective_refresh_answer_status_ids=_string_tuple(
            refresh_comparison["answer_status_ids"],
            field="selective_refresh.answer_status_ids",
        ),
    )


def _select_row(
    connection: Connection[Any], evaluation_run_id: str
) -> tuple[object, ...] | None:
    return connection.execute(
        """
        SELECT evaluation_run_id, treatment_manifest_id, baseline_manifest_id,
               corpus_snapshot_hash, split_id, config_hash, status, manifest
        FROM groundloop_impact_evaluation_run
        WHERE evaluation_run_id = %s
        """,
        (evaluation_run_id,),
    ).fetchone()


def run_and_persist_event_audit(
    connection: Connection[Any],
    *,
    spec: EventAuditSpec,
    audit_judge: PairJudge,
    refresh_judge: PairJudge,
) -> PersistedEventAudit:
    """Execute or exactly replay one sealed-event empirical evaluation.

    Model/judge callbacks run outside a database transaction.  Exact replays
    are returned from the persisted, content-validated record without invoking
    either callback.
    """
    if not connection.autocommit:
        raise ValidationError(
            "event audit requires an autocommit connection so judge callbacks "
            "cannot run inside a database transaction"
        )
    _validate_spec_semantics(spec)
    event = _load_sealed_event(connection, spec.event_id)
    _validate_event_inputs(connection, spec=spec, event=event)
    evaluation_run_id = _evaluation_run_id(spec)
    existing = _select_row(connection, evaluation_run_id)
    if existing is not None:
        return _stored_outcome(
            row=existing,
            spec=spec,
            event=event,
            disposition=EventAuditDisposition.REPLAYED,
        )

    audit_result = run_full_pair_audit(
        event_id=spec.event_id,
        registered_claim_ids=tuple(claim.claim_id for claim in spec.claims),
        inserted_active_chunk_ids=spec.inserted_active_chunk_ids,
        judge=audit_judge,
    )
    if any(judgment.split_id != spec.split_id for judgment in audit_result.judgments):
        raise ValidationError("audit judge returned a judgment for another split")
    claim_to_answer = {
        claim.claim_id: claim.answer_version_id for claim in spec.claims
    }
    required = frozenset(claim.claim_id for claim in spec.claims if claim.required)
    chunk_hashes = {
        chunk.chunk_version_id: chunk.text_hash
        for chunk in spec.active_chunks_after
    }
    exhaustive = compute_exhaustive_additive_delta(
        baseline_id=f"Bw-to-Bx:{spec.event_id}",
        audit=audit_result,
        claim_to_answer=claim_to_answer,
        required_claim_ids=required,
        active_chunk_text_hashes=chunk_hashes,
        surviving_judgments=spec.surviving_judgments,
        working_claim_states=spec.working_claim_states,
        working_answer_states=spec.working_answer_states,
    )
    refresh = run_snapshot_refresh(
        corpus_snapshot_hash=spec.corpus_snapshot_hash,
        refresh_policy_id=spec.refresh_policy_id,
        depth_k=spec.refresh_depth_k,
        claims=spec.claims,
        active_chunks=spec.active_chunks_after,
        judge=refresh_judge,
    )
    if any(judgment.split_id != spec.split_id for judgment in refresh.judgments):
        raise ValidationError("refresh judge returned a judgment for another split")
    selective_exhaustive = compute_affected_sets(
        baseline_id=f"Bs-vs-Bx:{spec.event_id}",
        before_claim_states=spec.selective_claim_states,
        after_claim_states=exhaustive.claim_states,
        before_answer_states=spec.selective_answer_states,
        after_answer_states=exhaustive.answer_states,
    )
    selective_refresh = compute_affected_sets(
        baseline_id=f"Bp-vs-RefreshK:{spec.event_id}",
        before_claim_states=spec.selective_claim_states,
        after_claim_states=refresh.claim_states,
        before_answer_states=spec.selective_answer_states,
        after_answer_states=refresh.answer_states,
    )
    missed_positive_pairs = tuple(
        sorted(set(audit_result.positive_pairs) - set(spec.selective_admitted_pairs))
    )
    detected_deliberate_miss_pairs = tuple(
        sorted(set(spec.deliberate_miss_pairs) & set(missed_positive_pairs))
    )
    manifest, _, _, baseline_manifest_id = _build_manifest(
        spec=spec,
        event=event,
        audit=exhaustive,
        refresh=refresh,
        selective_exhaustive=selective_exhaustive,
        selective_refresh=selective_refresh,
        missed_positive_pairs=missed_positive_pairs,
        detected_deliberate_miss_pairs=detected_deliberate_miss_pairs,
    )

    with connection.transaction():
        locked_event = _load_sealed_event(connection, spec.event_id)
        if locked_event != event:
            raise EventAuditConflictError(
                "sealed event identity changed while its audit was running"
            )
        _validate_event_inputs(connection, spec=spec, event=locked_event)
        inserted = connection.execute(
            """
            INSERT INTO groundloop_impact_evaluation_run (
                evaluation_run_id, treatment_manifest_id,
                baseline_manifest_id, corpus_snapshot_hash, split_id,
                config_hash, status, manifest, completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'completed', %s, now())
            ON CONFLICT (evaluation_run_id) DO NOTHING
            RETURNING evaluation_run_id
            """,
            (
                evaluation_run_id,
                spec.treatment_manifest_id,
                baseline_manifest_id,
                spec.corpus_snapshot_hash,
                spec.split_id,
                _config_hash(spec),
                Jsonb(manifest),
            ),
        ).fetchone()
        stored = _select_row(connection, evaluation_run_id)
        if stored is None:
            raise EventAuditPersistenceError("event-audit insert disappeared")
        disposition = (
            EventAuditDisposition.CREATED
            if inserted is not None
            else EventAuditDisposition.REPLAYED
        )
        return _stored_outcome(
            row=stored,
            spec=spec,
            event=event,
            disposition=disposition,
        )
