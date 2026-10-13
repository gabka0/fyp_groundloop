"""Package-private D32 readiness coordination, not a public/seal route.

The two edges own separate transactions. Semantic observations, D25 images,
matching contributions, publication heads and runtime mode are never changed.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection, Cursor, IsolationLevel, sql
from psycopg.pq import TransactionStatus

from groundloop.domain import ChunkVersion, DocumentVersion, SubjectKind
from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m4 import evaluation_overlay
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.pipeline import InsertedDocument, StructuralPayload
from groundloop.m5.digests import enum_field, int_field, text_field
from groundloop.m5.runtime import digests
from groundloop.m5.runtime import postgres_withdrawal as document_source
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    ActiveChunkSnapshotEntry,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5JobKind,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementRootProvenance,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
    SemanticPairKey,
)
from groundloop.m5.runtime.postgres_recovery import (
    _STRUCTURAL_OPEN_COUNTERS,
    _TIMING_COVERAGE_COLUMNS,
    _TIMING_SUM_COLUMNS,
    _timing_accumulator_from_row,
    _TimingAccumulator,
)
from groundloop.postgres.migrations import (
    M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID,
    _m5_readiness_bundle_row,
    _m5_readiness_schema,
    require_m5_document_update_completion_bundle,
    require_m5_semantic_readiness_bundle,
)

_WORK_COLUMNS = M5RuntimeWork.counter_names()
_KIND = M5RuntimeWorkContributionKind.SEMANTIC_READINESS
_PREDECESSORS = {
    "semantic_pending": "structural_committed",
    "semantic_complete": "semantic_pending",
}
_GROUP_KINDS = frozenset({"register_group", "replace_group", "retire_group"})
_R33_DOCUMENT_KINDS = frozenset(
    {"document_insert", "document_delete", "document_replace"}
)
_R33_COUNTER_FIELDS = (
    "epoch_id",
    "declaration_hash",
    "lifecycle_state",
    "default_evaluation_state",
    "confirmed_as_of_epoch",
    "open_discovery_scope_count",
    "revision",
)
_R33_SEAL_FIELDS = (
    "transition_id",
    "payload_hash",
    "transition_kind",
    "from_revision",
    "to_revision",
    "override_rows_written",
)
_R33_DIRECT_HISTORY = (
    "groundloop_semantic_job",
    "groundloop_discovery_scope",
    "groundloop_m5_direct_terminal_projection",
    "groundloop_m4_evaluation_override_counter",
)
_PENDING_KINDS = frozenset(
    {
        "structural_open",
        "m5_acquisition",
        "direct_acquisition",
        "m5_attempt_execution",
        "direct_attempt_execution",
        "direct_transition",
        "root_result_stage",
        "root_barrier",
        "verifier_completion",
        "cancellation",
        "preterminal_late_return",
        "semantic_readiness",
    }
)


@dataclass(frozen=True, slots=True)
class _ReadinessReceipt:
    anchor: M5TransitionTimingAnchor
    replay: bool


@dataclass(frozen=True, slots=True)
class _ReadinessArtifact:
    source_identity_hash: str
    work: M5RuntimeWork
    anchor: M5TransitionTimingAnchor


def _require_legacy_lookup_context(cursor: Cursor[Any], schema: str) -> None:
    """Reject ambiguous lookup before DML; retained 015/016 invokers are frozen.

    Qualifying a legacy call does not qualify its internal SQL. Do not rewrite
    it, trust a caller GUC or change settings. Require native builtins first,
    captured authority next, installed pgcrypto last (deduplicated when the
    captured schema is public), and no temporary names colliding with
    authority/native types. Noncolliding matching temp objects remain legal.
    Public is never a fallback authority source when distinct from the captured
    schema: the preceding exact 020 check verifies the complete own catalog.
    """
    row = cursor.execute("SELECT pg_catalog.current_schemas(true)").fetchone()
    assert row is not None
    permanent = tuple(name for name in row[0] if not name.startswith("pg_temp_"))
    expected = tuple(dict.fromkeys(("pg_catalog", schema, "public")))
    if permanent != expected:
        raise ValidationError("readiness rejects mixed legacy lookup namespaces")
    collision = cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class temp "
        "JOIN pg_catalog.pg_class own "
        "ON temp.relname OPERATOR(pg_catalog.=) own.relname "
        "JOIN pg_catalog.pg_namespace n "
        "ON n.oid OPERATOR(pg_catalog.=) own.relnamespace "
        "WHERE temp.relnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema() "
        "AND n.nspname OPERATOR(pg_catalog.=) %s) OR EXISTS ("
        "SELECT 1 FROM pg_catalog.pg_type temp JOIN pg_catalog.pg_type own "
        "ON temp.typname OPERATOR(pg_catalog.=) own.typname "
        "JOIN pg_catalog.pg_namespace n "
        "ON n.oid OPERATOR(pg_catalog.=) own.typnamespace "
        "WHERE temp.typnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema() "
        "AND (n.nspname OPERATOR(pg_catalog.=) %s "
        "OR n.nspname OPERATOR(pg_catalog.=) 'pg_catalog'))",
        (schema, schema),
    ).fetchone()
    if collision is None or collision[0] is not False:
        raise ValidationError("readiness rejects colliding temporary lookup authority")


def _readiness_artifact(
    *,
    epoch_id: int,
    structural_event_id: str,
    event_payload_hash: str,
    requirement_root_set_hash: str,
    target: str,
    expected_revision: int,
) -> _ReadinessArtifact:
    """Two explicit producer encodes/hashes; DTO validation is excluded work."""
    if type(target) is not str or target not in _PREDECESSORS:
        raise ValidationError("readiness target must name one of the two D32 edges")
    if type(expected_revision) is not int or expected_revision < 1:
        raise ValidationError("readiness input revision must be a positive integer")
    # Boundary S: source bytes are encoded once and hashed once by the producer.
    source_bytes = digests.semantic_readiness_transition_preimage(
        epoch_id=epoch_id,
        structural_event_id=structural_event_id,
        event_payload_hash=event_payload_hash,
        requirement_root_set_hash=requirement_root_set_hash,
        from_runtime_state=_PREDECESSORS[target],
        to_runtime_state=target,
        expected_revision=expected_revision,
        resulting_revision=expected_revision + 1,
    )
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    # Boundary K: unchanged D24 key bytes, separately encoded/hashed once.
    key_bytes = digests.stable_m5_preimage(
        "m5-runtime-work-contribution-key-v1",
        int_field(epoch_id),
        enum_field(_KIND),
        text_field(target),
    )
    key_hash = hashlib.sha256(key_bytes).hexdigest()
    byte_count = len(source_bytes) + len(key_bytes)
    return _ReadinessArtifact(
        source_hash,
        M5RuntimeWork(bytes_hashed=byte_count, bytes_serialized=byte_count),
        M5TransitionTimingAnchor(
            epoch_id=epoch_id,
            contribution_kind=_KIND,
            source_id=target,
            contribution_key_digest=key_hash,
            anchor_revision=expected_revision + 1,
            terminal_transition=False,
        ),
    )


def _read_row(
    cursor: Cursor[Any], schema: str, relation: str, epoch_id: int, *, lock: bool
) -> dict[str, Any]:
    statement = sql.SQL(
        "SELECT * FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s{}"
    ).format(sql.Identifier(schema, relation), sql.SQL(" FOR UPDATE" if lock else ""))
    row = cursor.execute(statement, (epoch_id,)).fetchone()
    if row is None or cursor.description is None:
        raise InvalidEventError("readiness requires an existing typed epoch point")
    result = dict(zip((column.name for column in cursor.description), row, strict=True))
    if result["epoch_id"] != epoch_id:
        raise ValidationError("readiness point returned another epoch")
    return result


def _hash(value: Any) -> str:
    # CHAR(64) catalog values retain no semantic padding.
    if type(value) is not str:
        raise ValidationError("stored readiness identity is not a hash string")
    result = value.rstrip(" ")
    if len(result) != 64 or any(
        character not in "0123456789abcdef" for character in result
    ):
        raise ValidationError("stored readiness identity is not a canonical hash")
    return result


def _check_epoch_identity(
    base: dict[str, Any], runtime: dict[str, Any], *, structural_event_id: str
) -> None:
    if (
        base["event_id"] != structural_event_id
        or runtime["structural_event_id"] != structural_event_id
        or base["epoch_id"] != runtime["epoch_id"]
        or base["revision"] != runtime["revision"]
    ):
        raise EventConflictError("readiness event/base/runtime identities diverge")
    state = runtime["runtime_state"]
    expected_status = {
        "structural_committed": ("committed", "pending", "pending"),
        "semantic_pending": ("committed", "pending", "pending"),
        "semantic_complete": ("committed", "complete", "complete"),
        "sealed": ("committed", "sealed", "complete"),
        "failed": ("failed", "failed", "failed"),
    }.get(state)
    terminal = state in {"sealed", "failed"}
    if (
        expected_status is None
        or (
            base["structural_status"],
            base["semantic_status"],
            base["evaluation_state"],
        )
        != expected_status
        or (base["sealed_at"] is not None) != (state == "sealed")
        or (runtime["terminal_at"] is not None) != terminal
    ):
        raise ValidationError("readiness rejects half-terminal/inconsistent headers")
    _hash(base["payload_hash"])
    _hash(runtime["requirement_root_set_hash"])


def _r33_require_authority(cursor: Cursor[Any], schema: str) -> bool:
    """A present021 is authoritative or an error, never an020 fallback."""
    present = (
        _m5_readiness_bundle_row(
            cursor, M5_DOCUMENT_UPDATE_COMPLETION_BUNDLE_ID, schema_name=schema
        )
        is not None
    )
    if present:
        require_m5_document_update_completion_bundle(cursor, schema_name=schema)
    else:
        require_m5_semantic_readiness_bundle(cursor, schema_name=schema)
    return present


def _r33_records(
    cursor: Cursor[Any], statement: sql.Composed, parameters: tuple[Any, ...]
) -> list[dict[str, Any]]:
    cursor.execute(statement, parameters)
    assert cursor.description is not None
    names = tuple(column.name for column in cursor.description)
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _r33_record(
    cursor: Cursor[Any], schema: str, relation: str, key: str, value: Any
) -> dict[str, Any]:
    records = _r33_records(
        cursor,
        sql.SQL("SELECT * FROM {} WHERE {} OPERATOR(pg_catalog.=) %s").format(
            sql.Identifier(schema, relation), sql.Identifier(key)
        ),
        (value,),
    )
    if len(records) != 1:
        raise EventConflictError(
            "readiness retained document source is absent/ambiguous"
        )
    return records[0]


def _r33_text(value: Any) -> str:
    if type(value) is not str or not value.strip():
        raise EventConflictError("readiness document source identifier is malformed")
    return value


def _r33_snapshot_headers(
    cursor: Cursor[Any], schema: str, epoch: int, runtime: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    registry_hash = _hash(runtime["requirement_registry_snapshot_digest"])
    chunk_hash = _hash(runtime["active_chunk_snapshot_digest"])
    registry = _r33_record(
        cursor,
        schema,
        "groundloop_m5_requirement_registry_snapshot",
        "requirement_registry_snapshot_digest",
        registry_hash,
    )
    chunks = _r33_record(
        cursor,
        schema,
        "groundloop_m5_active_chunk_snapshot",
        "active_chunk_snapshot_digest",
        chunk_hash,
    )
    for header in (registry, chunks):
        if (
            type(header["created_epoch_id"]) is not int
            or not 1 <= header["created_epoch_id"] <= epoch
            or not isinstance(header["created_at"], datetime)
        ):
            raise EventConflictError("readiness snapshot creation point changed")
    if (
        chunks["normalizer_id"] != document_source._D29_NORMALIZER_ID
        or _hash(chunks["normalizer_provenance_hash"])
        != document_source._D29_NORMALIZER_PROVENANCE_HASH
    ):
        raise EventConflictError("readiness snapshot normalizer authority changed")
    return registry, chunks


def _r33_snapshots(
    cursor: Cursor[Any], schema: str, epoch: int, runtime: dict[str, Any]
) -> tuple[RequirementRegistrySnapshot, ActiveChunkSnapshot]:
    """Re-encode retained snapshot members, not today's corpus or semantics."""
    registry, chunks = _r33_snapshot_headers(cursor, schema, epoch, runtime)
    registry_hash = _hash(runtime["requirement_registry_snapshot_digest"])
    chunk_hash = _hash(runtime["active_chunk_snapshot_digest"])
    members = _r33_records(
        cursor,
        sql.SQL(
            "SELECT * FROM {} WHERE requirement_registry_snapshot_digest "
            "OPERATOR(pg_catalog.=) %s ORDER BY member_ordinal"
        ).format(
            sql.Identifier(schema, "groundloop_m5_requirement_registry_snapshot_member")
        ),
        (registry_hash,),
    )
    chunk_members = _r33_records(
        cursor,
        sql.SQL(
            "SELECT * FROM {} WHERE active_chunk_snapshot_digest "
            "OPERATOR(pg_catalog.=) %s ORDER BY member_ordinal"
        ).format(sql.Identifier(schema, "groundloop_m5_active_chunk_snapshot_member")),
        (chunk_hash,),
    )
    for rows in (members, chunk_members):
        if tuple(row["member_ordinal"] for row in rows) != tuple(range(len(rows))):
            raise EventConflictError("readiness snapshot members are not dense")
    requirement_snapshot = RequirementRegistrySnapshot(
        registry["requirement_count"],
        tuple(
            RequirementRegistrySnapshotEntry(
                requirement_version_id=row["requirement_version_id"],
                group_version_id=row["group_version_id"],
                group_family_id=row["group_family_id"],
                owner_claim_id=row["owner_claim_id"],
                normalized_requirement_text=row["normalized_requirement_text"],
                requirement_text_hash=_hash(row["requirement_text_hash"]),
            )
            for row in members
        ),
        registry_hash,
    )
    chunk_snapshot = ActiveChunkSnapshot(
        chunks["chunk_count"],
        tuple(
            ActiveChunkSnapshotEntry(row["chunk_version_id"], _hash(row["text_hash"]))
            for row in chunk_members
        ),
        chunk_hash,
    )
    return requirement_snapshot, chunk_snapshot


def _r33_terminal_result(
    cursor: Cursor[Any], base: dict[str, Any], runtime: dict[str, Any]
) -> None:
    """Canonical immutable envelope first; never hydrate a terminal D29 plan."""
    result = document_source._load_canonical_terminal_result(
        cursor,
        structural_event_id=str(runtime["structural_event_id"]),
        payload_hash=_hash(base["payload_hash"]),
    )
    if (
        result is None
        or result.epoch_id != base["epoch_id"]
        or result.replayed_outcome is not M5ReplayedOutcome.SEALED
        or result.publication_receipt is None
        or result.publication_receipt.publication_id
        != stable_m4_digest("m4-publication-v1", str(base["epoch_id"]))
        or result.event_work.public_delta_count != len(result.combined_deltas)
        or any(
            (ref.epoch_id, ref.revision) != (base["epoch_id"], base["revision"])
            for ref in result.changed_state_references
        )
    ):
        raise EventConflictError("readiness terminal document envelope is not exact")


def _r33_predecessor(
    cursor: Cursor[Any], schema: str, epoch: int, predecessor: int
) -> dict[str, Any]:
    """Historical P authority, without requiring P to be today's head."""
    previous = _read_row(cursor, schema, "groundloop_epoch", predecessor, lock=False)
    if (
        not 0 < predecessor < epoch
        or (
            previous["structural_status"],
            previous["semantic_status"],
            previous["evaluation_state"],
            previous["publication_mode"],
        )
        != ("committed", "sealed", "complete", "strict")
        or previous["sealed_at"] is None
    ):
        raise EventConflictError("readiness document predecessor is not sealed")
    typed = _rows(cursor, schema, "groundloop_m5_runtime_epoch", predecessor)
    if typed:
        _check_epoch_identity(
            previous, typed[0], structural_event_id=str(previous["event_id"])
        )
        if typed[0]["runtime_state"] != "sealed" or any(
            typed[0][name] != 0
            for name in (
                "open_work_count",
                "open_scope_count",
                "blocking_failure_count",
            )
        ):
            raise EventConflictError("readiness predecessor has unresolved typed work")
        _r33_terminal_result(cursor, previous, typed[0])
        _r33_snapshot_headers(cursor, schema, predecessor, typed[0])
    else:
        activation = _r33_record(
            cursor, schema, "groundloop_m5_activation", "singleton", True
        )
        if activation["base_m4_epoch_id"] != predecessor or not isinstance(
            activation["activated_at"], datetime
        ):
            raise EventConflictError(
                "readiness predecessor is not activation bootstrap"
            )
        _r33_text(activation["activation_id"])
        _hash(activation["payload_hash"])
    return previous


def _r33_counter_point(
    base: dict[str, Any], runtime: dict[str, Any], predecessor: int
) -> None:
    """Validate ONLY the same-statement header/counter/history snapshot."""
    point = runtime.get("_r33_document_point")
    if type(point) is not dict or set(point) != {
        "transition_count",
        "direct_history",
        "owner_counter_bad",
        "answer_counter_bad",
        "counter",
        "seal",
    }:
        raise EventConflictError("readiness document counter snapshot is absent")
    if type(point["transition_count"]) is not int or any(
        type(point[name]) is not bool
        for name in ("direct_history", "owner_counter_bad", "answer_counter_bad")
    ):
        raise EventConflictError("readiness counter history snapshot is malformed")
    counter = point["counter"]
    transition = point["seal"]
    if (
        type(counter) is not dict
        or set(counter) != set(_R33_COUNTER_FIELDS)
        or type(transition) is not dict
        or set(transition) != set(_R33_SEAL_FIELDS)
    ):
        raise EventConflictError("readiness document counter/seal shape is malformed")
    if (
        any(
            type(counter[name]) is not int
            for name in (
                "epoch_id",
                "revision",
                "confirmed_as_of_epoch",
                "open_discovery_scope_count",
            )
        )
        or counter["epoch_id"] != base["epoch_id"]
        or counter["default_evaluation_state"] != "complete"
        or counter["open_discovery_scope_count"] != 0
        or _hash(counter["declaration_hash"])
        != evaluation_overlay._declaration_hash(
            int(base["epoch_id"]), 1, predecessor, 0
        )
        or point["direct_history"]
        or point["owner_counter_bad"]
        or point["answer_counter_bad"]
        or any(
            runtime[name] != 0
            for name in (
                "open_work_count",
                "open_scope_count",
                "blocking_failure_count",
            )
        )
    ):
        raise InvalidEventError("readiness document has direct history or nonzero work")
    if runtime["runtime_state"] == "failed":
        raise InvalidEventError("job-free document failure composition remains pending")
    if runtime["runtime_state"] != "sealed":
        if (
            (
                counter["lifecycle_state"],
                counter["revision"],
                counter["confirmed_as_of_epoch"],
            )
            != ("active", 1, predecessor)
            or point["transition_count"] != 0
            or any(value is not None for value in transition.values())
        ):
            raise EventConflictError(
                "readiness requires the original active counter1/P"
            )
        return
    canonical_id = stable_m4_digest("m4-evaluation-seal-v1", str(base["epoch_id"]))
    canonical_payload = evaluation_overlay._sha256_json(
        evaluation_overlay._transition_payload(
            evaluation_overlay.EvaluationTransition(
                canonical_id, 1, kind=evaluation_overlay.EvaluationTransitionKind.SEAL
            ),
            (),
        )
    )
    if (
        (
            counter["lifecycle_state"],
            counter["revision"],
            counter["confirmed_as_of_epoch"],
        )
        != ("sealed", 2, base["epoch_id"])
        or point["transition_count"] != 1
        or tuple(transition[name] for name in _R33_SEAL_FIELDS)
        != (canonical_id, canonical_payload, "seal", 1, 2, 0)
        or any(
            type(transition[name]) is not int
            for name in ("from_revision", "to_revision", "override_rows_written")
        )
    ):
        raise EventConflictError("readiness replay lacks canonical M4 SEAL1-to-2")


def _r33_payload(value: Any, kind: str) -> StructuralPayload:
    """Decode a structural sidecar, NEVER a legacy event or payload hash."""
    if (
        type(value) is not dict
        or set(value) != {"_groundloop_m4_runtime_v1"}
        or type(value["_groundloop_m4_runtime_v1"]) is not dict
    ):
        raise EventConflictError("readiness direct runtime manifest is malformed")
    runtime = value["_groundloop_m4_runtime_v1"]
    if (
        set(runtime) != {"event_manifest", "scope_claim_ids", "failure_reason"}
        or runtime["scope_claim_ids"] != {}
        or type(runtime["scope_claim_ids"]) is not dict
        or runtime["failure_reason"] is not None
        or type(runtime["event_manifest"]) is not dict
    ):
        raise EventConflictError("readiness direct runtime declaration changed")
    raw = runtime["event_manifest"]
    old = raw.get("deactivated_document_version_id")
    if (kind == "document_insert") != (old is None):
        raise EventConflictError("readiness action/deactivation identity changed")
    if old is not None:
        _r33_text(old)
    if kind == "document_delete":
        payload = StructuralPayload(deactivated_document_version_id=old)
    else:
        # The accepted InsertedDocument invariant requires at least one chunk.
        # Such a coherent INSERT/REPLACE necessarily has impact roots; do not
        # suppress them or invent empty-chunk positivity to reach this branch.
        try:
            if type(raw["inserted_chunks"]) is not list:
                raise ValidationError("inserted chunks are not an array")
            inserted = InsertedDocument(
                version=DocumentVersion(
                    _r33_text(raw["inserted_document_version_id"]),
                    _r33_text(raw["inserted_document_id"]),
                    _hash(raw["inserted_content_hash"]),
                ),
                chunks=tuple(ChunkVersion(**item) for item in raw["inserted_chunks"]),
                source_uri=raw["source_uri"],
                authority_class=_r33_text(raw["authority_class"]),
                chunker_version=_r33_text(raw["chunker_version"]),
                chunker_artifact_id=raw["chunker_artifact_id"],
                chunker_input_hash=raw["chunker_input_hash"],
            )
            payload = StructuralPayload(inserted, old)
        except (KeyError, TypeError, AttributeError, ValidationError) as error:
            raise EventConflictError(
                "readiness inserted source is malformed"
            ) from error
    if raw != payload.manifest:
        raise EventConflictError("readiness structural sidecar is not canonical")
    if payload.inserted_chunk_ids:
        raise InvalidEventError("inserted chunks require genuine direct impact work")
    return payload


def _r33_withdrawn_chunks(
    cursor: Cursor[Any],
    schema: str,
    epoch: int,
    predecessor: int,
    payload: StructuralPayload,
) -> tuple[str, ...]:
    old = payload.deactivated_document_version_id
    if old is None:
        raise EventConflictError("job-free document source lacks its withdrawn version")
    rows = _r33_records(
        cursor,
        sql.SQL(
            "SELECT version.*,chunk.chunk_version_id,chunk.chunk_index,chunk.text,"
            "chunk.text_hash,chunk.chunker_version,"
            "chunk.valid_from_epoch AS chunk_from,chunk.valid_to_epoch AS chunk_to,"
            "provenance.chunker_artifact_id,provenance.input_hash,"
            "artifact.chunker_version AS artifact_version "
            "FROM {} version JOIN {} document USING(document_id) "
            "JOIN {} chunk USING(document_version_id) "
            "LEFT JOIN {} provenance USING(chunk_version_id) "
            "LEFT JOIN {} artifact USING(chunker_artifact_id) "
            "WHERE version.document_version_id OPERATOR(pg_catalog.=) %s "
            'ORDER BY chunk.chunk_version_id COLLATE pg_catalog."C"'
        ).format(
            *(
                sql.Identifier(schema, table)
                for table in (
                    "groundloop_document_version",
                    "groundloop_document",
                    "groundloop_chunk_version",
                    "groundloop_chunk_provenance",
                    "groundloop_chunker_artifact",
                )
            )
        ),
        (old,),
    )
    if not rows:
        raise EventConflictError("readiness withdrawn source closure is absent")
    if any(
        row["document_version_id"] != old
        or type(row["valid_from_epoch"]) is not int
        or not 1 <= row["valid_from_epoch"] <= predecessor
        or row["chunk_from"] != row["valid_from_epoch"]
        or row["valid_to_epoch"] != epoch
        or row["chunk_to"] != epoch
        or row["artifact_version"] != row["chunker_version"]
        for row in rows
    ):
        raise EventConflictError("readiness withdrawn intervals/provenance changed")
    provenance = {
        (
            _r33_text(row["chunker_artifact_id"]),
            _hash(row["input_hash"]),
            _r33_text(row["chunker_version"]),
        )
        for row in rows
    }
    if len(provenance) != 1:
        raise EventConflictError("readiness withdrawn chunk provenance is not exact")
    artifact, input_hash, chunker = next(iter(provenance))
    version = DocumentVersion(
        old, _r33_text(rows[0]["document_id"]), _hash(rows[0]["content_hash"])
    )
    if any(
        row["document_id"] != version.document_id
        or _hash(row["content_hash"]) != version.content_hash
        for row in rows
    ):
        raise EventConflictError("readiness withdrawn parent identity changed")
    inserted = InsertedDocument(
        version=version,
        chunks=tuple(
            ChunkVersion(
                row["chunk_version_id"],
                old,
                row["chunk_index"],
                row["text"],
                _hash(row["text_hash"]),
            )
            for row in rows
        ),
        chunker_version=chunker,
        chunker_artifact_id=artifact,
        chunker_input_hash=input_hash,
    )
    deactivations = _rows(
        cursor, schema, "groundloop_m4_structural_deactivation", epoch
    )
    if len(deactivations) != 1 or deactivations[0]["document_version_id"] != old:
        raise EventConflictError("readiness deactivation set is not exact")
    for table, key in (
        ("groundloop_document_version", "valid_from_epoch"),
        ("groundloop_chunk_version", "valid_from_epoch"),
        ("groundloop_m4_document_metadata_overlay", "epoch_id"),
    ):
        if _r33_records(
            cursor,
            sql.SQL(
                "SELECT 1 FROM {} WHERE {} OPERATOR(pg_catalog.=) %s LIMIT 1"
            ).format(sql.Identifier(schema, table), sql.Identifier(key)),
            (epoch,),
        ):
            raise EventConflictError("job-free DELETE contains inserted sidecars")
    return tuple(chunk.chunk_version_id for chunk in inserted.chunks)


def _r33_original_contribution(
    cursor: Cursor[Any], schema: str, base: dict[str, Any], runtime: dict[str, Any]
) -> None:
    rows = _r33_records(
        cursor,
        sql.SQL(
            "SELECT * FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            "AND contribution_kind OPERATOR(pg_catalog.=) 'structural_open'"
        ).format(sql.Identifier(schema, "groundloop_m5_runtime_work_contribution")),
        (base["epoch_id"],),
    )
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=base["epoch_id"],
        contribution_kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        source_id=runtime["structural_event_id"],
        anchor_revision=1,
        terminal_transition=False,
    )
    if len(rows) != 1:
        raise EventConflictError("document requires exactly one structural-open proof")
    row = rows[0]
    M5RuntimeWork(
        **{name: row[name] for name in _WORK_COLUMNS},
        work_digest=_hash(row["work_digest"]),
    )
    if (
        row["epoch_id"] != base["epoch_id"]
        or any(
            row[name] != 0
            for name in _WORK_COLUMNS
            if name not in _STRUCTURAL_OPEN_COUNTERS
        )
        or row["source_id"] != anchor.source_id
        or type(row["applied_revision"]) is not int
        or row["applied_revision"] != 1
        or _hash(row["source_identity_hash"]) != _hash(base["payload_hash"])
        or _hash(row["contribution_key_digest"]) != anchor.contribution_key_digest
    ):
        raise EventConflictError("document structural-open key/source/revision changed")


def _r33_job_declarations(
    cursor: Cursor[Any],
    schema: str,
    runtime: dict[str, Any],
    policy: M5CandidatePolicyManifest,
    requirement_snapshot: RequirementRegistrySnapshot,
    chunk_snapshot: ActiveChunkSnapshot,
) -> None:
    """Validate retained D29 specs/provenance without constructing an event plan.

    Reuse native plan-independent completion validators with their explicit
    retained tuple layouts. This is validation work, not a producer/accounting
    boundary. In particular it never recomputes a legacy event payload hash.
    """
    epoch = runtime["epoch_id"]
    revision = runtime["revision"]
    job_columns = (
        "logical_job_id",
        "epoch_id",
        "structural_event_id",
        "job_kind",
        "candidate_policy_id",
        "candidate_policy_manifest_hash",
        "parent_job_id",
        "subject_kind",
        "subject_id",
        "chunk_version_id",
        "semantic_pair_digest",
        "admitted_pair_digest",
        "scope_contract_digest",
        "requirement_registry_snapshot_digest",
        "active_chunk_snapshot_digest",
        "role_template_hash",
        "execution_spec_hash",
        "expandable",
        "payload_hash",
        "job_state",
        "result_artifact_id",
        "result_artifact_hash",
        "scope_closure_digest",
        "child_set_hash",
        "archive_reason",
        "completion_digest",
        "cancelled_by_event_id",
        "cancelled_by_epoch_id",
        "cancellation_reason",
        "created_revision",
        "completed_revision",
        "created_at",
        "completed_at",
    )
    scope_columns = (
        "root_job_id",
        "epoch_id",
        "direction",
        "requirement_version_id",
        "inserted_chunk_version_id",
        "candidate_policy_id",
        "requirement_registry_snapshot_digest",
        "active_chunk_snapshot_digest",
        "scope_contract_digest",
        "scope_state",
        "created_revision",
        "staged_result_artifact_hash",
        "scope_closure_digest",
        "child_set_hash",
        "completion_digest",
        "staged_revision",
        "closed_revision",
        "created_at",
        "closed_at",
    )
    jobs = _r33_records(
        cursor,
        sql.SQL(
            "SELECT * FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            'ORDER BY job_state,logical_job_id COLLATE pg_catalog."C"'
        ).format(sql.Identifier(schema, "groundloop_m5_semantic_job")),
        (epoch,),
    )
    specs: dict[str, M5LogicalJobSpec] = {}
    retained: dict[str, dict[str, Any]] = {}
    for job in sorted(jobs, key=lambda row: _hash(row["logical_job_id"])):
        try:
            kind = M5JobKind(job["job_kind"])
            pair = None
            if kind is M5JobKind.VERIFY_REQUIREMENT_PAIR:
                if job["subject_kind"] != "requirement":
                    raise ValidationError("retained child has another subject kind")
                _hash(job["admitted_pair_digest"])
                pair = SemanticPairKey(
                    SubjectKind.REQUIREMENT,
                    _r33_text(job["subject_id"]),
                    _r33_text(job["chunk_version_id"]),
                )
                pair.validate_snapshots(requirement_snapshot, chunk_snapshot)
            elif any(job[name] is not None for name in job_columns[6:12]):
                raise ValidationError("retained root has child-only fields")
            spec = M5LogicalJobSpec(
                logical_job_id=_hash(job["logical_job_id"]),
                structural_event_id=_r33_text(job["structural_event_id"]),
                job_kind=kind,
                candidate_policy_id=_r33_text(job["candidate_policy_id"]),
                candidate_policy_manifest_hash=_hash(
                    job["candidate_policy_manifest_hash"]
                ),
                parent_job_id=None
                if job["parent_job_id"] is None
                else _hash(job["parent_job_id"]),
                pair=pair,
                semantic_pair_digest=None
                if pair is None
                else _hash(job["semantic_pair_digest"]),
                scope_contract_digest=_hash(job["scope_contract_digest"]),
                requirement_registry_snapshot_digest=_hash(
                    job["requirement_registry_snapshot_digest"]
                ),
                active_chunk_snapshot_digest=_hash(job["active_chunk_snapshot_digest"]),
                role_template_hash=_hash(job["role_template_hash"]),
                execution_spec_hash=_hash(job["execution_spec_hash"]),
                expandable=job["expandable"],
                payload_hash=_hash(job["payload_hash"]),
            )
        except (ValueError, ValidationError) as error:
            raise EventConflictError("retained document job spec is invalid") from error
        if (
            job["epoch_id"] != epoch
            or spec.structural_event_id != runtime["structural_event_id"]
            or spec.candidate_policy_id != policy.candidate_policy_id
            or spec.candidate_policy_manifest_hash != policy.manifest_hash
            or spec.requirement_registry_snapshot_digest
            != runtime["requirement_registry_snapshot_digest"]
            or spec.active_chunk_snapshot_digest
            != runtime["active_chunk_snapshot_digest"]
            or type(job["created_revision"]) is not int
            or not 1 <= job["created_revision"] <= revision
            or (spec.parent_job_id is None and job["created_revision"] != 1)
            or spec.logical_job_id in specs
        ):
            raise EventConflictError("retained document job declaration changed")
        specs[spec.logical_job_id] = spec
        retained[spec.logical_job_id] = job
    forward_roots = set()
    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for spec in specs.values():
        if spec.parent_job_id is not None:
            children_by_parent[spec.parent_job_id].append(spec.logical_job_id)
    for job_id, spec in specs.items():
        children = tuple(sorted(children_by_parent.get(job_id, ())))
        document_source._validate_m5_job_state(
            spec, tuple(retained[job_id][name] for name in job_columns), children
        )
        scope_rows = _r33_records(
            cursor,
            sql.SQL(
                "SELECT * FROM {} WHERE root_job_id OPERATOR(pg_catalog.=) %s"
            ).format(sql.Identifier(schema, "groundloop_m5_discovery_scope")),
            (job_id,),
        )
        provenance = _r33_records(
            cursor,
            sql.SQL(
                "SELECT * FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                "AND root_job_id OPERATOR(pg_catalog.=) %s"
            ).format(
                sql.Identifier(schema, "groundloop_m5_requirement_root_provenance")
            ),
            (epoch, job_id),
        )
        if spec.parent_job_id is not None:
            parent = specs.get(spec.parent_job_id)
            if (
                scope_rows
                or provenance
                or parent is None
                or parent.parent_job_id is not None
                or parent.scope_contract_digest != spec.scope_contract_digest
            ):
                raise EventConflictError(
                    "retained child has invalid scope/provenance/parent"
                )
            continue
        # A genuine no-direct-impact DELETE inserts no chunks, hence no reverse root.
        if (
            spec.job_kind is not M5JobKind.FORWARD_REQUIREMENT_RETRIEVAL
            or len(scope_rows) != 1
        ):
            raise EventConflictError(
                "job-free document has a reverse/missing root scope"
            )
        row = scope_rows[0]
        try:
            scope = M5DiscoveryScopeContract(
                M5DiscoveryDirection(row["direction"]),
                row["requirement_version_id"],
                row["inserted_chunk_version_id"],
                _r33_text(row["candidate_policy_id"]),
                _hash(row["requirement_registry_snapshot_digest"]),
                _hash(row["active_chunk_snapshot_digest"]),
                _hash(row["scope_contract_digest"]),
            )
            scope.validate_snapshots(requirement_snapshot, chunk_snapshot)
            spec.validate_manifest_and_scope(policy, scope)
            for child_id in children:
                specs[child_id].validate_manifest_and_scope(policy, scope)
        except (ValueError, ValidationError) as error:
            raise EventConflictError(
                "retained document scope/spec binding changed"
            ) from error
        if (
            row["root_job_id"] != job_id
            or row["epoch_id"] != epoch
            or type(row["created_revision"]) is not int
            or row["created_revision"] != 1
        ):
            raise EventConflictError("retained document scope declaration changed")
        document_source._validate_m5_scope_state(
            spec,
            tuple(retained[job_id][name] for name in job_columns),
            tuple(row[name] for name in scope_columns),
        )
        expected = M5RequirementRootProvenance.build(
            epoch_id=epoch, root_job_id=job_id, fallback_required=True
        )
        if (
            len(provenance) != 1
            or any(
                provenance[0][name] != getattr(expected, name)
                for name in (
                    "epoch_id",
                    "root_job_id",
                    "fallback_required",
                    "provenance_digest",
                )
            )
            or provenance[0]["fallback_required"] is not True
        ):
            raise EventConflictError(
                "retained document forward root lacks exact provenance"
            )
        forward_roots.add(job_id)
    if {
        _hash(row["root_job_id"])
        for row in _rows(
            cursor, schema, "groundloop_m5_requirement_root_provenance", epoch
        )
    } != forward_roots:
        raise EventConflictError("retained document provenance set is not exact")


def _r33_document_declaration(
    cursor: Cursor[Any],
    schema: str,
    base: dict[str, Any],
    runtime: dict[str, Any],
    update: dict[str, Any],
    *,
    replay: bool,
) -> None:
    epoch = int(base["epoch_id"])
    predecessor = int(update["previous_published_epoch_id"])
    if document_source._validate_document_manifest(update["manifest"]) != ((), (), ()):
        raise InvalidEventError("readiness requires all three direct arrays empty")
    # Terminal envelope precedes source inspection, never D29 hydration.
    if runtime["runtime_state"] == "sealed":
        _r33_terminal_result(cursor, base, runtime)
    _r33_counter_point(base, runtime, predecessor)
    direct = _read_row(cursor, schema, "groundloop_m4_update", epoch, lock=False)
    policy_id = _r33_text(runtime["candidate_policy_id"])
    policy = document_source._policy_by_id(cursor, policy_id)
    direct_policy = document_source._load_direct_candidate_policy(cursor, policy_id)
    kind = {
        "document_insert": "insert",
        "document_delete": "delete",
        "document_replace": "replace",
    }
    if (
        direct["update_kind"] != kind[update["update_kind"]]
        or direct["previous_published_epoch_id"] != predecessor
        or _r33_text(direct["candidate_policy_id"]) != policy_id
        or direct["registry_snapshot_id"] != direct_policy.claim_registry_snapshot_id
        or policy.manifest_hash != _hash(runtime["candidate_policy_manifest_hash"])
        or policy.decision_policy_version != update["decision_policy_version"]
        or direct_policy.decision_policy_version != policy.decision_policy_version
        or direct_policy.verifier_execution_spec_hash
        != policy.verifier_execution_spec_hash
    ):
        raise EventConflictError("readiness retained document policy/action changed")
    if base["publication_mode"] != (
        "strict" if runtime["runtime_state"] == "sealed" else "provisional"
    ):
        raise EventConflictError("readiness document publication shape changed")
    registry = _r33_record(
        cursor,
        schema,
        "groundloop_m4_claim_registry_snapshot",
        "claim_registry_snapshot_id",
        direct["registry_snapshot_id"],
    )
    members = _r33_records(
        cursor,
        sql.SQL(
            "SELECT * FROM {} WHERE claim_registry_snapshot_id "
            "OPERATOR(pg_catalog.=) %s "
            'ORDER BY claim_id COLLATE pg_catalog."C"'
        ).format(sql.Identifier(schema, "groundloop_m4_claim_registry_member")),
        (direct["registry_snapshot_id"],),
    )
    ids = tuple(_r33_text(row["claim_id"]) for row in members)
    if (
        len(ids) != direct_policy.claim_count
        or registry["claim_count"] != len(ids)
        or tuple(row["member_ordinal"] for row in members) != tuple(range(len(ids)))
        or _hash(registry["claim_set_hash"])
        != stable_m4_digest("m4-claim-registry-snapshot-v1", *ids)
    ):
        raise EventConflictError("readiness direct claim registry is not exact")
    payload = _r33_payload(direct["manifest"], update["update_kind"])
    chunks = _r33_withdrawn_chunks(cursor, schema, epoch, predecessor, payload)
    previous = _r33_predecessor(cursor, schema, epoch, predecessor)
    snapshots = _r33_snapshots(cursor, schema, epoch, runtime)
    _r33_original_contribution(cursor, schema, base, runtime)
    _r33_job_declarations(cursor, schema, runtime, policy, *snapshots)
    # Original fallback is as-of P. Never select today's heads/currency on replay.
    for table, subject in (
        ("groundloop_published_observation_currency", "subject_kind='claim' AND "),
        ("groundloop_candidate_frontier", ""),
    ):
        rows = _r33_records(
            cursor,
            sql.SQL(
                "SELECT 1 FROM {} WHERE "
                + subject
                + "chunk_version_id OPERATOR(pg_catalog.=) ANY(%s) "
                "AND valid_from_epoch OPERATOR(pg_catalog.<=) %s "
                "AND (valid_to_epoch IS NULL OR "
                "%s OPERATOR(pg_catalog.<) valid_to_epoch) LIMIT 1"
            ).format(sql.Identifier(schema, table)),
            (list(chunks), predecessor, predecessor),
        )
        if rows:
            raise InvalidEventError("readiness document requires direct fallback work")
    if replay:
        _check_jobs(cursor, schema, runtime, target="semantic_complete")
        return
    heads = cursor.execute(
        sql.SQL(
            "SELECT h4.epoch_id,h5.epoch_id,h5.sealed_revision FROM {} h4 "
            "JOIN {} h5 ON h5.singleton WHERE h4.singleton"
        ).format(
            sql.Identifier(schema, "groundloop_m4_publication_head"),
            sql.Identifier(schema, "groundloop_m5_publication_head"),
        )
    ).fetchone()
    if heads != (predecessor, predecessor, previous["revision"]):
        raise EventConflictError("fresh readiness predecessor heads changed")
    if _r33_records(
        cursor,
        sql.SQL(
            "SELECT 1 FROM {} WHERE subject_kind='claim' "
            "AND chunk_version_id OPERATOR(pg_catalog.=) ANY(%s) LIMIT 1"
        ).format(sql.Identifier(schema, "groundloop_observation_currency")),
        (list(chunks),),
    ):
        raise InvalidEventError("fresh readiness retains direct claim currency")


def _check_declaration(
    cursor: Cursor[Any],
    schema: str,
    base: dict[str, Any],
    runtime: dict[str, Any],
    *,
    document_authority: bool = False,
    replay: bool = False,
) -> None:
    update = _read_row(
        cursor, schema, "groundloop_m5_update", int(base["epoch_id"]), lock=False
    )
    if (
        update["update_kind"] not in (_GROUP_KINDS | _R33_DOCUMENT_KINDS)
        or update["previous_published_epoch_id"]
        != runtime["expected_previous_published_epoch_id"]
    ):
        raise InvalidEventError(
            "readiness is restricted to the exact group declaration"
        )
    policy = cursor.execute(
        sql.SQL(
            "SELECT candidate_policy_manifest_hash,decision_policy_version "
            "FROM {} WHERE candidate_policy_id OPERATOR(pg_catalog.=) %s"
        ).format(sql.Identifier(schema, "groundloop_m5_candidate_policy")),
        (runtime["candidate_policy_id"],),
    ).fetchone()
    if policy is None or (_hash(policy[0]), policy[1]) != (
        _hash(runtime["candidate_policy_manifest_hash"]),
        update["decision_policy_version"],
    ):
        raise ValidationError("readiness candidate declaration is not exact")
    original = cursor.execute(
        sql.SQL(
            "SELECT source_identity_hash,applied_revision FROM {} "
            "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            "AND contribution_kind OPERATOR(pg_catalog.=) 'structural_open' "
            "AND source_id OPERATOR(pg_catalog.=) %s"
        ).format(sql.Identifier(schema, "groundloop_m5_runtime_work_contribution")),
        (base["epoch_id"], runtime["structural_event_id"]),
    ).fetchone()
    if original is None or (_hash(original[0]), original[1]) != (
        _hash(base["payload_hash"]),
        1,
    ):
        raise ValidationError("readiness lacks the immutable structural-open proof")
    if update["update_kind"] in _R33_DOCUMENT_KINDS:
        if not document_authority:
            raise InvalidEventError(
                "readiness requires021 for documents, not the exact group declaration"
            )
        _r33_document_declaration(cursor, schema, base, runtime, update, replay=replay)
        return
    for relation in (
        "groundloop_m4_update",
        "groundloop_m4_evaluation_epoch_counter",
        "groundloop_semantic_job",
        "groundloop_discovery_scope",
        "groundloop_m5_direct_terminal_projection",
    ):
        if (
            cursor.execute(
                sql.SQL(
                    "SELECT 1 FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s LIMIT 1"
                ).format(sql.Identifier(schema, relation)),
                (base["epoch_id"],),
            ).fetchone()
            is not None
        ):
            raise InvalidEventError(
                "readiness cannot infer readiness for a direct surface"
            )


def _rows(
    cursor: Cursor[Any], schema: str, relation: str, epoch_id: int
) -> list[dict[str, Any]]:
    values = cursor.execute(
        sql.SQL("SELECT * FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s").format(
            sql.Identifier(schema, relation)
        ),
        (epoch_id,),
    ).fetchall()
    assert cursor.description is not None
    names = tuple(column.name for column in cursor.description)
    result = [dict(zip(names, row, strict=True)) for row in values]
    if any(row["epoch_id"] != epoch_id for row in result):
        raise ValidationError("readiness detail returned another epoch")
    return result


def _lock_counters(
    cursor: Cursor[Any], schema: str, epoch_id: int, revision: int
) -> tuple[int, int]:
    sizes = []
    for relation, key in (
        ("groundloop_m5_owner_pending_counter", "owner_claim_id"),
        ("groundloop_m5_answer_pending_counter", "answer_version_id"),
    ):
        values = cursor.execute(
            sql.SQL(
                "SELECT updated_revision,pending_multiplicity FROM {} "
                "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                'ORDER BY {} COLLATE pg_catalog."C" FOR UPDATE'
            ).format(sql.Identifier(schema, relation), sql.Identifier(key)),
            (epoch_id,),
        ).fetchall()
        if any(row != (revision, 0) for row in values):
            raise ValidationError("readiness owner/answer counters must be exact zero")
        sizes.append(len(values))
    return sizes[0], sizes[1]


def _check_jobs(
    cursor: Cursor[Any], schema: str, runtime: dict[str, Any], *, target: str
) -> None:
    """Independent epoch-local eligibility, separate from the BEFORE guard."""
    epoch = int(runtime["epoch_id"])
    revision = int(runtime["revision"])
    jobs = _rows(cursor, schema, "groundloop_m5_semantic_job", epoch)
    scopes = _rows(cursor, schema, "groundloop_m5_discovery_scope", epoch)
    roots = [job for job in jobs if job["parent_job_id"] is None]
    if digests.requirement_root_set_digest(
        str(job["logical_job_id"]).rstrip(" ") for job in roots
    ) != _hash(runtime["requirement_root_set_hash"]):
        raise ValidationError("readiness root-set declaration is not exact")
    if target == "semantic_pending" and (jobs or scopes):
        raise InvalidEventError(
            "readiness start requires a genuinely job-free root set"
        )
    scope_by_root = {scope["root_job_id"]: scope for scope in scopes}
    if len(scope_by_root) != len(scopes) or set(scope_by_root) != {
        job["logical_job_id"] for job in roots
    }:
        raise ValidationError("readiness scopes are not the exact declared root set")
    cancellation_groups: dict[tuple[str, int], list[str]] = defaultdict(list)
    for scope in scopes:
        if (
            scope["scope_state"]
            not in {"closed_active", "closed_inactive", "cancelled"}
            or scope["closed_revision"] is None
            or int(scope["closed_revision"]) > revision
            or scope["completion_digest"] is None
        ):
            raise InvalidEventError("readiness retains an unresolved/failed scope")
    for job in jobs:
        state = job["job_state"]
        if (
            state not in {"completed_active", "completed_inactive", "cancelled"}
            or job["completed_revision"] is None
            or int(job["completed_revision"]) > revision
            or job["completion_digest"] is None
        ):
            raise InvalidEventError("readiness retains an unresolved/failed job")
        if state == "cancelled":
            reason = job["cancellation_reason"]
            if (
                reason not in {"subject_inactive", "scope_retired"}
                or reason != job["archive_reason"]
                or job["cancelled_by_event_id"] != runtime["structural_event_id"]
                or job["cancelled_by_epoch_id"] != epoch
            ):
                raise ValidationError(
                    "readiness cancellation coordinates are not exact"
                )
            cancellation_groups[(str(reason), int(job["completed_revision"]))].append(
                _hash(job["logical_job_id"])
            )
        if job["parent_job_id"] is not None:
            continue
        root_scope = scope_by_root.get(job["logical_job_id"])
        scope_state = {
            "completed_active": "closed_active",
            "completed_inactive": "closed_inactive",
            "cancelled": "cancelled",
        }[state]
        if (
            root_scope is None
            or root_scope["scope_state"] != scope_state
            or root_scope["closed_revision"] != job["completed_revision"]
            or root_scope["completion_digest"] != job["completion_digest"]
            or root_scope["scope_contract_digest"] != job["scope_contract_digest"]
        ):
            raise ValidationError("readiness root lacks its genuine terminal scope")
        if state != "cancelled":
            if (
                job["scope_closure_digest"] is None
                or job["child_set_hash"] is None
                or root_scope["scope_closure_digest"] != job["scope_closure_digest"]
                or root_scope["child_set_hash"] != job["child_set_hash"]
            ):
                raise ValidationError("readiness root closure/child set is not exact")
            if (
                cursor.execute(
                    sql.SQL(
                        "SELECT 1 FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                        "AND contribution_kind OPERATOR(pg_catalog.=) 'root_barrier' "
                        "AND source_id OPERATOR(pg_catalog.=) %s "
                        "AND applied_revision OPERATOR(pg_catalog.=) %s"
                    ).format(
                        sql.Identifier(
                            schema, "groundloop_m5_runtime_work_contribution"
                        )
                    ),
                    (epoch, runtime["structural_event_id"], job["completed_revision"]),
                ).fetchone()
                is None
            ):
                raise ValidationError("readiness root lacks a durable barrier artifact")
    for (reason, completed), identifiers in cancellation_groups.items():
        identity = digests.cancellation_plan_digest(
            structural_event_id=str(runtime["structural_event_id"]),
            epoch_id=epoch,
            cancelled_job_ids=tuple(sorted(identifiers)),
            reason=M5TerminalReason(reason),
        )
        record = cursor.execute(
            sql.SQL(
                "SELECT source_identity_hash,requirement_cancelled_job_count "
                "FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                "AND contribution_kind OPERATOR(pg_catalog.=) 'cancellation' "
                "AND source_id OPERATOR(pg_catalog.=) %s "
                "AND applied_revision OPERATOR(pg_catalog.=) %s"
            ).format(sql.Identifier(schema, "groundloop_m5_runtime_work_contribution")),
            (epoch, identity, completed),
        ).fetchone()
        if record is None or (_hash(record[0]), record[1]) != (
            identity,
            len(identifiers),
        ):
            raise ValidationError(
                "readiness cancellation group lacks its exact artifact"
            )


def _read_contribution(
    cursor: Cursor[Any], schema: str, epoch_id: int, target: str
) -> dict[str, Any] | None:
    record = cursor.execute(
        sql.SQL(
            "SELECT * FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            "AND contribution_kind OPERATOR(pg_catalog.=) %s "
            "AND source_id OPERATOR(pg_catalog.=) %s"
        ).format(sql.Identifier(schema, "groundloop_m5_runtime_work_contribution")),
        (epoch_id, _KIND.value, target),
    ).fetchone()
    if record is None:
        return None
    assert cursor.description is not None
    result = dict(
        zip((column.name for column in cursor.description), record, strict=True)
    )
    if (result["epoch_id"], result["contribution_kind"], result["source_id"]) != (
        epoch_id,
        _KIND.value,
        target,
    ):
        raise ValidationError("readiness contribution returned another key")
    return result


def _validate_replay(
    row: dict[str, Any], base: dict[str, Any], artifact: _ReadinessArtifact
) -> _ReadinessReceipt:
    stored = M5RuntimeWork(
        **{name: int(row[name]) for name in _WORK_COLUMNS},
        work_digest=_hash(row["work_digest"]),
    )
    if (
        (row["epoch_id"], row["contribution_kind"], row["source_id"])
        != (artifact.anchor.epoch_id, _KIND.value, artifact.anchor.source_id)
        or base["epoch_id"] != artifact.anchor.epoch_id
        or row["applied_revision"] != artifact.anchor.anchor_revision
        or int(base["revision"]) < artifact.anchor.anchor_revision
        or _hash(row["source_identity_hash"]) != artifact.source_identity_hash
        or _hash(row["contribution_key_digest"])
        != artifact.anchor.contribution_key_digest
        or stored != artifact.work
    ):
        raise EventConflictError(
            "readiness replay differs from its original coordinates"
        )
    return _ReadinessReceipt(artifact.anchor, True)


def _lock_accounting(
    cursor: Cursor[Any], schema: str, epoch_id: int, revision: int
) -> tuple[M5RuntimeWork, _TimingAccumulator]:
    work_row = _read_row(
        cursor, schema, "groundloop_m5_runtime_work_accumulator", epoch_id, lock=True
    )
    if (
        work_row["updated_revision"] != revision
        or work_row["terminalized"] is not False
    ):
        raise ValidationError("readiness work point is not at its predecessor")
    work = M5RuntimeWork(
        **{name: int(work_row[name]) for name in _WORK_COLUMNS},
        work_digest=_hash(work_row["work_digest"]),
    )
    timing_row = _read_row(
        cursor, schema, "groundloop_m5_runtime_timing_accumulator", epoch_id, lock=True
    )
    timing = _timing_accumulator_from_row(
        tuple(
            timing_row[name]
            for name in (*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS)
        )
        + tuple(
            timing_row[name]
            for name in (
                "updated_revision",
                "terminalized",
                "pending_contribution_kind",
                "pending_source_id",
                "pending_contribution_key_digest",
                "pending_anchor_revision",
            )
        )
    )
    if timing.updated_revision != revision or timing.terminalized:
        raise ValidationError("readiness timing point is not at its predecessor")
    timing.project(pending_as_missing=True)
    if timing.has_pending_anchor:
        if timing.pending_contribution_kind not in _PENDING_KINDS:
            raise ValidationError("readiness prior anchor is not a nonterminal kind")
        pending = cursor.execute(
            sql.SQL(
                "SELECT contribution_key_digest,applied_revision FROM {} "
                "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                "AND contribution_kind OPERATOR(pg_catalog.=) %s "
                "AND source_id OPERATOR(pg_catalog.=) %s"
            ).format(sql.Identifier(schema, "groundloop_m5_runtime_work_contribution")),
            (epoch_id, timing.pending_contribution_kind, timing.pending_source_id),
        ).fetchone()
        if pending is None or (_hash(pending[0]), pending[1]) != (
            timing.pending_contribution_key_digest,
            timing.pending_anchor_revision,
        ):
            raise ValidationError("readiness prior pending point is not immutable")
        assert timing.pending_anchor_revision is not None
        if timing.pending_anchor_revision > revision:
            raise ValidationError("readiness prior pending point is in the future")
    return work, timing


def _read_header_pair(
    cursor: Cursor[Any], schema: str, epoch_id: int
) -> tuple[dict[str, Any], dict[str, Any]]:
    """One statement snapshot for zero-write historical replay."""
    base_fields = (
        "epoch_id",
        "event_id",
        "payload_hash",
        "revision",
        "structural_status",
        "semantic_status",
        "evaluation_state",
        "publication_mode",
        "sealed_at",
    )
    runtime_fields = (
        "epoch_id",
        "structural_event_id",
        "revision",
        "runtime_state",
        "open_work_count",
        "open_scope_count",
        "blocking_failure_count",
        "terminal_at",
        "candidate_policy_id",
        "candidate_policy_manifest_hash",
        "requirement_registry_snapshot_digest",
        "active_chunk_snapshot_digest",
        "expected_previous_published_epoch_id",
        "requirement_root_set_hash",
    )
    header_columns = [
        sql.SQL("{}.{}").format(sql.Identifier(alias), sql.Identifier(name))
        for alias, fields in (("b", base_fields), ("r", runtime_fields))
        for name in fields
    ]
    counter_columns = [
        sql.SQL("c.{}").format(sql.Identifier(name)) for name in _R33_COUNTER_FIELDS
    ]
    seal_columns = [
        sql.SQL("s.{}").format(sql.Identifier(name)) for name in _R33_SEAL_FIELDS
    ]
    history = sql.SQL(" OR ").join(
        sql.SQL(
            "EXISTS (SELECT 1 FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) b.epoch_id)"
        ).format(sql.Identifier(schema, table))
        for table in _R33_DIRECT_HISTORY
    )
    counter_bad = [
        sql.SQL(
            "EXISTS (SELECT 1 FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) b.epoch_id "
            "AND (updated_revision OPERATOR(pg_catalog.<>) r.revision "
            "OR pending_multiplicity OPERATOR(pg_catalog.<>) 0))"
        ).format(sql.Identifier(schema, table))
        for table in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        )
    ]
    columns = sql.SQL(",").join(
        [
            *header_columns,
            *counter_columns,
            *seal_columns,
            history,
            sql.SQL(
                "(SELECT pg_catalog.count(*) FROM {} "
                "WHERE epoch_id OPERATOR(pg_catalog.=) b.epoch_id)"
            ).format(
                sql.Identifier(schema, "groundloop_m4_evaluation_counter_transition")
            ),
            *counter_bad,
        ]
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {} FROM {} b JOIN {} r "
            "ON r.epoch_id OPERATOR(pg_catalog.=) b.epoch_id "
            "LEFT JOIN {} c ON c.epoch_id OPERATOR(pg_catalog.=) b.epoch_id "
            "LEFT JOIN {} s ON s.epoch_id OPERATOR(pg_catalog.=) b.epoch_id "
            "AND s.transition_kind OPERATOR(pg_catalog.=) 'seal' "
            "WHERE b.epoch_id OPERATOR(pg_catalog.=) %s"
        ).format(
            columns,
            sql.Identifier(schema, "groundloop_epoch"),
            sql.Identifier(schema, "groundloop_m5_runtime_epoch"),
            sql.Identifier(schema, "groundloop_m4_evaluation_epoch_counter"),
            sql.Identifier(schema, "groundloop_m4_evaluation_counter_transition"),
        ),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("readiness requires an existing typed epoch")
    boundary = len(base_fields)
    if row[0] != epoch_id or row[boundary] != epoch_id:
        raise ValidationError("readiness replay headers returned another epoch")
    end = boundary + len(runtime_fields)
    counter_end = end + len(_R33_COUNTER_FIELDS)
    seal_end = counter_end + len(_R33_SEAL_FIELDS)
    runtime = dict(zip(runtime_fields, row[boundary:end], strict=True))
    runtime["_r33_document_point"] = {
        "counter": dict(zip(_R33_COUNTER_FIELDS, row[end:counter_end], strict=True)),
        "seal": dict(zip(_R33_SEAL_FIELDS, row[counter_end:seal_end], strict=True)),
        "direct_history": row[seal_end],
        "transition_count": row[seal_end + 1],
        "owner_counter_bad": row[seal_end + 2],
        "answer_counter_bad": row[seal_end + 3],
    }
    return dict(zip(base_fields, row[:boundary], strict=True)), runtime


def _derive_artifact(
    base: dict[str, Any], runtime: dict[str, Any], *, target: str, revision: int
) -> _ReadinessArtifact:
    return _readiness_artifact(
        epoch_id=int(base["epoch_id"]),
        structural_event_id=str(runtime["structural_event_id"]),
        event_payload_hash=_hash(base["payload_hash"]),
        requirement_root_set_hash=_hash(runtime["requirement_root_set_hash"]),
        target=target,
        expected_revision=revision,
    )


def _resolve_prior_pending(
    cursor: Cursor[Any], schema: str, epoch_id: int, timing: _TimingAccumulator
) -> None:
    if not timing.has_pending_anchor:
        return
    assert timing.pending_contribution_kind is not None
    assert timing.pending_source_id is not None
    assert timing.pending_contribution_key_digest is not None
    assert timing.pending_anchor_revision is not None
    observation = M5RuntimeTimingObservation.build(None)
    digest = digests.transition_call_timing_digest(
        epoch_id=epoch_id,
        contribution_kind=timing.pending_contribution_kind,
        source_id=timing.pending_source_id,
        contribution_key_digest=timing.pending_contribution_key_digest,
        anchor_revision=timing.pending_anchor_revision,
        observation_digest=observation.observation_digest,
    )
    cursor.execute(
        sql.SQL(
            "INSERT INTO {} (epoch_id,contribution_kind,source_id,"
            "contribution_key_digest,anchor_revision,required_interval_observed,"
            "coordinator_non_db_non_neural_ns,neural_wall_ns,postgres_roundtrip_wall_ns,"
            "external_io_wall_ns,end_to_end_wall_ns,postgres_server_execution_ns,"
            "postgres_lock_wait_ns,postgres_wal_bytes,postgres_shared_block_reads,"
            "observation_digest,transition_timing_digest) VALUES "
            "(%s,%s,%s,%s,%s,false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,%s,%s)"
        ).format(sql.Identifier(schema, "groundloop_m5_transition_call_timing")),
        (
            epoch_id,
            timing.pending_contribution_kind,
            timing.pending_source_id,
            timing.pending_contribution_key_digest,
            timing.pending_anchor_revision,
            observation.observation_digest,
            digest,
        ),
    )


def _insert_artifact(
    cursor: Cursor[Any], schema: str, artifact: _ReadinessArtifact
) -> None:
    names = (
        "epoch_id",
        "contribution_kind",
        "source_id",
        "source_identity_hash",
        "contribution_key_digest",
        "applied_revision",
        *_WORK_COLUMNS,
        "work_digest",
    )
    cursor.execute(
        sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(schema, "groundloop_m5_runtime_work_contribution"),
            sql.SQL(",").join(map(sql.Identifier, names)),
            sql.SQL(",").join(sql.Placeholder() for _ in names),
        ),
        (
            artifact.anchor.epoch_id,
            _KIND.value,
            artifact.anchor.source_id,
            artifact.source_identity_hash,
            artifact.anchor.contribution_key_digest,
            artifact.anchor.anchor_revision,
            *artifact.work.counter_values(),
            artifact.work.work_digest,
        ),
    )


def _finish_accounting(
    cursor: Cursor[Any],
    schema: str,
    artifact: _ReadinessArtifact,
    prior_work: M5RuntimeWork,
    timing: _TimingAccumulator,
    *,
    revision: int,
    cut: Callable[[str], None],
) -> None:
    counters: dict[str, Any] = dict(
        zip(
            _WORK_COLUMNS,
            (
                a + b
                for a, b in zip(
                    prior_work.counter_values(),
                    artifact.work.counter_values(),
                    strict=True,
                )
            ),
            strict=True,
        )
    )
    total = M5RuntimeWork(**counters)
    assignments = sql.SQL(",").join(
        sql.SQL("{}=%s").format(sql.Identifier(name)) for name in _WORK_COLUMNS
    )
    changed = cursor.execute(
        sql.SQL(
            "UPDATE {} SET {},work_digest=%s,updated_revision=%s,"
            "updated_at=pg_catalog.clock_timestamp() "
            "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            "AND updated_revision OPERATOR(pg_catalog.=) %s "
            "AND work_digest OPERATOR(pg_catalog.=) %s AND NOT terminalized"
        ).format(
            sql.Identifier(schema, "groundloop_m5_runtime_work_accumulator"),
            assignments,
        ),
        (
            *total.counter_values(),
            total.work_digest,
            artifact.anchor.anchor_revision,
            artifact.anchor.epoch_id,
            revision,
            prior_work.work_digest,
        ),
    ).rowcount
    if changed != 1:
        raise ValidationError("readiness work CAS lost its predecessor")
    cut("after_work_cas")
    changes: list[sql.Composable] = []
    for prefix in (
        "required",
        "postgres_server_execution",
        "postgres_lock_wait",
        "postgres_wal_bytes",
        "postgres_shared_block_reads",
    ):
        changes.extend(
            (
                sql.SQL("{}={} OPERATOR(pg_catalog.+) 1").format(
                    sql.Identifier(prefix + "_expected_count"),
                    sql.Identifier(prefix + "_expected_count"),
                ),
                sql.SQL("{}={} OPERATOR(pg_catalog.+) %s").format(
                    sql.Identifier(prefix + "_missing_count"),
                    sql.Identifier(prefix + "_missing_count"),
                ),
            )
        )
    changed = cursor.execute(
        sql.SQL(
            "UPDATE {} SET {},pending_contribution_kind=%s,pending_source_id=%s,"
            "pending_contribution_key_digest=%s,pending_anchor_revision=%s,"
            "updated_revision=%s,updated_at=pg_catalog.clock_timestamp() "
            "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            "AND updated_revision OPERATOR(pg_catalog.=) %s AND NOT terminalized"
        ).format(
            sql.Identifier(schema, "groundloop_m5_runtime_timing_accumulator"),
            sql.SQL(",").join(changes),
        ),
        (
            *([int(timing.has_pending_anchor)] * 5),
            _KIND.value,
            artifact.anchor.source_id,
            artifact.anchor.contribution_key_digest,
            artifact.anchor.anchor_revision,
            artifact.anchor.anchor_revision,
            artifact.anchor.epoch_id,
            revision,
        ),
    ).rowcount
    if changed != 1:
        raise ValidationError("readiness timing CAS lost its predecessor")
    cut("after_timing_cas")


def _advance_semantic_readiness_atomically(
    connection: Connection[Any],
    *,
    epoch_id: int,
    structural_event_id: str,
    target: str,
    expected_revision: int,
    failure_injector: Callable[[str], None] | None = None,
) -> _ReadinessReceipt:
    """One D32 edge, or immutable original-coordinate zero-write replay."""
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise InvalidEventError(
            "readiness requires an idle connection and outer transaction"
        )
    if connection.read_only is True or connection.isolation_level not in (
        None,
        IsolationLevel.READ_COMMITTED,
    ):
        raise InvalidEventError("readiness requires read-write READ COMMITTED")
    if type(epoch_id) is not int or epoch_id <= 0:
        raise ValidationError("readiness epoch must be a positive integer")
    if type(expected_revision) is not int or expected_revision <= 0:
        raise ValidationError("readiness revision must be a positive integer")
    if type(structural_event_id) is not str or not structural_event_id:
        raise ValidationError("readiness event identity must be nonempty text")
    if type(target) is not str or target not in _PREDECESSORS:
        raise ValidationError("readiness must name one of the two exact edges")

    def cut(point: str) -> None:
        if failure_injector is not None:
            failure_injector(point)

    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE")
        with connection.cursor() as cursor:
            schema = _m5_readiness_schema(cursor)
            document_authority = _r33_require_authority(cursor, schema)
            _require_legacy_lookup_context(cursor, schema)
            cut("after_authority")
            existing = _read_contribution(cursor, schema, epoch_id, target)
            if existing is not None:
                base, runtime = _read_header_pair(cursor, schema, epoch_id)
                _check_epoch_identity(
                    base, runtime, structural_event_id=structural_event_id
                )
                _check_declaration(
                    cursor,
                    schema,
                    base,
                    runtime,
                    document_authority=document_authority,
                    replay=True,
                )
                cut("after_replay_read")
                return _validate_replay(
                    existing,
                    base,
                    _derive_artifact(
                        base, runtime, target=target, revision=expected_revision
                    ),
                )
            base = _read_row(cursor, schema, "groundloop_epoch", epoch_id, lock=True)
            cut("after_base_lock")
            runtime = _read_row(
                cursor, schema, "groundloop_m5_runtime_epoch", epoch_id, lock=True
            )
            cut("after_runtime_lock")
            _check_epoch_identity(
                base, runtime, structural_event_id=structural_event_id
            )
            existing = _read_contribution(cursor, schema, epoch_id, target)
            point_base, point_runtime = _read_header_pair(cursor, schema, epoch_id)
            if any(base[name] != value for name, value in point_base.items()) or any(
                runtime[name] != value
                for name, value in point_runtime.items()
                if name != "_r33_document_point"
            ):
                raise EventConflictError("locked readiness headers changed")
            runtime["_r33_document_point"] = point_runtime["_r33_document_point"]
            _check_declaration(
                cursor,
                schema,
                base,
                runtime,
                document_authority=document_authority,
                replay=existing is not None,
            )
            if existing is not None:
                return _validate_replay(
                    existing,
                    base,
                    _derive_artifact(
                        base, runtime, target=target, revision=expected_revision
                    ),
                )
            if (
                int(runtime["revision"]) != expected_revision
                or runtime["runtime_state"] != _PREDECESSORS[target]
                or any(
                    runtime[name] != 0
                    for name in (
                        "open_work_count",
                        "open_scope_count",
                        "blocking_failure_count",
                    )
                )
            ):
                raise InvalidEventError(
                    "readiness predecessor/state/counts are not eligible"
                )
            sizes = _lock_counters(cursor, schema, epoch_id, expected_revision)
            cut("after_counter_locks")
            prior_work, timing = _lock_accounting(
                cursor, schema, epoch_id, expected_revision
            )
            cut("after_accounting_locks")
            _check_jobs(cursor, schema, runtime, target=target)
            artifact = _derive_artifact(
                base, runtime, target=target, revision=expected_revision
            )
            _resolve_prior_pending(cursor, schema, epoch_id, timing)
            cut("after_pending_resolved")
            _insert_artifact(cursor, schema, artifact)
            cut("after_contribution")
            cursor.execute(
                sql.SQL("SELECT {}(%s,%s)").format(
                    sql.Identifier(schema, "groundloop_m5_authorize_checked_transition")
                ),
                (epoch_id, expected_revision),
            )
            cut("after_authorized_transition")
            status = "complete" if target == "semantic_complete" else "pending"
            changed = cursor.execute(
                sql.SQL(
                    "UPDATE {} SET revision=%s,semantic_status=%s,evaluation_state=%s "
                    "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                    "AND revision OPERATOR(pg_catalog.=) %s"
                ).format(sql.Identifier(schema, "groundloop_epoch")),
                (expected_revision + 1, status, status, epoch_id, expected_revision),
            ).rowcount
            if changed != 1:
                raise ValidationError("readiness base revision CAS failed")
            cut("after_base_advance")
            changed = cursor.execute(
                sql.SQL(
                    "UPDATE {} SET revision=%s,runtime_state=%s "
                    "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                    "AND revision OPERATOR(pg_catalog.=) %s "
                    "AND runtime_state OPERATOR(pg_catalog.=) %s"
                ).format(sql.Identifier(schema, "groundloop_m5_runtime_epoch")),
                (
                    expected_revision + 1,
                    target,
                    epoch_id,
                    expected_revision,
                    _PREDECESSORS[target],
                ),
            ).rowcount
            if changed != 1:
                raise ValidationError("readiness runtime revision CAS failed")
            cut("after_runtime_advance")
            for (relation, boundary), size in zip(
                (
                    ("groundloop_m5_owner_pending_counter", "owner"),
                    ("groundloop_m5_answer_pending_counter", "answer"),
                ),
                sizes,
                strict=True,
            ):
                changed = cursor.execute(
                    sql.SQL(
                        "UPDATE {} SET updated_revision=%s "
                        "WHERE epoch_id OPERATOR(pg_catalog.=) %s "
                        "AND updated_revision OPERATOR(pg_catalog.=) %s"
                    ).format(sql.Identifier(schema, relation)),
                    (expected_revision + 1, epoch_id, expected_revision),
                ).rowcount
                if changed != size:
                    raise ValidationError("readiness counter revision binding changed")
                cut("after_" + boundary + "_advance")
            _finish_accounting(
                cursor,
                schema,
                artifact,
                prior_work,
                timing,
                revision=expected_revision,
                cut=cut,
            )
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cut("after_deferred_validation")
            return _ReadinessReceipt(artifact.anchor, False)
