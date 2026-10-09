"""Package-private D32 readiness coordination, not a public/seal route.

The two edges own separate transactions. Semantic observations, D25 images,
matching contributions, publication heads and runtime mode are never changed.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg import Connection, Cursor, IsolationLevel, sql
from psycopg.pq import TransactionStatus

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.digests import enum_field, int_field, text_field
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
    M5TransitionTimingAnchor,
)
from groundloop.m5.runtime.postgres_recovery import (
    _TIMING_COVERAGE_COLUMNS,
    _TIMING_SUM_COLUMNS,
    _timing_accumulator_from_row,
    _TimingAccumulator,
)
from groundloop.postgres.migrations import (
    _m5_readiness_schema,
    require_m5_semantic_readiness_bundle,
)

_WORK_COLUMNS = M5RuntimeWork.counter_names()
_KIND = M5RuntimeWorkContributionKind.SEMANTIC_READINESS
_PREDECESSORS = {
    "semantic_pending": "structural_committed",
    "semantic_complete": "semantic_pending",
}
_GROUP_KINDS = frozenset({"register_group", "replace_group", "retire_group"})
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


def _check_declaration(
    cursor: Cursor[Any], schema: str, base: dict[str, Any], runtime: dict[str, Any]
) -> None:
    update = _read_row(
        cursor, schema, "groundloop_m5_update", int(base["epoch_id"]), lock=False
    )
    if (
        update["update_kind"] not in _GROUP_KINDS
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
    columns = sql.SQL(",").join(
        sql.SQL("{}.{}").format(sql.Identifier(alias), sql.Identifier(name))
        for alias, fields in (("b", base_fields), ("r", runtime_fields))
        for name in fields
    )
    row = cursor.execute(
        sql.SQL(
            "SELECT {} FROM {} b JOIN {} r "
            "ON r.epoch_id OPERATOR(pg_catalog.=) b.epoch_id "
            "WHERE b.epoch_id OPERATOR(pg_catalog.=) %s"
        ).format(
            columns,
            sql.Identifier(schema, "groundloop_epoch"),
            sql.Identifier(schema, "groundloop_m5_runtime_epoch"),
        ),
        (epoch_id,),
    ).fetchone()
    if row is None:
        raise InvalidEventError("readiness requires an existing typed epoch")
    boundary = len(base_fields)
    if row[0] != epoch_id or row[boundary] != epoch_id:
        raise ValidationError("readiness replay headers returned another epoch")
    return (
        dict(zip(base_fields, row[:boundary], strict=True)),
        dict(zip(runtime_fields, row[boundary:], strict=True)),
    )


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
            require_m5_semantic_readiness_bundle(cursor, schema_name=schema)
            _require_legacy_lookup_context(cursor, schema)
            cut("after_authority")
            existing = _read_contribution(cursor, schema, epoch_id, target)
            if existing is not None:
                base, runtime = _read_header_pair(cursor, schema, epoch_id)
                _check_epoch_identity(
                    base, runtime, structural_event_id=structural_event_id
                )
                _check_declaration(cursor, schema, base, runtime)
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
            _check_declaration(cursor, schema, base, runtime)
            existing = _read_contribution(cursor, schema, epoch_id, target)
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
