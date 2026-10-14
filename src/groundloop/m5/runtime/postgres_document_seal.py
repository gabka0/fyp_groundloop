"""Private D33 cursor-local seal staging; no outer transaction or public route.

The caller owns the combined M5 seal, accounting, result and cache adoption.
Only genuine no-direct-history documents use the explicit M5 N / M4 1 clocks.
The legacy M4 seal wrapper and all its other publication primitives stay exact.
"""

from __future__ import annotations

from typing import Any

from psycopg import Cursor, sql
from psycopg.pq import TransactionStatus

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.m4.application import PublicationReceipt
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind, stable_m4_digest
from groundloop.m4.evaluation_overlay import (
    EvaluationTransition,
    EvaluationTransitionKind,
    _declaration_hash,
    _sha256_json,
    _transition_payload,
)
from groundloop.m4.pipeline import PostgresM4ApplicationPorts, StructuralPayload
from groundloop.m5.runtime import postgres_matching
from groundloop.m5.runtime import postgres_readiness as readiness
from groundloop.postgres.migrations import (
    _m5_readiness_schema,
    require_m5_document_update_completion_bundle,
)
from groundloop.repository import InMemoryRepository


def _require_cursor_context(
    ports: PostgresM4ApplicationPorts, cursor: Cursor[Any]
) -> None:
    if type(ports) is not PostgresM4ApplicationPorts or not ports._measured:
        raise ValidationError("job-free seal requires genuine measured M4 ports")
    connection = cursor.connection
    if (
        connection is not ports.connection
        or ports.runtime_store._connection is not connection
        or ports.evaluation_store._connection is not connection
    ):
        raise ValidationError("job-free seal cursor/store connection differs")
    if connection.info.transaction_status is not TransactionStatus.INTRANS:
        raise InvalidEventError(
            "job-free seal requires the caller's active transaction"
        )
    if cursor.execute("SHOW transaction_isolation").fetchone() != ("read committed",):
        raise InvalidEventError("job-free seal requires READ COMMITTED")
    if cursor.execute("SHOW transaction_read_only").fetchone() != ("off",):
        raise InvalidEventError("job-free seal requires a read-write transaction")


def _check_complete_point(
    base: dict[str, Any],
    runtime: dict[str, Any],
    *,
    epoch_id: int,
    expected_revision: int,
    update: CorpusUpdateIdentity,
) -> None:
    readiness._check_epoch_identity(base, runtime, structural_event_id=update.event_id)
    if (
        base["epoch_id"] != epoch_id
        or base["payload_hash"].strip() != update.payload_hash
        or base["revision"] != expected_revision
        or expected_revision < 3
        or runtime["runtime_state"] != "semantic_complete"
        or base["publication_mode"] != "provisional"
        or runtime["expected_previous_published_epoch_id"]
        != update.previous_published_epoch_id
        or runtime["candidate_policy_id"] != update.candidate_policy_id
        or any(
            runtime[name] != 0
            for name in (
                "open_work_count",
                "open_scope_count",
                "blocking_failure_count",
            )
        )
    ):
        raise InvalidEventError("job-free seal requires the exact complete M5 N point")


def _prior_committed_xmin(cursor: Cursor[Any], row: Any) -> bool:
    """Distinguish a visible prior row from own top/subtransaction insertion.

    Use actual full top XID, never age()'s potentially earlier cached reference.
    The signed32 lift is the backend's nearby-XID operation. A visible older
    XID cannot be own; newer candidates must be below snapshot xmax and actually
    committed. xmax is latestCompleted+1, NOT the next allocated XID. Own current
    and released/subcommitted children remain in progress to pg_xact_status.

    Ambiguous ancient frozen raw-XID aliases fail closed; this is not a promise
    of arbitrary frozen-row liveness, nor a new persisted identity/attestation.
    """
    if (
        type(row) is not tuple
        or len(row) != 3
        or any(
            type(value) is not str or not value.isascii() or not value.isdecimal()
            for value in row
        )
    ):
        return False
    xmin, top, xmax = (int(value) for value in row)
    if not (0 < xmin < 1 << 32 and 3 <= top < 1 << 64 and 3 <= xmax < 1 << 64):
        return False
    if xmin < 3:  # Visible permanent/frozen XIDs are never current insertions.
        return True
    delta = ((xmin - (top & ((1 << 32) - 1)) + (1 << 31)) % (1 << 32)) - (1 << 31)
    full = top + delta
    if full < top:
        return True
    if full >= xmax:
        return False
    return cursor.execute(
        "SELECT pg_catalog.pg_xact_status(%s::pg_catalog.xid8)", (str(full),)
    ).fetchone() == ("committed",)


def _require_committed_readiness(
    cursor: Cursor[Any], schema: str, base: dict[str, Any], runtime: dict[str, Any]
) -> None:
    epoch = int(base["epoch_id"])
    revision = int(base["revision"])
    contribution = readiness._read_contribution(
        cursor, schema, epoch, "semantic_complete"
    )
    if contribution is None:
        raise InvalidEventError("job-free seal lacks genuine complete readiness")
    readiness._validate_replay(
        contribution,
        base,
        readiness._derive_artifact(
            base, runtime, target="semantic_complete", revision=revision - 1
        ),
    )
    # The complete edge must belong to an earlier committed outer transaction.
    # This read neither assigns a transaction ID nor creates an attestation.
    visibility = cursor.execute(
        sql.SQL(
            "SELECT xmin::pg_catalog.text, "
            "pg_catalog.pg_current_xact_id_if_assigned()::pg_catalog.text, "
            "pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())"
            "::pg_catalog.text "
            "FROM {} WHERE epoch_id OPERATOR(pg_catalog.=) %s "
            "AND contribution_kind='semantic_readiness' "
            "AND source_id='semantic_complete'"
        ).format(sql.Identifier(schema, "groundloop_m5_runtime_work_contribution")),
        (epoch,),
    ).fetchone()
    if not _prior_committed_xmin(cursor, visibility):
        raise InvalidEventError("job-free seal requires separately committed readiness")


def _check_consumed_cache(
    cursor: Cursor[Any],
    ports: PostgresM4ApplicationPorts,
    *,
    epoch_id: int,
    payload: StructuralPayload,
    decision_policy_version: str,
) -> None:
    """Compare bounded consumed values, not cache origin or whole-state equality.

    Native OPEN retains inactive objects; native reconnect hydration omits them.
    Both are valid. No hydration, cache mutation or full oracle runs here.
    """
    repository, engine = ports._working_repository, ports._working_engine
    if (
        ports._active_epoch_id != epoch_id
        or type(repository) is not InMemoryRepository
        or type(engine) is not IncrementalMaintenanceEngine
        or repository.current_policy()
        != postgres_matching._document_direct_policy(cursor, decision_policy_version)
    ):
        raise EventConflictError("job-free seal requires the adopted working cache")
    old = payload.deactivated_document_version_id
    if old is not None:
        row = cursor.execute(
            "SELECT document_id,btrim(content_hash) FROM groundloop_document_version "
            "WHERE document_version_id=%s",
            (old,),
        ).fetchone()
        if row is None or repository.active_document_version(str(row[0])) == old:
            raise EventConflictError(
                "job-free seal cache retains the withdrawn version"
            )
        if repository.has_document_version(old):
            version = repository.document_version(old)
            if (version.document_id, version.content_hash) != tuple(row):
                raise EventConflictError("job-free seal cached version source changed")
        for chunk_id, index, text, text_hash in cursor.execute(
            "SELECT chunk_version_id,chunk_index,text,btrim(text_hash) "
            "FROM groundloop_chunk_version WHERE document_version_id=%s "
            'ORDER BY chunk_version_id COLLATE "C"',
            (old,),
        ).fetchall():
            if repository.is_chunk_active(chunk_id):
                raise EventConflictError(
                    "job-free seal cache retains a withdrawn chunk"
                )
            if repository.has_chunk_version(chunk_id):
                chunk = repository.chunk_version(chunk_id)
                if (
                    chunk.document_version_id,
                    chunk.chunk_index,
                    chunk.text,
                    chunk.text_hash,
                ) != (old, index, text, text_hash):
                    raise EventConflictError(
                        "job-free seal cached chunk source changed"
                    )
    if payload.inserted is not None:
        inserted = payload.inserted
        if (
            not repository.has_document_version(inserted.version.document_version_id)
            or repository.document_version(inserted.version.document_version_id)
            != inserted.version
            or repository.active_document_version(inserted.version.document_id)
            != inserted.version.document_version_id
            or tuple(
                sorted(
                    repository.chunk_ids_of_document_version(
                        inserted.version.document_version_id
                    )
                )
            )
            != payload.inserted_chunk_ids
        ):
            raise EventConflictError("job-free seal inserted cache source changed")
        for chunk in inserted.chunks:
            if (
                not repository.has_chunk_version(chunk.chunk_version_id)
                or repository.chunk_version(chunk.chunk_version_id) != chunk
                or not repository.is_chunk_active(chunk.chunk_version_id)
            ):
                raise EventConflictError("job-free seal inserted chunk cache changed")
    for row in cursor.execute(
        "SELECT claim_id,support_count,refute_count,best_support_score,"
        "best_refute_score,supporting_observation_ids,refuting_observation_ids,status "
        "FROM groundloop_m4_working_claim_state WHERE epoch_id=%s ORDER BY claim_id",
        (epoch_id,),
    ).fetchall():
        try:
            state = engine.claim_state(str(row[0]))
        except KeyError as error:
            raise EventConflictError(
                "job-free seal consumed claim cache is absent"
            ) from error
        if tuple(row) != (
            state.claim_id,
            state.support_count,
            state.refute_count,
            state.best_support_score,
            state.best_refute_score,
            list(state.supporting_observation_ids),
            list(state.refuting_observation_ids),
            state.status.value,
        ):
            raise EventConflictError("job-free seal consumed claim cache differs")
    for row in cursor.execute(
        "SELECT answer_version_id,required_claim_count,supported_count,"
        "unsupported_count,refuted_count,conflicted_count,status "
        "FROM groundloop_m4_working_answer_state WHERE epoch_id=%s "
        "ORDER BY answer_version_id",
        (epoch_id,),
    ).fetchall():
        try:
            answer_state = engine.answer_state(str(row[0]))
        except KeyError as error:
            raise EventConflictError(
                "job-free seal consumed answer cache is absent"
            ) from error
        if tuple(row) != (
            answer_state.answer_version_id,
            answer_state.required_claim_count,
            answer_state.supported_count,
            answer_state.unsupported_count,
            answer_state.refuted_count,
            answer_state.conflicted_count,
            answer_state.status.value,
        ):
            raise EventConflictError("job-free seal consumed answer cache differs")


def _check_staged_evaluation(
    cursor: Cursor[Any],
    epoch_id: int,
    predecessor: int,
    transition: EvaluationTransition,
) -> None:
    counter = cursor.execute(
        "SELECT btrim(declaration_hash),lifecycle_state,default_evaluation_state,"
        "confirmed_as_of_epoch,open_discovery_scope_count,revision "
        "FROM groundloop_m4_evaluation_epoch_counter WHERE epoch_id=%s",
        (epoch_id,),
    ).fetchone()
    if counter != (
        _declaration_hash(epoch_id, 1, predecessor, 0),
        "sealed",
        "complete",
        epoch_id,
        0,
        2,
    ):
        raise EventConflictError("job-free seal did not stage canonical counter2/K")
    transitions = cursor.execute(
        "SELECT transition_id,btrim(payload_hash),transition_kind,from_revision,"
        "to_revision,override_rows_written "
        "FROM groundloop_m4_evaluation_counter_transition "
        "WHERE epoch_id=%s",
        (epoch_id,),
    ).fetchall()
    if transitions != [
        (
            transition.transition_id,
            _sha256_json(_transition_payload(transition, ())),
            "seal",
            1,
            2,
            0,
        )
    ]:
        raise EventConflictError("job-free seal has a noncanonical evaluation ledger")


def _stage_job_free_document_seal_local(
    ports: PostgresM4ApplicationPorts,
    cursor: Cursor[Any],
    epoch_id: int,
    expected_revision: int,
    update: CorpusUpdateIdentity,
) -> PublicationReceipt:
    """Stage a fresh D33 adapter image in the caller's combined-seal transaction.

    Exact losing/terminal replay belongs to the outer retained-result reader;
    this adapter never reruns promotions after the canonical SEAL already exists.
    It intentionally leaves M5/base/heads/results/work/timing/cache unchanged.
    """
    if type(epoch_id) is not int or epoch_id <= 0:
        raise ValidationError("job-free seal epoch must be a positive integer")
    if type(expected_revision) is not int or expected_revision < 3:
        raise ValidationError("job-free seal requires the complete revision N>=3")
    if type(update) is not CorpusUpdateIdentity or update.update_kind not in {
        UpdateKind.INSERT,
        UpdateKind.DELETE,
        UpdateKind.REPLACE,
    }:
        raise ValidationError("job-free seal requires a document update identity")
    _require_cursor_context(ports, cursor)
    schema = _m5_readiness_schema(cursor)
    require_m5_document_update_completion_bundle(cursor, schema_name=schema)
    readiness._require_legacy_lookup_context(cursor, schema)
    readiness._read_row(cursor, schema, "groundloop_epoch", epoch_id, lock=True)
    readiness._read_row(
        cursor, schema, "groundloop_m5_runtime_epoch", epoch_id, lock=True
    )
    base, runtime = readiness._read_header_pair(cursor, schema, epoch_id)
    _check_complete_point(
        base,
        runtime,
        epoch_id=epoch_id,
        expected_revision=expected_revision,
        update=update,
    )
    typed_update = readiness._read_row(
        cursor, schema, "groundloop_m5_update", epoch_id, lock=False
    )
    if typed_update["update_kind"] != "document_" + update.update_kind.value:
        raise EventConflictError("job-free seal persisted document action differs")
    direct = readiness._read_row(
        cursor, schema, "groundloop_m4_update", epoch_id, lock=False
    )
    persisted = CorpusUpdateIdentity(
        event_id=str(base["event_id"]),
        payload_hash=readiness._hash(base["payload_hash"]),
        update_kind=UpdateKind(str(direct["update_kind"])),
        previous_published_epoch_id=direct["previous_published_epoch_id"],
        candidate_policy_id=str(direct["candidate_policy_id"]),
    )
    if persisted != update:
        raise EventConflictError("publication update differs from runtime epoch")
    readiness._check_declaration(cursor, schema, base, runtime, document_authority=True)
    readiness._check_jobs(cursor, schema, runtime, target="semantic_complete")
    _require_committed_readiness(cursor, schema, base, runtime)
    checked = ports.runtime_store.validate_seal_point_local(
        cursor,
        epoch_id,
        expected_revision,
        failure_injector=lambda point: ports._inject(
            "publication_store_" + point.removeprefix("point_")
        ),
    )
    if checked.replayed or checked.header.revision != expected_revision:
        raise InvalidEventError("job-free seal cannot restage a terminal point")
    _check_consumed_cache(
        cursor,
        ports,
        epoch_id=epoch_id,
        payload=readiness._r33_payload(direct["manifest"], typed_update["update_kind"]),
        decision_policy_version=str(typed_update["decision_policy_version"]),
    )
    publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
    ports._promote_structural_overlay(cursor, epoch_id)
    ports._inject("publication_structure_promoted")
    ports._promote_observation_currency(cursor, epoch_id)
    ports._inject("publication_currency_promoted")
    ports._publish_grounding_states(
        cursor, epoch_id, expected_revision + 1, update, emit_public_deltas=False
    )
    ports._inject("publication_states_written")
    transition = EvaluationTransition(
        transition_id=stable_m4_digest("m4-evaluation-seal-v1", str(epoch_id)),
        expected_revision=1,
        kind=EvaluationTransitionKind.SEAL,
    )
    receipt = ports.evaluation_store.apply_transition_local(
        cursor, epoch_id, transition
    )
    if receipt.replayed or (
        receipt.from_revision,
        receipt.to_revision,
        receipt.override_rows_written,
    ) != (1, 2, 0):
        raise EventConflictError("job-free seal did not create exactly one SEAL1-to-2")
    assert update.previous_published_epoch_id is not None
    _check_staged_evaluation(
        cursor, epoch_id, update.previous_published_epoch_id, transition
    )
    ports._inject("publication_evaluation_promoted")
    return PublicationReceipt(epoch_id, publication_id)
