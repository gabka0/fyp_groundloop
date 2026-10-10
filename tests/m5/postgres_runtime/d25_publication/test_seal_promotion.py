"""One-epoch promotion ownership and ordering tests."""

from __future__ import annotations

import importlib
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, cast

import psycopg
import pytest
from psycopg import sql

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.postgres_matching_publication import (
    M5MatchingPublicationChildren,
    _prepare_preterminal_matching_publication_children,
    build_matching_publication_children,
    promote_matching_overlay,
)
from groundloop.postgres.migrations import (
    install_m5_bounded_document_withdrawal_bundle,
    install_m5_preterminal_seal_context_bundle,
)
from tests.m5.postgres_runtime.test_semantic_readiness import (
    readiness_db as readiness_db,
)


@contextmanager
def _seal_runtime_session(
    owner: Any, schema_name: str, *, nonowner: bool
) -> Iterator[Any]:
    if not nonowner:
        yield owner
        return
    role = f"task16_runtime_{uuid.uuid4().hex}"
    password = uuid.uuid4().hex
    migration017 = importlib.import_module(
        "tests.m5.postgres_runtime.test_migration_017"
    )
    owner.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password)
        )
    )
    owner.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema_name), sql.Identifier(role)
        )
    )
    owner.execute(
        sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(
            sql.Identifier(schema_name), sql.Identifier(role)
        )
    )
    owner.execute(
        sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
            sql.Identifier(schema_name), sql.Identifier(role)
        )
    )
    owner.commit()
    try:
        with psycopg.connect(
            migration017._database_url(), user=role, password=password
        ) as runtime:
            runtime.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            runtime.commit()
            yield runtime
    finally:
        owner.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        owner.commit()


class _RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = 0

    def execute(self, query: str, params: object = None) -> _RecordingCursor:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        if normalized.startswith("UPDATE groundloop_m5_matching_image_current"):
            self.rowcount = 1
        elif normalized.startswith("DELETE FROM"):
            self.rowcount = 2
        elif normalized.startswith("INSERT INTO"):
            self.rowcount = 3
        else:
            self.rowcount = 0
        return self


class _PreterminalSealConnection:
    """Pause the retained live fixture at the newly authorized seal boundary."""

    def __init__(
        self,
        connection: Any,
        boundary_check: Callable[[Any, int, int], None] | None = None,
    ) -> None:
        self.connection = connection
        self.boundary_check = boundary_check
        self.contribution_query: object | None = None
        self.contribution_params: tuple[object, ...] | None = None
        self.result_query: object | None = None
        self.result_params: tuple[object, ...] | None = None
        self.timing_coverage_query: object | None = None
        self.timing_coverage_params: tuple[object, ...] | None = None
        self.preterminal_children: M5MatchingPublicationChildren | None = None
        self.terminal_children: M5MatchingPublicationChildren | None = None
        self.late_contribution_rejected = False

    def __getattr__(self, name: str) -> object:
        return getattr(self.connection, name)

    def _query_text(self, query: object) -> str:
        if isinstance(query, str):
            return query
        return str(cast(Any, query).as_string(self.connection))

    def execute(self, query: object, params: object = None) -> Any:
        normalized = " ".join(self._query_text(query).split())
        exact_params = None if params is None else tuple(cast(Any, params))
        if normalized.startswith("INSERT INTO groundloop_m5_runtime_work_contribution"):
            assert exact_params is not None
            self.contribution_query = query
            self.contribution_params = exact_params
            return self
        if normalized.startswith(
            "INSERT INTO groundloop_m5_event_result_state_reference"
        ) or normalized.startswith("INSERT INTO groundloop_m5_event_result_delta"):
            return self
        if normalized.startswith("INSERT INTO groundloop_m5_event_result ("):
            assert exact_params is not None
            self.result_query = query
            self.result_params = exact_params
            return self
        if normalized.startswith("INSERT INTO groundloop_m5_event_timing_coverage"):
            assert exact_params is not None
            self.timing_coverage_query = query
            self.timing_coverage_params = exact_params
            return self

        result = self.connection.execute(query, params)
        if normalized.startswith("UPDATE groundloop_epoch SET revision=") and (
            "semantic_status='sealed'" in normalized
        ):
            assert exact_params is not None
            epoch_id = int(exact_params[-1])
            sealed_revision = int(exact_params[0])
            expected_revision = sealed_revision - 1
            assert self.contribution_query is not None
            assert self.contribution_params is not None
            assert self.result_query is not None
            assert self.result_params is not None
            assert self.timing_coverage_query is not None
            assert self.timing_coverage_params is not None
            assert self.connection.execute(
                "SELECT runtime.runtime_state,runtime.revision,"
                "runtime.terminal_at IS NULL,work.updated_revision,"
                "work.terminalized,timing.updated_revision,timing.terminalized "
                "FROM groundloop_m5_runtime_epoch AS runtime "
                "JOIN groundloop_m5_runtime_work_accumulator AS work "
                "USING (epoch_id) "
                "JOIN groundloop_m5_runtime_timing_accumulator AS timing "
                "USING (epoch_id) WHERE runtime.epoch_id=%s",
                (epoch_id,),
            ).fetchone() == (
                "semantic_complete",
                expected_revision,
                True,
                expected_revision,
                False,
                expected_revision,
                False,
            )
            assert self.connection.execute(
                "SELECT (SELECT count(*) FROM groundloop_m5_event_result "
                "WHERE epoch_id=%s),(SELECT count(*) FROM "
                "groundloop_m5_event_result_delta WHERE structural_event_id=%s),"
                "(SELECT count(*) FROM groundloop_m5_event_result_state_reference "
                "WHERE structural_event_id=%s)",
                (epoch_id, self.result_params[0], self.result_params[0]),
            ).fetchone() == (0, 0, 0)
            distinct_owner = self.connection.execute(
                "SELECT relation.relowner <> current_user::regrole::oid "
                "FROM pg_catalog.pg_class AS relation WHERE relation.oid="
                "to_regclass('pg_temp.groundloop_m5_matching_promotion_context')"
            ).fetchone()[0]
            if distinct_owner:
                for query in (
                    "SELECT * FROM pg_temp.groundloop_m5_matching_promotion_context",
                    "SELECT groundloop_m5_matching_private_temp_triplet("
                    "to_regclass('pg_temp.groundloop_m5_matching_promotion_context'),"
                    "to_regclass('pg_temp.groundloop_m5_matching_promotion_journal'),"
                    "to_regclass('pg_temp.groundloop_m5_matching_promotion_expected'))",
                ):
                    self.connection.execute("SAVEPOINT task16_raw_access_denial")
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        self.connection.execute(query)
                    self.connection.execute(
                        "ROLLBACK TO SAVEPOINT task16_raw_access_denial"
                    )
                    self.connection.execute(
                        "RELEASE SAVEPOINT task16_raw_access_denial"
                    )
            if self.boundary_check is not None:
                self.boundary_check(self.connection, epoch_id, sealed_revision)
            prepared = _prepare_preterminal_matching_publication_children(
                self.connection.cursor(),
                epoch_id=epoch_id,
                expected_revision=expected_revision,
                sealed_revision=sealed_revision,
            )
            self.preterminal_children = prepared
            combined_hash = digests.combined_status_delta_set_digest(
                prepared.combined_deltas
            )
            changed_hash = digests.changed_state_set_digest(
                reference.reference_digest
                for reference in prepared.changed_state_references
            )
            event_id = str(self.result_params[0])
            publication_id = stable_m4_digest("m4-publication-v1", str(epoch_id))
            contribution_params = list(self.contribution_params)
            assert contribution_params[-5:] == [
                "seal",
                event_id,
                contribution_params[-3],
                contribution_params[-2],
                sealed_revision,
            ]
            contribution_params[-3] = digests.seal_contribution_source_digest(
                structural_event_id=event_id,
                combined_status_delta_set_hash=combined_hash,
                changed_state_set_hash=changed_hash,
                publication_id=publication_id,
            )
            self.connection.execute(self.contribution_query, tuple(contribution_params))
            self.contribution_params = tuple(contribution_params)
            assert self.connection.execute(
                "SELECT runtime.runtime_state,runtime.revision,"
                "work.terminalized,timing.terminalized,"
                "(SELECT count(*) "
                "FROM groundloop_m5_runtime_work_contribution AS contribution "
                "WHERE contribution.epoch_id=runtime.epoch_id "
                "AND contribution.contribution_kind='seal' "
                "AND contribution.source_id=%s) "
                "FROM groundloop_m5_runtime_epoch AS runtime "
                "JOIN groundloop_m5_runtime_work_accumulator AS work "
                "USING (epoch_id) "
                "JOIN groundloop_m5_runtime_timing_accumulator AS timing "
                "USING (epoch_id) WHERE runtime.epoch_id=%s",
                (event_id, epoch_id),
            ).fetchone() == (
                "semantic_complete",
                expected_revision,
                False,
                False,
                1,
            )
        elif normalized.startswith("UPDATE groundloop_m5_runtime_epoch") and (
            "runtime_state='sealed'" in normalized
        ):
            assert self.contribution_query is not None
            assert self.contribution_params is not None
            with pytest.raises(ValidationError, match="preterminal"):
                _prepare_preterminal_matching_publication_children(
                    self.connection.cursor(),
                    epoch_id=int(cast(Any, self.contribution_params[1])),
                    expected_revision=int(cast(Any, self.contribution_params[-1])) - 1,
                    sealed_revision=int(cast(Any, self.contribution_params[-1])),
                )
            late_params = list(self.contribution_params)
            late_source_id = f"{late_params[-4]}-after-terminal"
            late_params[-4] = late_source_id
            late_params[-2] = digests.runtime_work_contribution_key_digest(
                epoch_id=int(cast(Any, late_params[1])),
                contribution_kind="seal",
                source_id=late_source_id,
            )
            self.connection.execute("SAVEPOINT late_seal_contribution")
            try:
                self.connection.execute(self.contribution_query, tuple(late_params))
            except psycopg.Error as exc:
                assert (
                    "terminal M5 epoch rejects event-accounted work or timing"
                    in str(exc)
                )
                self.connection.execute("ROLLBACK TO SAVEPOINT late_seal_contribution")
                self.connection.execute("RELEASE SAVEPOINT late_seal_contribution")
                self.late_contribution_rejected = True
            else:  # pragma: no cover - this would violate migration 016.
                raise AssertionError("terminal runtime accepted a seal contribution")
        elif (
            normalized.startswith("UPDATE groundloop_m5_runtime_timing_accumulator")
            and "terminalized=true" in normalized
        ):
            assert self.result_query is not None
            assert self.result_params is not None
            assert self.timing_coverage_query is not None
            assert self.timing_coverage_params is not None
            assert self.preterminal_children is not None
            assert exact_params is not None
            result_params = list(self.result_params)
            event_id = str(result_params[0])
            epoch_id = int(cast(Any, result_params[2]))
            combined_hash = digests.combined_status_delta_set_digest(
                self.preterminal_children.combined_deltas
            )
            changed_hash = digests.changed_state_set_digest(
                reference.reference_digest
                for reference in self.preterminal_children.changed_state_references
            )
            result_params[7] = combined_hash
            result_params[8] = changed_hash
            result_params[9] = digests.event_run_logical_result_digest(
                event_id=event_id,
                payload_hash=str(result_params[1]),
                epoch_id=epoch_id,
                sealed_or_failed_outcome="sealed",
                original_open_receipt_binding_hash=str(result_params[3]),
                original_publication_receipt_binding_hash=str(result_params[5]),
                event_work_digest=str(result_params[6]),
                combined_status_delta_set_hash=combined_hash,
                changed_state_set_hash=changed_hash,
                failure_reason=None,
            )
            result_params[10] = len(self.preterminal_children.changed_state_references)
            self.connection.execute(self.result_query, tuple(result_params))
            self.result_params = tuple(result_params)
            self.connection.execute(
                self.timing_coverage_query, self.timing_coverage_params
            )
            terminal_children = build_matching_publication_children(
                self.connection.cursor(),
                epoch_id=epoch_id,
                sealed_revision=int(cast(Any, exact_params[0])),
            )
            assert terminal_children == self.preterminal_children
            self.terminal_children = terminal_children
            for ordinal, delta in enumerate(terminal_children.combined_deltas):
                self.connection.execute(
                    "INSERT INTO groundloop_m5_event_result_delta VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s)",
                    (
                        event_id,
                        ordinal,
                        delta.object_type,
                        delta.object_id,
                        delta.old_status,
                        delta.new_status,
                        delta.reason,
                    ),
                )
            for ordinal, reference in enumerate(
                terminal_children.changed_state_references
            ):
                self.connection.execute(
                    "INSERT INTO groundloop_m5_event_result_state_reference "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        event_id,
                        ordinal,
                        reference.kind.value,
                        reference.object_id,
                        reference.epoch_id,
                        reference.revision,
                        reference.state_artifact_hash,
                        reference.reference_digest,
                    ),
                )
        return result


@pytest.mark.parametrize("action", ("REPLACE", "RETIRE"))
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_readiness_pending_survives_preparation_and_freezes_missing(
    readiness_db: Any, action: str, nonowner: bool
) -> None:
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres_runtime.test_semantic_readiness import (
        _runtime_login,
        _snapshot,
    )

    db = readiness_db
    # The accepted matching-open fixture reads a definer-owned private context.
    # Establish that foundation as its owner; this lane tests a distinct runtime
    # principal's real readiness, pending seal, and replay, not non-owner open.
    opened = _open_pending_matching_group(db, db.connection, action)
    context = _runtime_login(db) if nonowner else db.reconnect()
    with context as connection:
        principal = connection.execute("SELECT current_user").fetchone()[0]
        owner = db.connection.execute("SELECT current_user").fetchone()[0]
        db.connection.commit()
        assert (principal != owner) is nonowner
        connection.commit()
        plan, epoch, anchor = _genuine_pending_ready(
            db, connection, action, opened=opened
        )
        before = PostgresM5RuntimeStore(connection).current_event_work(epoch)
        statements: list[str] = []

        class RecordingCursor(psycopg.Cursor[Any]):
            def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
                text = (
                    query
                    if isinstance(query, str)
                    else query.as_string(self.connection)
                )
                statements.append(" ".join(text.split()))
                return super().execute(query, params, **kwargs)

        factory = connection.cursor_factory
        connection.cursor_factory = RecordingCursor
        try:
            result = _sql_pending_seal_boundary(
                db, connection, plan, epoch, anchor.anchor_revision
            )
        finally:
            connection.cursor_factory = factory
        assert (
            sum(
                q.startswith("UPDATE groundloop_m5_runtime_timing_accumulator ")
                for q in statements
            )
            == 1
        )
        assert (
            sum(
                q.startswith("UPDATE groundloop_m5_runtime_work_accumulator ")
                for q in statements
            )
            == 1
        )
        contribution = next(
            i
            for i, q in enumerate(statements)
            if q.startswith("INSERT INTO groundloop_m5_runtime_work_contribution ")
        )
        terminal = next(
            i
            for i, q in enumerate(statements)
            if q.startswith("UPDATE groundloop_m5_runtime_epoch ")
        )
        assert contribution < terminal
        assert not any("SUM(" in q.upper() for q in statements)
        assert statements[-1] == "SET CONSTRAINTS ALL IMMEDIATE"
        assert all(
            a + b == c
            for a, b, c in zip(
                before.counter_values(),
                result.call_work.counter_values(),
                result.event_work.counter_values(),
                strict=True,
            )
        )
        # Independent exhaustive test oracle, outside the terminal adapter. The
        # adapter's authority remains the locked cumulative point, not this SUM.
        sums = connection.execute(
            sql.SQL("SELECT {} FROM {} WHERE epoch_id=%s").format(
                sql.SQL(",").join(
                    sql.SQL("sum({})").format(sql.Identifier(name))
                    for name in result.event_work.counter_names()
                ),
                sql.Identifier(
                    db.schema_name, "groundloop_m5_runtime_work_contribution"
                ),
            ),
            (epoch,),
        ).fetchone()
        assert sums == result.event_work.counter_values()
        row = connection.execute(
            "SELECT contribution_key_digest,coordinator_non_db_non_neural_ns,"
            "neural_wall_ns,postgres_roundtrip_wall_ns,external_io_wall_ns,end_to_end_wall_ns,"
            "postgres_server_execution_ns,postgres_lock_wait_ns,postgres_wal_bytes,"
            "postgres_shared_block_reads FROM groundloop_m5_transition_call_timing "
            "WHERE epoch_id=%s AND contribution_kind=%s AND source_id=%s "
            "AND anchor_revision=%s",
            (
                epoch,
                anchor.contribution_kind.value,
                anchor.source_id,
                anchor.anchor_revision,
            ),
        ).fetchone()
        assert row == (
            anchor.contribution_key_digest,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        assert connection.execute(
            "SELECT updated_revision,terminalized,pending_contribution_kind,"
            "pending_source_id,"
            "pending_contribution_key_digest,pending_anchor_revision,required_expected_count,"
            "required_observed_count,required_missing_count "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
            (epoch,),
        ).fetchone() == (
            anchor.anchor_revision + 1,
            True,
            None,
            None,
            None,
            None,
            4,
            0,
            4,
        )
        assert len(result.changed_state_references) == (4 if action == "REPLACE" else 2)
        connection.commit()
        before_replay = _snapshot(connection, db.schema_name)
        replay = PostgresM5RuntimeStore(connection).read_typed_event_result(
            plan.structural_event_id, plan.payload_hash
        )
        assert (
            replay is not None
            and replay.logical_result_hash == result.logical_result_hash
        )
        assert replay.event_work == result.event_work
        assert not any(replay.call_work.counter_values())
        assert _snapshot(connection, db.schema_name) == before_replay
        with pytest.raises(
            (ValidationError, psycopg.errors.RaiseException), match="preterminal"
        ):
            with connection.transaction(), connection.cursor() as cursor:
                _prepare_preterminal_matching_publication_children(
                    cursor,
                    epoch_id=epoch,
                    expected_revision=anchor.anchor_revision,
                    sealed_revision=anchor.anchor_revision + 1,
                )


@pytest.mark.parametrize("action", ("REPLACE", "RETIRE"))
def test_genuine_pending_seal_each_mutation_cut_rolls_back(
    readiness_db: Any, action: str
) -> None:
    from tests.m5.postgres_runtime.test_semantic_readiness import _snapshot

    db = readiness_db
    plan, epoch, anchor = _genuine_pending_ready(db, db.connection, action)
    before = _snapshot(db.connection, db.schema_name)
    for point in (
        "after_accounting_locks",
        "after_half_terminal",
        "after_preparation",
        "after_contribution",
        "after_prior_missing",
        "after_terminal_runtime",
        "after_work_cas",
        "after_timing_cas",
        "after_result_children",
        "after_constraints",
    ):

        def fail(current: str, *, expected: str = point) -> None:
            if current == expected:
                raise RuntimeError("reader-boundary injected " + expected)

        with pytest.raises(RuntimeError, match="reader-boundary injected"):
            _sql_pending_seal_boundary(
                db, db.connection, plan, epoch, anchor.anchor_revision, fail
            )
        assert _snapshot(db.connection, db.schema_name) == before
    _sql_pending_seal_boundary(db, db.connection, plan, epoch, anchor.anchor_revision)


def test_seal_promotion_is_one_epoch_and_does_not_own_outer_transaction() -> None:
    cursor = _RecordingCursor()
    receipt = promote_matching_overlay(
        cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=5
    )
    assert receipt.mode == "seal"
    assert receipt.observation_deletes == 2
    assert receipt.observation_writes == 3
    assert receipt.edge_deletes == 2
    assert receipt.edge_writes == 3
    assert receipt.mask_deletes == 2
    assert receipt.mask_writes == 3
    assert receipt.hall_deletes == 2
    assert receipt.hall_writes == 3
    sql = "\n".join(query for query, _ in cursor.calls).lower()
    assert "groundloop_m5_authorize_checked_transition" in sql
    assert "groundloop_m5_authorize_persisted_matching_seal" in sql
    assert "set_config('groundloop.m5_checked_transition'" not in sql
    assert cursor.calls[0][1] == (17, 4)
    assert "where epoch_id = %s" in sql
    assert "groundloop_m5_publication_head" not in sql
    assert "groundloop_m5_event_result" not in sql
    assert "commit" not in sql
    assert "rollback" not in sql


def test_seal_promotion_rejects_nonadjacent_seal_before_sql() -> None:
    cursor = _RecordingCursor()
    with pytest.raises(ValidationError, match=r"expected_revision \+ 1"):
        promote_matching_overlay(
            cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=6
        )
    assert cursor.calls == []


def test_live_nonempty_tombstone_promotion_uses_checked_authority() -> None:
    migration017 = cast(
        Any,
        importlib.import_module("tests.m5.postgres_runtime.test_migration_017"),
    )

    prefix = "lane-b-live-promotion"
    with migration017._pre017_schema() as (connection, schema_name):
        snapshot = migration017._seed_b3_activated_snapshot(
            connection,
            prefix,
            complete_owner_survivor=True,
        )
        assert migration017.install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        migration017._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = migration017._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        assert predecessor.certificate_digest is None
        open_prefix = f"{prefix}-open"
        event_id = f"{open_prefix}-event"
        payload_hash, _ = migration017._d26_structural_payload(
            connection,
            group_id=predecessor.group_id,
            event_id=event_id,
            action="RETIRE",
        )
        epoch_id, actual_event, actual_payload = migration017._open_b2_runtime_epoch(
            connection,
            open_prefix,
            snapshot.epoch_id,
            snapshot.policy_version,
            direct_bridge=False,
            update_kind="retire_group",
            payload_override=payload_hash,
        )
        assert (actual_event, actual_payload) == (event_id, payload_hash)
        migration017._stage_b2_group_deactivation(
            connection,
            epoch=epoch_id,
            event=event_id,
            group=predecessor.group_id,
            action="RETIRE",
        )
        absent_states = tuple(
            [
                *zip(
                    ("requirement_state",) * len(predecessor.requirement_ids),
                    predecessor.requirement_ids,
                    predecessor.requirement_state_hashes,
                    strict=True,
                ),
                (
                    "group_state",
                    predecessor.group_id,
                    predecessor.group_state_hash,
                ),
            ]
        )
        current_relations = (
            "groundloop_m5_matching_observation_current",
            "groundloop_m5_matching_edge_current",
            "groundloop_m5_matching_hash_mask_current",
            "groundloop_m5_matching_hall_current",
        )
        before_counts = tuple(
            int(
                connection.execute(
                    f"SELECT count(*) FROM {relation} WHERE group_version_id=%s",
                    (predecessor.group_id,),
                ).fetchone()[0]
            )
            for relation in current_relations
        )
        assert all(count > 0 for count in before_counts)
        migration017._apply_empty_b2_structural(
            connection,
            epoch=epoch_id,
            event=event_id,
            payload=payload_hash,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            absent_states=absent_states,
            remove_current_group=predecessor.group_id,
        )
        connection.commit()

        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s,1)",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_epoch SET revision=2 WHERE epoch_id=%s AND revision=1",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch "
            "SET runtime_state='semantic_pending',revision=2 "
            "WHERE epoch_id=%s AND revision=1",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET updated_revision=2 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_answer_pending_counter "
            "SET updated_revision=2 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        connection.execute(
            "SELECT groundloop_m5_authorize_checked_transition(%s,2)",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_epoch "
            "SET revision=3,semantic_status='complete',evaluation_state='complete' "
            "WHERE epoch_id=%s AND revision=2",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_runtime_epoch "
            "SET runtime_state='semantic_complete',revision=3 "
            "WHERE epoch_id=%s AND revision=2",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_owner_pending_counter "
            "SET updated_revision=3 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute(
            "UPDATE groundloop_m5_answer_pending_counter "
            "SET updated_revision=3 WHERE epoch_id=%s",
            (epoch_id,),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
        connection.commit()

        receipt = promote_matching_overlay(
            connection.cursor(),
            epoch_id=epoch_id,
            expected_revision=3,
            sealed_revision=4,
        )
        assert (
            receipt.observation_deletes,
            receipt.edge_deletes,
            receipt.mask_deletes,
            receipt.hall_deletes,
        ) == before_counts
        assert (
            receipt.observation_writes,
            receipt.edge_writes,
            receipt.mask_writes,
            receipt.hall_writes,
        ) == (0, 0, 0, 0)
        assert connection.execute(
            "SELECT installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current WHERE singleton"
        ).fetchone() == (epoch_id, 4)
        assert all(
            connection.execute(
                f"SELECT count(*) FROM {relation} WHERE group_version_id=%s",
                (predecessor.group_id,),
            ).fetchone()
            == (0,)
            for relation in current_relations
        )
        assert connection.execute(
            """SELECT
                 (SELECT count(*) FROM groundloop_m5_matching_observation_working
                   WHERE epoch_id=%s AND NOT present) +
                 (SELECT count(*) FROM groundloop_m5_matching_edge_working
                   WHERE epoch_id=%s AND refcount=0) +
                 (SELECT count(*) FROM groundloop_m5_matching_hash_mask_working
                   WHERE epoch_id=%s AND mask=0) +
                 (SELECT count(*) FROM groundloop_m5_matching_hall_working
                   WHERE epoch_id=%s AND NOT present)""",
            (epoch_id, epoch_id, epoch_id, epoch_id),
        ).fetchone() == (sum(before_counts),)
        with psycopg.connect(migration017._database_url()) as contender:
            contender.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            contender.execute("SET lock_timeout='500ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                promote_matching_overlay(
                    contender.cursor(),
                    epoch_id=epoch_id,
                    expected_revision=3,
                    sealed_revision=4,
                )
            contender.rollback()
        connection.rollback()
        assert connection.execute(
            "SELECT installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current WHERE singleton"
        ).fetchone() == (snapshot.epoch_id, snapshot.revision)


@pytest.mark.parametrize("nonowner", (False, True), ids=("owner", "nonowner"))
@pytest.mark.parametrize("action", ("REPLACE", "RETIRE"))
def test_live_preterminal_children_bind_contribution_and_match_terminal_bytes(
    action: str,
    nonowner: bool,
) -> None:
    migration017 = cast(
        Any,
        importlib.import_module("tests.m5.postgres_runtime.test_migration_017"),
    )

    prefix = f"c1-r-preterminal-{action.lower()}"
    with migration017._pre017_schema() as (owner, schema_name):
        snapshot = migration017._seed_b3_activated_snapshot(
            owner,
            prefix,
            complete_owner_survivor=True,
        )
        assert migration017.install_m5_persisted_matching_bundle(owner).applied
        owner.commit()
        assert install_m5_bounded_document_withdrawal_bundle(owner).applied
        assert install_m5_preterminal_seal_context_bundle(owner).applied
        migration017._register_d26_candidate_policy(owner, snapshot)
        owner.commit()
        predecessor = migration017._d26_predecessor_from_published_image(
            owner,
            group_id=snapshot.complete_group_id,
            base=snapshot.epoch_id,
        )
        assert predecessor.certificate_digest is not None
        owner.commit()
        with _seal_runtime_session(owner, schema_name, nonowner=nonowner) as connection:
            _assert_live_preterminal_seal(
                connection, migration017, prefix, snapshot, predecessor, action
            )


def _assert_live_preterminal_seal(
    connection: Any,
    migration017: Any,
    prefix: str,
    snapshot: Any,
    predecessor: Any,
    action: str,
    boundary_check: Callable[[Any, int, int], None] | None = None,
) -> None:
    proxy = _PreterminalSealConnection(connection, boundary_check)
    sealed = migration017._seal_d26_event(
        cast(Any, proxy),
        prefix=prefix,
        base=snapshot.epoch_id,
        base_revision=snapshot.revision,
        policy=snapshot.policy_version,
        predecessor=predecessor,
        action=action,
        retire_current_physical=True,
    )
    assert proxy.preterminal_children is not None
    assert proxy.terminal_children == proxy.preterminal_children
    assert proxy.late_contribution_rejected
    assert {
        (
            reference.kind.value,
            reference.object_id,
            reference.state_artifact_hash,
            reference.reference_digest,
        )
        for reference in proxy.preterminal_children.changed_state_references
    } == set(sealed.references)
    assert {
        reference.kind.value
        for reference in proxy.preterminal_children.changed_state_references
    }.issuperset({"requirement_state", "group_state", "group_certificate"})
    assert (
        build_matching_publication_children(
            connection.cursor(),
            epoch_id=sealed.epoch_id,
            sealed_revision=sealed.revision,
        )
        == proxy.preterminal_children
    )
    assert connection.execute(
        "SELECT contribution_kind,source_id,applied_revision "
        "FROM groundloop_m5_runtime_work_contribution "
        "WHERE epoch_id=%s AND contribution_kind='seal'",
        (sealed.epoch_id,),
    ).fetchall() == [("seal", sealed.event_id, sealed.revision)]
    connection.commit()


def test_live_preterminal_one_schema_matrix_never_falls_through() -> None:
    migration017 = importlib.import_module(
        "tests.m5.postgres_runtime.test_migration_017"
    )
    migration019 = importlib.import_module(
        "tests.m5.postgres_runtime.test_migration_019"
    )
    with migration017._pre017_schema() as (owner, selected):
        prefix = "task16-schema-matrix"
        snapshot = migration017._seed_b3_activated_snapshot(
            owner, prefix, complete_owner_survivor=True
        )
        assert migration017.install_m5_persisted_matching_bundle(owner).applied
        owner.commit()
        assert install_m5_bounded_document_withdrawal_bundle(owner).applied
        assert install_m5_preterminal_seal_context_bundle(owner).applied
        migration017._register_d26_candidate_policy(owner, snapshot)
        owner.commit()
        predecessor = migration017._d26_predecessor_from_published_image(
            owner, group_id=snapshot.complete_group_id, base=snapshot.epoch_id
        )
        owner.commit()
        with migration019._pre019_schema() as (other, fallback):
            assert install_m5_preterminal_seal_context_bundle(other).applied
            other.commit()

            def check(connection: Any, epoch_id: int, revision: int) -> None:
                def prepare() -> M5MatchingPublicationChildren:
                    return _prepare_preterminal_matching_publication_children(
                        connection.cursor(),
                        epoch_id=epoch_id,
                        expected_revision=revision - 1,
                        sealed_revision=revision,
                    )

                # A later namespace supplies a real pinned accessor/ledger and
                # an exact epoch view, so unqualified lookup could conceal a
                # missing selected component. These are negative fixtures only.
                connection.execute(
                    sql.SQL(
                        "ALTER TABLE {}.groundloop_epoch RENAME TO "
                        "fallback_original_epoch"
                    ).format(sql.Identifier(fallback))
                )
                connection.execute(
                    sql.SQL(
                        "CREATE VIEW {}.groundloop_epoch AS SELECT * FROM "
                        "{}.groundloop_epoch"
                    ).format(sql.Identifier(fallback), sql.Identifier(selected))
                )
                connection.execute(
                    sql.SQL("SET LOCAL search_path TO {}, {}, public").format(
                        sql.Identifier(selected), sql.Identifier(fallback)
                    )
                )
                baseline = prepare()
                # Every nonempty subset of ledger/accessor/envelope missing in
                # the selected namespace must reject, never source its suffix.
                for missing in range(1, 8):
                    connection.execute("SAVEPOINT task16_schema_mixture")
                    if missing & 1:
                        connection.execute(
                            sql.SQL(
                                "ALTER TABLE {}.groundloop_m5_schema_bundle "
                                "RENAME TO task16_hidden_ledger"
                            ).format(sql.Identifier(selected))
                        )
                    if missing & 2:
                        connection.execute(
                            sql.SQL(
                                "ALTER FUNCTION {}.groundloop_m5_matching_"
                                "read_preterminal_seal_context(bigint,bigint,bigint) "
                                "RENAME TO task16_hidden_accessor"
                            ).format(sql.Identifier(selected))
                        )
                    if missing & 4:
                        connection.execute(
                            sql.SQL(
                                "ALTER TABLE {}.groundloop_epoch RENAME TO "
                                "task16_hidden_epoch"
                            ).format(sql.Identifier(selected))
                        )
                    with pytest.raises((ValidationError, psycopg.Error)):
                        prepare()
                    connection.execute("ROLLBACK TO SAVEPOINT task16_schema_mixture")
                    connection.execute("RELEASE SAVEPOINT task16_schema_mixture")
                    assert prepare() == baseline

                # Earlier unmigrated and decoy namespaces must fail before a
                # later valid namespace; a decoy function must never execute.
                for decoy in (False, True):
                    connection.execute("SAVEPOINT task16_earlier_schema")
                    earlier = f"task16_earlier_{uuid.uuid4().hex}"
                    connection.execute(
                        sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(earlier))
                    )
                    if decoy:
                        connection.execute(
                            sql.SQL(
                                "CREATE TABLE {}.groundloop_m5_schema_bundle "
                                "(bundle_id text, bundle_sha256 text, "
                                "migration_sha256 text, oracle_sha256 text, "
                                "prerequisite_sha256 text)"
                            ).format(sql.Identifier(earlier))
                        )
                        connection.execute(
                            sql.SQL(
                                "CREATE FUNCTION {}.groundloop_m5_matching_"
                                "read_preterminal_seal_context(bigint,bigint,bigint) "
                                "RETURNS void LANGUAGE plpgsql AS $$ BEGIN "
                                "RAISE EXCEPTION 'decoy executed'; END $$"
                            ).format(sql.Identifier(earlier))
                        )
                    connection.execute(
                        sql.SQL("SET LOCAL search_path TO {}, {}, public").format(
                            sql.Identifier(earlier), sql.Identifier(selected)
                        )
                    )
                    with pytest.raises(
                        ValidationError,
                        match=("exact migration 019" if decoy else "permanent ledger"),
                    ):
                        prepare()
                    connection.execute("ROLLBACK TO SAVEPOINT task16_earlier_schema")
                    connection.execute("RELEASE SAVEPOINT task16_earlier_schema")
                assert prepare() == baseline

            _assert_live_preterminal_seal(
                owner, migration017, prefix, snapshot, predecessor, "RETIRE", check
            )


@pytest.mark.parametrize("action", ("REPLACE", "RETIRE"))
def test_live_preterminal_preparation_failure_rolls_back_seal(action: str) -> None:
    migration017 = importlib.import_module(
        "tests.m5.postgres_runtime.test_migration_017"
    )
    with migration017._pre017_schema() as (connection, _):
        prefix = f"task16-rollback-{action.lower()}"
        snapshot = migration017._seed_b3_activated_snapshot(
            connection, prefix, complete_owner_survivor=True
        )
        assert migration017.install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        assert install_m5_bounded_document_withdrawal_bundle(connection).applied
        assert install_m5_preterminal_seal_context_bundle(connection).applied
        migration017._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = migration017._d26_predecessor_from_published_image(
            connection, group_id=snapshot.complete_group_id, base=snapshot.epoch_id
        )
        connection.commit()
        captured: list[int] = []

        def fail(connection: Any, epoch_id: int, revision: int) -> None:
            prepared = _prepare_preterminal_matching_publication_children(
                connection.cursor(),
                epoch_id=epoch_id,
                expected_revision=revision - 1,
                sealed_revision=revision,
            )
            assert prepared.changed_state_references
            captured.append(epoch_id)
            raise RuntimeError("task16 injected after preparation")

        proxy = _PreterminalSealConnection(connection, fail)
        with pytest.raises(RuntimeError, match="task16 injected"):
            migration017._seal_d26_event(
                proxy,
                prefix=prefix,
                base=snapshot.epoch_id,
                base_revision=snapshot.revision,
                policy=snapshot.policy_version,
                predecessor=predecessor,
                action=action,
                retire_current_physical=True,
            )
        connection.rollback()
        assert len(captured) == 1
        epoch_id = captured[0]
        assert connection.execute(
            "SELECT epoch.revision,epoch.semantic_status,runtime.revision,"
            "runtime.runtime_state,runtime.terminal_at "
            "FROM groundloop_epoch AS epoch JOIN groundloop_m5_runtime_epoch "
            "AS runtime USING(epoch_id) WHERE epoch.epoch_id=%s",
            (epoch_id,),
        ).fetchone() == (3, "complete", 3, "semantic_complete", None)
        assert connection.execute(
            "SELECT installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current WHERE singleton"
        ).fetchone() == (snapshot.epoch_id, snapshot.revision)
        assert connection.execute(
            "SELECT m4.epoch_id,m5.epoch_id,m5.sealed_revision "
            "FROM groundloop_m4_publication_head AS m4 "
            "CROSS JOIN groundloop_m5_publication_head AS m5 "
            "WHERE m4.singleton AND m5.singleton"
        ).fetchone() == (snapshot.epoch_id, snapshot.epoch_id, snapshot.revision)
        assert connection.execute(
            "SELECT valid_to_epoch FROM groundloop_m5_published_group_state "
            "WHERE group_version_id=%s AND valid_from_epoch<=%s "
            "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
            (predecessor.group_id, snapshot.epoch_id, snapshot.epoch_id),
        ).fetchone() == (None,)
        assert connection.execute(
            "SELECT (SELECT count(*) FROM groundloop_m5_event_result "
            "WHERE epoch_id=%s),(SELECT count(*) "
            "FROM groundloop_m5_runtime_work_contribution "
            "WHERE epoch_id=%s AND contribution_kind='seal')",
            (epoch_id, epoch_id),
        ).fetchone() == (0, 0)


def _open_pending_matching_group(
    db: Any, connection: Any, action: str
) -> tuple[Any, int]:
    """Compose accepted helpers before rev-1 commit, never held C1 or post-hoc.

    The real stage writes are counted from newly inserted epoch-local rows.
    Bytes come from the accepted structural-open producer. This is a test
    foundation, not a claim of production C1 structural composition.
    """
    from groundloop.m5.runtime.contracts import M5PersistedMatchingSourceKind
    from groundloop.m5.runtime.persistence import (
        _derive_structural_open_work,
        _load_candidate_manifest,
        _persist_active_chunk_snapshot,
        _persist_initial_pending_counters,
        _persist_requirement_snapshot,
        _persist_root_declarations,
        _root_declarations_for_open,
        _stage_structure,
        _validate_effective_snapshots,
        _validate_structure_declaration,
    )
    from groundloop.m5.runtime.postgres_matching import (
        _finalize_prepared_matching_transition,
        _prepare_matching_transition,
        _stage_prepared_matching_transition,
    )
    from groundloop.m5.runtime.postgres_recovery import (
        persist_structural_open_accounting,
        persist_structural_open_identity,
    )
    from tests.m5.postgres_runtime.d25_store_core.conftest import (
        authorize_and_derive_matching_transition,
    )

    plan = getattr(db, action.lower() + "_plan")(
        event_id="d32-reader-pending-" + action.lower()
    )
    with connection.transaction(), connection.cursor() as cursor:
        for table in (
            "groundloop_runtime_mode",
            "groundloop_m4_publication_head",
            "groundloop_m5_publication_head",
            "groundloop_m5_activation",
        ):
            assert cursor.execute(
                sql.SQL("SELECT * FROM {} WHERE singleton FOR UPDATE").format(
                    sql.Identifier(db.schema_name, table)
                )
            ).fetchone()
        manifest = _load_candidate_manifest(cursor, plan.candidate_policy_id)
        roots, root_set = _root_declarations_for_open(plan, manifest, None, None)
        epoch = cursor.execute(
            "INSERT INTO groundloop_epoch "
            "(event_id,payload_hash,revision,structural_status,semantic_status,"
            "evaluation_state,publication_mode,sealed_at) VALUES "
            "(%s,%s,1,'committed','pending','pending','provisional',NULL) "
            "RETURNING epoch_id",
            (plan.structural_event_id, plan.payload_hash),
        ).fetchone()[0]
        cursor.execute(
            "INSERT INTO groundloop_m5_update "
            "(epoch_id,update_kind,previous_published_epoch_id,decision_policy_version,"
            "manifest) VALUES (%s,%s,%s,%s,'{}'::jsonb)",
            (
                epoch,
                action.lower() + "_group",
                plan.expected_previous_published_epoch_id,
                manifest.decision_policy_version,
            ),
        )
        cursor.execute(
            "INSERT INTO groundloop_m5_runtime_epoch "
            "(epoch_id,structural_event_id,candidate_policy_id,"
            "candidate_policy_manifest_hash,requirement_registry_snapshot_digest,"
            "active_chunk_snapshot_digest,expected_previous_published_epoch_id,"
            "requirement_root_set_hash,runtime_state,revision,open_work_count,"
            "open_scope_count,blocking_failure_count,terminal_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'structural_committed',1,%s,%s,0,NULL)",
            (
                epoch,
                plan.structural_event_id,
                plan.candidate_policy_id,
                plan.candidate_policy_manifest_hash,
                plan.requirement_registry_snapshot.requirement_registry_snapshot_digest,
                plan.active_chunk_snapshot.active_chunk_snapshot_digest,
                plan.expected_previous_published_epoch_id,
                root_set,
                len(roots),
                len(roots),
            ),
        )
        _validate_structure_declaration(cursor, plan.event)
        _stage_structure(
            cursor, event=plan.event, epoch_id=epoch, failure_injector=None
        )
        _validate_effective_snapshots(
            cursor, plan=plan, epoch_id=epoch, direct_open=False
        )
        _persist_requirement_snapshot(
            cursor, snapshot=plan.requirement_registry_snapshot, epoch_id=epoch
        )
        _persist_active_chunk_snapshot(
            cursor, snapshot=plan.active_chunk_snapshot, epoch_id=epoch
        )
        _persist_root_declarations(
            cursor,
            structural_event_id=plan.structural_event_id,
            epoch_id=epoch,
            roots=roots,
        )
        _persist_initial_pending_counters(
            cursor,
            snapshot=plan.requirement_registry_snapshot,
            epoch_id=epoch,
            roots=roots,
        )
        persist_structural_open_identity(
            cursor,
            epoch_id=epoch,
            config=db.operational_config,
            root_fallback_required={r.job.logical_job_id: False for r in roots},
        )
        intent = authorize_and_derive_matching_transition(
            cursor,
            epoch_id=epoch,
            expected_runtime_revision=1,
            resulting_revision=1,
            source_kind=M5PersistedMatchingSourceKind.STRUCTURAL_OPEN,
            source_id=plan.structural_event_id,
            expected_source_identity_hash=plan.payload_hash,
        )
        prepared = _prepare_matching_transition(cursor, intent)
        staged = _stage_prepared_matching_transition(cursor, intent, prepared)
        measured = {}
        for kind in ("group", "claim", "answer"):
            measured[kind + "_state_write_count"] = cursor.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE epoch_id=%s").format(
                    sql.Identifier(
                        db.schema_name, "groundloop_m5_working_" + kind + "_state"
                    )
                ),
                (epoch,),
            ).fetchone()[0]
        measured["certificate_binding_write_count"] = sum(
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE epoch_id=%s").format(
                    sql.Identifier(
                        db.schema_name,
                        "groundloop_m5_working_" + kind + "_certificate_binding",
                    )
                ),
                (epoch,),
            ).fetchone()[0]
            for kind in ("group", "claim")
        )
        assert measured["group_state_write_count"] == (
            prepared.d24_owned_planned_write_counts.group_state_write_count
        )
        base = _derive_structural_open_work(cursor, epoch_id=epoch)
        values = dict(zip(base.counter_names(), base.counter_values(), strict=True))
        values.update(measured)
        values["public_delta_count"] = 0
        persist_structural_open_accounting(
            cursor,
            epoch_id=epoch,
            structural_event_id=plan.structural_event_id,
            payload_hash=plan.payload_hash,
            structural_work=type(base)(**values),
        )
        _finalize_prepared_matching_transition(cursor, intent, staged)
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return plan, epoch


def _genuine_pending_ready(
    db: Any,
    connection: Any,
    action: str,
    *,
    opened: tuple[Any, int] | None = None,
) -> tuple[Any, int, Any]:
    from groundloop.m5.runtime.contracts import M5CancellationPlan, M5TerminalReason
    from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
    from tests.m5.postgres_runtime.test_semantic_readiness import _edge, _roots

    plan, epoch = (
        _open_pending_matching_group(db, connection, action)
        if opened is None
        else opened
    )
    if action == "RETIRE":
        pending = _edge(
            connection, epoch, plan.structural_event_id, "semantic_pending", 1
        )
        revision = pending.anchor.anchor_revision
    else:
        roots = _roots(plan, db.manifest)
        cancellation = PostgresM5RuntimeStore(connection).cancel_m5_work_atomically(
            epoch,
            1,
            M5CancellationPlan.build(
                epoch_id=epoch,
                structural_event_id=plan.structural_event_id,
                reason=M5TerminalReason.SCOPE_RETIRED,
                cancelled_job_ids=tuple(sorted(job.logical_job_id for _, job in roots)),
            ),
        )
        revision = cancellation.resulting_revision
    ready = _edge(
        connection, epoch, plan.structural_event_id, "semantic_complete", revision
    )
    return plan, epoch, ready.anchor


def _sql_pending_seal_boundary(
    db: Any,
    connection: Any,
    plan: Any,
    epoch: int,
    revision: int,
    cut: Callable[[str], None] = lambda _: None,
) -> Any:
    """Reader/SQL-boundary adapter, not production C1 or full work measurement.

    Publication rowcounts and explicit frozen seal S/K encodes/hashes are
    measured. Other compare-only helpers/validation/SQL producers are excluded
    from this partial diagnostic; no complete producer or utility claim follows.
    """
    import hashlib

    from groundloop.errors import InvalidEventError
    from groundloop.m4.application import OpenEventReceipt, PublicationReceipt
    from groundloop.m5.digests import enum_field, hash_field, int_field, text_field
    from groundloop.m5.runtime.contracts import (
        M5EventRunResult,
        M5RunState,
        M5RuntimeTiming,
        M5RuntimeTimingCoverage,
        M5RuntimeWork,
    )
    from groundloop.m5.runtime.persistence import _insert_runtime_work
    from groundloop.m5.runtime.postgres_readiness import (
        _lock_accounting,
        _resolve_prior_pending,
    )
    from groundloop.m5.runtime.postgres_recovery import (
        _TIMING_COVERAGE_COLUMNS,
        _TIMING_SUM_COLUMNS,
        _timing_accumulator_from_row,
    )

    sealed = revision + 1
    with connection.transaction(), connection.cursor() as cursor:
        for table in (
            "groundloop_runtime_mode",
            "groundloop_m4_publication_head",
            "groundloop_m5_publication_head",
        ):
            cursor.execute(
                sql.SQL("SELECT * FROM {} WHERE singleton FOR UPDATE").format(
                    sql.Identifier(db.schema_name, table)
                )
            ).fetchone()
        base = cursor.execute(
            "SELECT revision,semantic_status FROM groundloop_epoch "
            "WHERE epoch_id=%s FOR UPDATE",
            (epoch,),
        ).fetchone()
        runtime = cursor.execute(
            "SELECT revision,runtime_state FROM groundloop_m5_runtime_epoch "
            "WHERE epoch_id=%s FOR UPDATE",
            (epoch,),
        ).fetchone()
        if base != (revision, "complete") or runtime != (revision, "semantic_complete"):
            raise InvalidEventError("SQL adapter requires genuine semantic readiness")
        for table, key in (
            ("groundloop_m5_owner_pending_counter", "owner_claim_id"),
            ("groundloop_m5_answer_pending_counter", "answer_version_id"),
        ):
            cursor.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE epoch_id=%s "
                    'ORDER BY {} COLLATE "C" FOR UPDATE'
                ).format(sql.Identifier(db.schema_name, table), sql.Identifier(key)),
                (epoch,),
            ).fetchall()
        prior, timing = _lock_accounting(cursor, db.schema_name, epoch, revision)
        assert timing.has_pending_anchor
        assert timing.pending_contribution_kind == "semantic_readiness"
        assert timing.pending_source_id == "semantic_complete"
        assert timing.pending_anchor_revision == revision
        pending_coordinates = (
            timing.pending_contribution_kind,
            timing.pending_source_id,
            timing.pending_contribution_key_digest,
            timing.pending_anchor_revision,
        )
        cut("after_accounting_locks")
        promote_matching_overlay(
            cursor, epoch_id=epoch, expected_revision=revision, sealed_revision=sealed
        )
        deactivation = cursor.execute(
            "SELECT d.group_version_id,d.action,d.successor_group_version_id,"
            "v.group_family_id FROM groundloop_m5_group_deactivation d "
            "JOIN groundloop_m5_group_version v USING(group_version_id) "
            "WHERE d.epoch_id=%s AND d.event_id=%s",
            (epoch, plan.structural_event_id),
        ).fetchone()
        assert deactivation is not None
        old_group, action, successor, family = deactivation
        assert cursor.execute(
            "SELECT certificate_digest FROM groundloop_m5_group_state_materialized "
            "WHERE group_version_id=%s",
            (old_group,),
        ).fetchone() == (None,)
        requirements = [
            r[0]
            for r in cursor.execute(
                "SELECT requirement_version_id FROM groundloop_m5_requirement_version "
                'WHERE group_version_id=%s ORDER BY requirement_version_id COLLATE "C"',
                (old_group,),
            ).fetchall()
        ]
        cursor.execute(
            "UPDATE groundloop_m5_group_validity SET valid_to_epoch=%s "
            "WHERE group_version_id=%s AND valid_to_epoch IS NULL",
            (epoch, old_group),
        )
        state_counts = {kind: 0 for kind in ("requirement", "group", "claim", "answer")}
        for kind, key, keys in (
            ("requirement", "requirement_version_id", requirements),
            ("group", "group_version_id", [old_group]),
        ):
            state_counts[kind] += cursor.execute(
                sql.SQL(
                    "UPDATE {} SET valid_to_epoch=%s WHERE {}=ANY(%s) "
                    "AND valid_to_epoch IS NULL"
                ).format(
                    sql.Identifier(
                        db.schema_name, "groundloop_m5_published_" + kind + "_state"
                    ),
                    sql.Identifier(key),
                ),
                (epoch, keys),
            ).rowcount
            state_counts[kind] += cursor.execute(
                sql.SQL("DELETE FROM {} WHERE {}=ANY(%s)").format(
                    sql.Identifier(
                        db.schema_name, "groundloop_m5_" + kind + "_state_materialized"
                    ),
                    sql.Identifier(key),
                ),
                (keys,),
            ).rowcount
        if action == "RETIRE":
            assert successor is None
            cursor.execute(
                "INSERT INTO groundloop_m5_group_family_retirement VALUES (%s,%s,%s)",
                (family, epoch, plan.structural_event_id),
            )
        else:
            assert action == "REPLACE" and successor is not None
            cursor.execute(
                "UPDATE groundloop_m5_group_version SET lifecycle_state='PUBLISHED' "
                "WHERE group_version_id=%s",
                (successor,),
            )
            cursor.execute(
                "UPDATE groundloop_m5_requirement_version "
                "SET lifecycle_state='PUBLISHED' "
                "WHERE group_version_id=%s",
                (successor,),
            )
            cursor.execute(
                "INSERT INTO groundloop_m5_group_validity "
                "(group_version_id,group_family_id,claim_id,semantic_structure_hash,"
                "supersedes_group_version_id,valid_from_epoch,valid_to_epoch) "
                "SELECT v.group_version_id,v.group_family_id,f.claim_id,"
                "v.semantic_structure_hash,v.supersedes_group_version_id,%s,NULL "
                "FROM groundloop_m5_group_version v JOIN groundloop_m5_group_family f "
                "USING(group_family_id) WHERE v.group_version_id=%s",
                (epoch, successor),
            )
        for kind, key in (
            ("requirement", "requirement_version_id"),
            ("group", "group_version_id"),
            ("claim", "claim_id"),
            ("answer", "answer_version_id"),
        ):
            working = "groundloop_m5_working_" + kind + "_state"
            keys = [
                r[0]
                for r in cursor.execute(
                    sql.SQL("SELECT {} FROM {} WHERE epoch_id=%s").format(
                        sql.Identifier(key), sql.Identifier(db.schema_name, working)
                    ),
                    (epoch,),
                ).fetchall()
            ]
            if not keys:
                continue
            columns = [
                r[0]
                for r in cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s "
                    "AND column_name NOT IN ('epoch_id','updated_revision') "
                    "ORDER BY ordinal_position",
                    (db.schema_name, working),
                ).fetchall()
            ]
            col = sql.SQL(",").join(map(sql.Identifier, columns))
            published = sql.Identifier(
                db.schema_name, "groundloop_m5_published_" + kind + "_state"
            )
            materialized = sql.Identifier(
                db.schema_name, "groundloop_m5_" + kind + "_state_materialized"
            )
            state_counts[kind] += cursor.execute(
                sql.SQL(
                    "UPDATE {} SET valid_to_epoch=%s WHERE {}=ANY(%s) "
                    "AND valid_to_epoch IS NULL"
                ).format(published, sql.Identifier(key)),
                (epoch, keys),
            ).rowcount
            state_counts[kind] += cursor.execute(
                sql.SQL("DELETE FROM {} WHERE {}=ANY(%s)").format(
                    materialized, sql.Identifier(key)
                ),
                (keys,),
            ).rowcount
            state_counts[kind] += cursor.execute(
                sql.SQL(
                    "INSERT INTO {} "
                    "({},valid_from_epoch,valid_to_epoch,sealed_revision) "
                    "SELECT {},%s,NULL,%s FROM {} WHERE epoch_id=%s"
                ).format(published, col, col, sql.Identifier(db.schema_name, working)),
                (epoch, sealed, epoch),
            ).rowcount
            state_counts[kind] += cursor.execute(
                sql.SQL(
                    "INSERT INTO {} ({},updated_epoch,updated_revision) "
                    "SELECT {},%s,%s FROM {} WHERE epoch_id=%s"
                ).format(
                    materialized, col, col, sql.Identifier(db.schema_name, working)
                ),
                (epoch, sealed, epoch),
            ).rowcount
        cursor.execute(
            "UPDATE groundloop_m4_publication_head SET epoch_id=%s WHERE singleton",
            (epoch,),
        )
        cursor.execute(
            "UPDATE groundloop_m5_publication_head "
            "SET epoch_id=%s,sealed_revision=%s WHERE singleton",
            (epoch, sealed),
        )
        cursor.execute(
            "UPDATE groundloop_epoch SET revision=%s,semantic_status='sealed',"
            "evaluation_state='complete',publication_mode='strict',"
            "sealed_at=clock_timestamp() "
            "WHERE epoch_id=%s AND revision=%s",
            (sealed, epoch, revision),
        )
        cut("after_half_terminal")
        before_pending = cursor.execute(
            "SELECT pending_contribution_kind,pending_source_id,"
            "pending_contribution_key_digest,pending_anchor_revision "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        assert before_pending == pending_coordinates
        children = _prepare_preterminal_matching_publication_children(
            cursor, epoch_id=epoch, expected_revision=revision, sealed_revision=sealed
        )
        assert (
            cursor.execute(
                "SELECT pending_contribution_kind,pending_source_id,"
                "pending_contribution_key_digest,pending_anchor_revision "
                "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
                (epoch,),
            ).fetchone()
            == before_pending
        )
        cut("after_preparation")
        publication = stable_m4_digest("m4-publication-v1", str(epoch))
        combined = digests.combined_status_delta_set_digest(children.combined_deltas)
        changed = digests.changed_state_set_digest(
            r.reference_digest for r in children.changed_state_references
        )
        source_bytes = digests.stable_m5_preimage(
            "m5-seal-contribution-source-v1",
            text_field(plan.structural_event_id),
            hash_field(combined),
            hash_field(changed),
            text_field(publication),
        )
        source = hashlib.sha256(source_bytes).hexdigest()
        key_bytes = digests.stable_m5_preimage(
            "m5-runtime-work-contribution-key-v1",
            int_field(epoch),
            enum_field("seal"),
            text_field(plan.structural_event_id),
        )
        key = hashlib.sha256(key_bytes).hexdigest()
        work = M5RuntimeWork(
            group_state_write_count=state_counts["group"],
            claim_state_write_count=state_counts["claim"],
            answer_state_write_count=state_counts["answer"],
            public_delta_count=len(children.combined_deltas),
            bytes_hashed=len(source_bytes) + len(key_bytes),
            bytes_serialized=len(source_bytes) + len(key_bytes),
        )
        total = M5RuntimeWork(
            **dict(
                zip(
                    prior.counter_names(),
                    (
                        a + b
                        for a, b in zip(
                            prior.counter_values(), work.counter_values(), strict=True
                        )
                    ),
                    strict=True,
                )
            )
        )
        names = (
            "epoch_id",
            "contribution_kind",
            "source_id",
            "source_identity_hash",
            "contribution_key_digest",
            "applied_revision",
            *work.counter_names(),
            "work_digest",
        )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_runtime_work_contribution ({}) VALUES ({})"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, names)),
                sql.SQL(",").join(sql.Placeholder() for _ in names),
            ),
            (
                epoch,
                "seal",
                plan.structural_event_id,
                source,
                key,
                sealed,
                *work.counter_values(),
                work.work_digest,
            ),
        )
        cut("after_contribution")
        _resolve_prior_pending(cursor, db.schema_name, epoch, timing)
        cut("after_prior_missing")
        cursor.execute(
            "UPDATE groundloop_m5_runtime_epoch SET revision=%s,runtime_state='sealed',"
            "terminal_at=clock_timestamp() WHERE epoch_id=%s AND revision=%s",
            (sealed, epoch, revision),
        )
        for table in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            cursor.execute(
                sql.SQL("UPDATE {} SET updated_revision=%s WHERE epoch_id=%s").format(
                    sql.Identifier(db.schema_name, table)
                ),
                (sealed, epoch),
            )
        cut("after_terminal_runtime")
        assignments = sql.SQL(",").join(
            sql.SQL("{}=%s").format(sql.Identifier(n)) for n in total.counter_names()
        )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_work_accumulator SET {},"
                    "work_digest=%s,updated_revision=%s,terminalized=true "
                    "WHERE epoch_id=%s AND updated_revision=%s AND work_digest=%s "
                    "AND NOT terminalized"
                ).format(assignments),
                (
                    *total.counter_values(),
                    total.work_digest,
                    sealed,
                    epoch,
                    revision,
                    prior.work_digest,
                ),
            ).rowcount
            == 1
        )
        cut("after_work_cas")
        increments = []
        for prefix in (
            "required",
            "postgres_server_execution",
            "postgres_lock_wait",
            "postgres_wal_bytes",
            "postgres_shared_block_reads",
        ):
            increments.extend(
                (
                    sql.SQL("{}={}+1").format(
                        sql.Identifier(prefix + "_expected_count"),
                        sql.Identifier(prefix + "_expected_count"),
                    ),
                    sql.SQL("{}={}+2").format(
                        sql.Identifier(prefix + "_missing_count"),
                        sql.Identifier(prefix + "_missing_count"),
                    ),
                )
            )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_timing_accumulator SET {},"
                    "updated_revision=%s,terminalized=true,pending_contribution_kind=NULL,"
                    "pending_source_id=NULL,pending_contribution_key_digest=NULL,"
                    "pending_anchor_revision=NULL WHERE epoch_id=%s "
                    "AND updated_revision=%s AND NOT terminalized"
                ).format(sql.SQL(",").join(increments)),
                (sealed, epoch, revision),
            ).rowcount
            == 1
        )
        cut("after_timing_cas")
        row = cursor.execute(
            "SELECT "
            + ",".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
            + ",updated_revision,terminalized,pending_contribution_kind,"
            "pending_source_id,"
            "pending_contribution_key_digest,pending_anchor_revision "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        event_timing, coverage = _timing_accumulator_from_row(row).project(
            pending_as_missing=False
        )
        for kind, vector in (("event", total), ("call", work)):
            _insert_runtime_work(
                cursor,
                structural_event_id=plan.structural_event_id,
                epoch_id=epoch,
                work_kind=kind,
                work=vector,
            )
        result = M5EventRunResult.build(
            event_id=plan.structural_event_id,
            payload_hash=plan.payload_hash,
            epoch_id=epoch,
            state=M5RunState.SEALED,
            replayed_outcome=None,
            open_receipt=OpenEventReceipt(epoch, False, False),
            publication_receipt=PublicationReceipt(epoch, publication, False),
            event_work=total,
            call_work=work,
            event_timing=event_timing,
            call_timing=M5RuntimeTiming(),
            combined_deltas=children.combined_deltas,
            changed_state_references=children.changed_state_references,
            failure_reason=None,
            event_timing_coverage=coverage,
            call_timing_coverage=M5RuntimeTimingCoverage.single_point(
                None, terminal_client_roundtrip_included=False
            ),
        )
        result_names = (
            "structural_event_id",
            "payload_hash",
            "epoch_id",
            "outcome",
            "original_open_receipt_binding_hash",
            "publication_id",
            "original_publication_receipt_binding_hash",
            "event_work_kind",
            "event_work_digest",
            "combined_status_delta_set_hash",
            "changed_state_set_hash",
            "failure_reason",
            "logical_result_hash",
            "delta_count",
            "state_reference_count",
            *_TIMING_SUM_COLUMNS,
        )
        result_values = (
            plan.structural_event_id,
            plan.payload_hash,
            epoch,
            "sealed",
            digests.open_event_receipt_binding_digest(
                epoch_id=epoch,
                replayed=False,
                already_sealed=False,
                publication_id=None,
                already_failed=False,
                failure_reason=None,
            ),
            publication,
            digests.publication_receipt_binding_digest(
                epoch_id=epoch, publication_id=publication, replayed=False
            ),
            "event",
            total.work_digest,
            combined,
            changed,
            None,
            result.logical_result_hash,
            len(children.combined_deltas),
            len(children.changed_state_references),
            *(getattr(event_timing, n) for n in _TIMING_SUM_COLUMNS),
        )
        cursor.execute(
            sql.SQL("INSERT INTO groundloop_m5_event_result ({}) VALUES ({})").format(
                sql.SQL(",").join(map(sql.Identifier, result_names)),
                sql.SQL(",").join(sql.Placeholder() for _ in result_names),
            ),
            result_values,
        )
        assert (
            build_matching_publication_children(
                cursor, epoch_id=epoch, sealed_revision=sealed
            )
            == children
        )
        for ordinal, delta in enumerate(children.combined_deltas):
            cursor.execute(
                "INSERT INTO groundloop_m5_event_result_delta "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan.structural_event_id,
                    ordinal,
                    delta.object_type,
                    delta.object_id,
                    delta.old_status,
                    delta.new_status,
                    delta.reason,
                ),
            )
        for ordinal, reference in enumerate(children.changed_state_references):
            cursor.execute(
                "INSERT INTO groundloop_m5_event_result_state_reference "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    plan.structural_event_id,
                    ordinal,
                    reference.kind.value,
                    reference.object_id,
                    reference.epoch_id,
                    reference.revision,
                    reference.state_artifact_hash,
                    reference.reference_digest,
                ),
            )
        coverage_names = (
            "structural_event_id",
            "epoch_id",
            *_TIMING_COVERAGE_COLUMNS,
            "terminal_client_roundtrip_included",
        )
        cursor.execute(
            sql.SQL(
                "INSERT INTO groundloop_m5_event_timing_coverage ({}) VALUES ({})"
            ).format(
                sql.SQL(",").join(map(sql.Identifier, coverage_names)),
                sql.SQL(",").join(sql.Placeholder() for _ in coverage_names),
            ),
            (
                plan.structural_event_id,
                epoch,
                *(getattr(coverage, n) for n in _TIMING_COVERAGE_COLUMNS),
                False,
            ),
        )
        cut("after_result_children")
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        cut("after_constraints")
        return result
