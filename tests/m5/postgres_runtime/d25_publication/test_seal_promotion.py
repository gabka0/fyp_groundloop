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
