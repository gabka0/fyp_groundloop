"""Separate-connection serializer tests with observed blocking and wakeup."""

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any

import pytest

from groundloop.errors import EventConflictError
from tests.m5.postgres_runtime.d34_requirement_completion.conftest import capture_sql


@pytest.mark.parametrize("order", [0, 1])
@pytest.mark.parametrize(
    "case", ["duplicate", "two-fresh", "fresh-replay", "active-inactive"]
)
def test_real_serializer_orders_after_wait_classification(
    c2_db: Any, order: int, case: str
) -> None:
    db = c2_db
    a, b = (
        db.build(chunk, label)
        for chunk, label in zip(db.chunks[:2], ("SUPPORT", "NEUTRAL"), strict=True)
    )
    if case == "active-inactive":
        b = db.build(db.chunks[2], "SUPPORT")
        with db.connection.transaction():
            db.connection.execute(
                "UPDATE groundloop_chunk_version SET valid_to_epoch=%s "
                "WHERE chunk_version_id=%s",
                (db.runtime.epoch_id, db.chunks[2]),
            )
            db.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    if case == "fresh-replay":
        db.complete(a)
    operations = [a, a if case == "duplicate" else b]
    if case == "fresh-replay":
        operations = [a, b]  # retained A versus fresh B
    before = db.revision()
    held, release, started = Event(), Event(), Event()
    outcomes: dict[int, Any] = {}
    traces: dict[int, Any] = {}
    with db.runtime.two_connections() as connections:
        pids = [c.info.backend_pid for c in connections]
        leader = order

        def run(index: int) -> Any:
            def pause(stage: str) -> None:
                target = (
                    "d34_retained_classified"
                    if case == "fresh-replay" and index == 0
                    else "verifier_output_reserved"
                )
                if index == leader and stage == target:
                    held.set()
                    assert release.wait(15), "test did not release serializer holder"

            started.set()
            with capture_sql(connections[index]) as trace:
                outcomes[index] = db.complete(
                    operations[index],
                    connection=connections[index],
                    expected_revision=before,
                    failure_injector=pause,
                )
                traces[index] = trace
            return outcomes[index]

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(run, leader)
            try:
                assert held.wait(10)
                second = executor.submit(run, 1 - leader)
                deadline = time.monotonic() + 10
                blocked = False
                while time.monotonic() < deadline:
                    blockers = db.connection.execute(
                        "SELECT pg_blocking_pids(%s)", (pids[1 - leader],)
                    ).fetchone()[0]
                    if pids[leader] in blockers:
                        blocked = True
                        break
                    time.sleep(0.02)
                assert blocked, "no actual wait on the held serializer was observed"
            finally:
                release.set()
            first.result(timeout=15)
            second.result(timeout=15)
    assert started.is_set()
    if case == "duplicate":
        assert outcomes[leader].exact_replay is False
        assert outcomes[1 - leader].exact_replay is True
        assert db.revision() == before + 1
    elif case in {"two-fresh", "active-inactive"}:
        assert all(not result.exact_replay for result in outcomes.values())
        assert db.revision() == before + 2
        assert sorted(r.resulting_revision for r in outcomes.values()) == [
            before + 1,
            before + 2,
        ]
        if case == "active-inactive":
            assert db.connection.execute(
                "SELECT count(*) FROM groundloop_m5_matching_work_contribution "
                "WHERE epoch_id=%s AND source_kind='requirement_completion'",
                (db.runtime.epoch_id,),
            ).fetchone() == (1,)
            assert db.connection.execute(
                "SELECT eligible_for_currency FROM groundloop_semantic_observation "
                "WHERE observation_id=%s",
                (b.verifier_artifact.to_semantic_observation().observation_id,),
            ).fetchone() == (False,)
            for family in ("owner", "answer"):
                assert db.connection.execute(
                    "SELECT verifier_job_count FROM "
                    f"groundloop_m5_{family}_pending_counter "
                    "WHERE epoch_id=%s",
                    (db.runtime.epoch_id,),
                ).fetchone() == (1,)
            assert db.connection.execute(
                "SELECT count(*) FROM groundloop_m5_runtime_work_contribution "
                "WHERE epoch_id=%s AND contribution_kind='verifier_completion'",
                (db.runtime.epoch_id,),
            ).fetchone() == (2,)
    else:
        assert outcomes[0].exact_replay and not outcomes[1].exact_replay
        assert db.revision() == before + 1
    for index, result in outcomes.items():
        sqls = [s for s, _, _ in traces[index]]
        fresh = [
            s
            for s in sqls
            if s.lstrip().startswith(
                "SELECT groundloop_m5_authorize_persisted_matching_transition("
            )
        ]
        if result.exact_replay:
            assert fresh == []
            assert not any(
                s.lstrip()
                .upper()
                .startswith(("INSERT", "UPDATE", "DELETE", "SET CONSTRAINTS"))
                for s in sqls
            )
            assert not any(
                "FROM groundloop_semantic_observation" in s and "FOR UPDATE" in s
                for s in sqls
            )
        elif not (case == "active-inactive" and index == 1):
            assert len(fresh) == 1
        else:
            assert fresh == []


@pytest.mark.parametrize("c2_db", ["representative"], indirect=True)
def test_old_retained_source_and_new_dense_representative_fk_make_guarded_progress(
    c2_db: Any,
) -> None:
    """Old sealed replay versus a new real DELETE/fallback C2 certificate."""
    from psycopg.errors import LockNotAvailable

    from groundloop.m5.runtime.postgres_verifier import (
        _d34_complete_m5_verifier_atomically,
    )
    from tests.m5.postgres_runtime.d34_requirement_completion import test_history as h

    db = c2_db
    h._seal_genuine_baseline(db)
    old_source, old_args, _ = next(
        call for call in db.calls if call[0].job.pair.chunk_version_id == db.chunks[0]
    )
    old_epoch = old_args[0]
    old_observation = (
        old_source.verifier_artifact.to_semantic_observation().observation_id
    )
    h._open_genuine_delete_fallback(
        db,
        requirement_ordinal=1,
        selected_chunks=("c2-D",),
    )
    assert db.runtime.epoch_id != old_epoch
    fresh_source = db.build("c2-D", "SUPPORT")
    old_held, new_held, old_release, new_release = Event(), Event(), Event(), Event()
    traces: dict[str, Any] = {}

    with db.runtime.two_connections() as connections:
        old_connection, new_connection = connections

        def old_replay() -> Any:
            def pause(stage: str) -> None:
                if stage == "d34_retained_source_locked":
                    old_held.set()
                    assert old_release.wait(30)

            with capture_sql(old_connection) as trace:
                result = _d34_complete_m5_verifier_atomically(
                    old_connection,
                    *old_args,
                    failure_injector=pause,
                )
                traces["old"] = trace
            return result

        def new_complete() -> Any:
            def pause(stage: str) -> None:
                if stage == "d34_matching_staged":
                    new_held.set()
                    assert new_release.wait(30)

            with capture_sql(new_connection) as trace:
                result = db.complete(
                    fresh_source,
                    connection=new_connection,
                    failure_injector=pause,
                )
                traces["new"] = trace
            return result

        def observation_probe() -> None:
            with db.runtime.reconnect() as probe:
                with probe.transaction():
                    assert probe.execute(
                        "SELECT observation_id FROM groundloop_semantic_observation "
                        "WHERE observation_id=%s FOR UPDATE NOWAIT",
                        (old_observation,),
                    ).fetchone() == (old_observation,)

        with ThreadPoolExecutor(max_workers=2) as executor:
            old_future = executor.submit(old_replay)
            try:
                assert old_held.wait(15)
                observation_probe()  # Retained source locks do NOT row-lock O.
                new_future = executor.submit(new_complete)
                assert new_held.wait(15)
                assert db.connection.execute(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity "
                    "WHERE pid=%s AND state='idle in transaction'",
                    (new_connection.info.backend_pid,),
                ).fetchone() == (1,)
                with pytest.raises(LockNotAvailable):
                    observation_probe()  # New dense artifact's real immediate FK.
                old_release.set()
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    blockers = db.connection.execute(
                        "SELECT pg_catalog.pg_blocking_pids(%s)",
                        (old_connection.info.backend_pid,),
                    ).fetchone()[0]
                    if new_connection.info.backend_pid in blockers:
                        break
                    time.sleep(0.02)
                else:
                    pytest.fail(
                        "old retained replay did not wait on the new native image"
                    )
            finally:
                old_release.set()
                new_release.set()
            new_result = new_future.result(timeout=20)
            old_result = old_future.result(timeout=20)
    assert not new_result.exact_replay and old_result.exact_replay
    assert old_result.transition_anchor is None
    assert not any(
        s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "SET CONSTRAINTS"))
        or "matching_read_transition_" in s
        or "FROM groundloop_semantic_observation" in s
        and "FOR UPDATE" in s
        for s, _, _ in traces["old"]
    )
    expected = h.expected_currency_from_sources(db)
    expected = {key: value for key, value in expected.items() if key[2] != db.chunks[2]}
    # DELETE records explicit absence rather than silently omitting old currency.
    removed = next(
        call[0].verifier_artifact.to_semantic_observation()
        for call in db.calls
        if call[1][0] == old_epoch and call[0].job.pair.chunk_version_id == db.chunks[2]
    )
    expected[
        ("requirement", removed.subject_id, removed.chunk_version_id, removed.task_type)
    ] = None
    h.assert_currency_sources_and_raw_history(db, expected)
    h._assert_complete_point(db, db.runtime.epoch_id, db.revision(), expected)


def test_actual_execution_then_verifier_replay_cannot_invert_fresh_archive_preflight(
    c2_db: Any,
) -> None:
    """Real native replay held at E-before-V versus a negative fresh alias map."""
    import psycopg
    from psycopg.errors import LockNotAvailable

    from groundloop.m5.runtime import postgres_matching as matching
    from groundloop.m5.runtime.postgres_roots import _lock_epoch
    from groundloop.m5.runtime.postgres_verifier import (
        _d34_complete_m5_verifier_atomically,
    )
    from tests.m5.postgres_runtime.d34_requirement_completion import (
        test_archive_order as archive,
    )

    db = c2_db
    old, old_args, old_map, _, fresh_map = archive._old_and_fresh_component_maps(db)
    foreign = next(
        c
        for c in old_map
        if c.relation == "groundloop_m5_requirement_verifier_artifact"
        and c.columns == ("artifact_id",)
    )
    combined = tuple(
        foreign if (c.relation, c.columns) == (foreign.relation, foreign.columns) else c
        for c in fresh_map
    )
    held, release = Event(), Event()
    with db.runtime.two_connections() as connections:
        old_connection, fresh_connection = connections
        original = old_connection.cursor_factory

        class PauseAfterExecution(psycopg.Cursor):
            def execute(self, query: Any, params: Any = None, **kw: Any) -> Any:
                text = (
                    query
                    if isinstance(query, str)
                    else query.as_string(self.connection)
                )
                value = super().execute(query, params, **kw)
                if (
                    "FROM groundloop_m5_requirement_verifier_execution" in text
                    and "FOR UPDATE" in text
                ):
                    held.set()
                    assert release.wait(30)
                return value

        def replay() -> Any:
            old_connection.cursor_factory = PauseAfterExecution
            try:
                return _d34_complete_m5_verifier_atomically(old_connection, *old_args)
            finally:
                old_connection.cursor_factory = original

        def fresh_component() -> Any:
            with capture_sql(fresh_connection) as trace:
                with (
                    fresh_connection.transaction(),
                    fresh_connection.cursor() as cursor,
                ):
                    header = _lock_epoch(cursor, db.runtime.epoch_id)
                    matching._authorize_checked_prefix(
                        cursor,
                        epoch_id=header.epoch_id,
                        expected_revision=header.revision,
                    )
                    matching._d34_reserve_archive(cursor, combined)
                    with pytest.raises(EventConflictError, match="occupied"):
                        matching._d34_preflight_archive(cursor, combined)
            return trace

        with ThreadPoolExecutor(max_workers=2) as executor:
            old_future = executor.submit(replay)
            try:
                assert held.wait(15)
                # Establish the exact dangerous intermediate lock order with
                # actual separate connections: E held, V still independently lockable.
                with db.runtime.reconnect() as probe:
                    with pytest.raises(LockNotAvailable):
                        with probe.transaction():
                            probe.execute(
                                "SELECT artifact_id FROM "
                                "groundloop_m5_requirement_verifier_execution "
                                "WHERE artifact_id=%s FOR UPDATE NOWAIT",
                                (old.verifier_artifact.artifact_id,),
                            )
                    with probe.transaction():
                        assert probe.execute(
                            "SELECT artifact_id FROM "
                            "groundloop_m5_requirement_verifier_artifact "
                            "WHERE artifact_id=%s FOR UPDATE NOWAIT",
                            (old.verifier_artifact.artifact_id,),
                        ).fetchone() == (old.verifier_artifact.artifact_id,)
                fresh_future = executor.submit(fresh_component)
                trace = fresh_future.result(timeout=5)
                assert not old_future.done()
                assert not any(
                    "FOR UPDATE" in s
                    and (
                        "requirement_verifier_artifact" in s
                        or "requirement_verifier_execution" in s
                    )
                    or s.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
                    for s, _, _ in trace
                )
            finally:
                release.set()
            assert old_future.result(timeout=15).exact_replay
