"""B33 one-operation promotion: unit coverage is not live journal evidence."""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace
from typing import Any, cast

import psycopg
import pytest
from psycopg import sql

from groundloop.errors import InvalidEventError, ValidationError
from groundloop.m4.application import OpenEventReceipt
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m4.pipeline import M4ExecutionMode, PostgresM4ApplicationPorts
from groundloop.m5.digests import enum_field, hash_field, int_field, text_field
from groundloop.m5.reference import (
    compute_reference_states,
    derive_active_requirement_witnesses,
)
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshotEntry,
    M5EventRunResult,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
)
from groundloop.m5.runtime.persistence import (
    _insert_runtime_work,
    _load_terminal_result,
)
from groundloop.m5.runtime.postgres_matching_publication import (
    _prepare_preterminal_matching_publication_children,
    build_matching_publication_children,
    promote_matching_overlay,
)
from groundloop.m5.runtime.postgres_readiness import (
    _lock_accounting,
    _resolve_prior_pending,
)
from groundloop.m5.runtime.postgres_recovery import (
    _TIMING_COVERAGE_COLUMNS,
    _TIMING_SUM_COLUMNS,
    _timing_accumulator_from_row,
)
from groundloop.postgres.m5 import read_m5_mismatch_counts
from tests.m5.postgres.helpers import (
    insert_observation,
    insert_published_group,
    install_current_currency,
    make_group,
    sha,
)
from tests.m5.postgres_runtime import test_semantic_readiness as r33
from tests.m5.postgres_runtime.d24_application.conftest import database_snapshot
from tests.m5.postgres_runtime.d25_publication import test_seal_promotion as group_seal
from tests.m5.postgres_runtime.d33_document_completion import (
    test_document_open_prerequisite as o33,
)
from tests.m5.postgres_runtime.d33_document_completion import (
    test_document_seal as j33,
)
from tests.m5.postgres_runtime.d34_requirement_completion import (
    conftest as c2_fixtures,
)
from tests.m5.postgres_runtime.d34_requirement_completion import (
    test_history as c2_history,
)

open_db = o33.open_db
readiness_db = r33.readiness_db
c2_db = c2_fixtures.c2_db

FAMILIES = (
    ("observation", "observation", ("observation_id",), "working.present"),
    (
        "edge",
        "edge",
        ("requirement_version_id", "text_hash"),
        "working.refcount > 0",
    ),
    ("hash_mask", "mask", ("group_version_id", "text_hash"), "working.mask > 0"),
    ("hall", "hall", ("group_version_id",), "working.present"),
)


class PromotionCursor:
    """Record actual issued SQL; synthetic counts test receipt arithmetic only."""

    def __init__(self, *, header_count: int = 1, fail_after: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = 0
        self.header_count = header_count
        self.fail_after = fail_after
        self.mutations = 0

    def execute(self, query: str, params: object = None) -> PromotionCursor:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        self.rowcount = 0
        if normalized.startswith("UPDATE groundloop_m5_matching_image_current"):
            self.rowcount = self.header_count
        else:
            for ordinal, (family, _, _, _) in enumerate(FAMILIES, 1):
                relation = "groundloop_m5_matching_" + family + "_current"
                if normalized.startswith("DELETE FROM " + relation):
                    self.rowcount = ordinal
                elif normalized.startswith("UPDATE " + relation):
                    self.rowcount = 2 * ordinal
                elif normalized.startswith("INSERT INTO " + relation):
                    self.rowcount = 3 * ordinal
        if normalized.startswith(("UPDATE ", "DELETE ", "INSERT ")):
            self.mutations += 1
            if self.mutations == self.fail_after:
                raise RuntimeError("B33 unit mutation cut")
        return self


def test_four_family_sql_partition_and_actual_rowcount_arithmetic() -> None:
    cursor = PromotionCursor()
    receipt = promote_matching_overlay(
        cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=5
    )
    assert len(cursor.calls) == 15
    assert cursor.calls[:2] == [
        ("SELECT groundloop_m5_authorize_checked_transition(%s,%s)", (17, 4)),
        (
            "SELECT groundloop_m5_authorize_persisted_matching_seal(%s,%s,%s)",
            (17, 4, 5),
        ),
    ]
    assert cursor.calls[2][0].startswith("UPDATE groundloop_m5_matching_image_current")
    for ordinal, (family, receipt_name, keys, positive) in enumerate(FAMILIES, 1):
        delete, update, insert = cursor.calls[3 * ordinal : 3 * ordinal + 3]
        relation = "groundloop_m5_matching_" + family + "_current"
        working = "groundloop_m5_matching_" + family + "_working"
        assert delete[0].startswith("DELETE FROM " + relation)
        assert update[0].startswith("UPDATE " + relation + " AS current_row SET ")
        assert insert[0].startswith("INSERT INTO " + relation)
        assert delete[1] == (17,)
        assert update[1] == insert[1] == (17, 5, 17)
        for query, _ in (update, insert):
            assert "FROM " + working + " AS working" in query
            assert "WHERE working.epoch_id = %s AND " + positive in query
            for key in keys:
                assert "current_row." + key + " = working." + key in query
            assert "ON CONFLICT" not in query
        assert "installed_epoch_id = %s, installed_revision = %s" in update[0]
        assert "IS DISTINCT FROM" not in update[0]  # Value-equal overlaps also write.
        assert "AND NOT EXISTS ( SELECT 1 FROM " + relation in insert[0]
        assert "ORDER BY working." in insert[0]
        assert 'COLLATE "C"' in insert[0]
        assert getattr(receipt, receipt_name + "_writes") == 5 * ordinal
        assert getattr(receipt, receipt_name + "_deletes") == ordinal
    joined = " ".join(query for query, _ in cursor.calls).upper()
    for forbidden in (
        "COMMIT",
        "ROLLBACK",
        "SET CONSTRAINTS",
        "FOR UPDATE",
        "PG_TEMP",
        "PUBLICATION_HEAD",
        "EVENT_RESULT",
        "ORACLE",
        "ON CONFLICT",
    ):
        assert forbidden not in joined


@pytest.mark.parametrize("cut", range(1, 14))
def test_unit_failure_propagates_at_every_statement_without_more_sql(cut: int) -> None:
    cursor = PromotionCursor(fail_after=cut)
    with pytest.raises(RuntimeError, match="B33 unit mutation cut"):
        promote_matching_overlay(
            cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=5
        )
    assert cursor.mutations == cut
    assert len(cursor.calls) == cut + 2


@pytest.mark.parametrize("header_count", (0, 2))
def test_bad_header_cardinality_stops_before_family_writes(header_count: int) -> None:
    cursor = PromotionCursor(header_count=header_count)
    with pytest.raises(ValidationError, match="one current image header"):
        promote_matching_overlay(
            cast(Any, cursor), epoch_id=17, expected_revision=4, sealed_revision=5
        )
    assert cursor.mutations == 1
    assert len(cursor.calls) == 3


class _B33Rollback(RuntimeError):
    pass


def _incomplete_foundation(db: o33.OpenDatabase) -> None:
    """Raw domain facts BEFORE activation; no matching/runtime/working seeding."""
    assert db.group is None
    connection = db.connection
    group = make_group(
        group_id="b33-incomplete-group",
        family_id="b33-incomplete-family",
        claim_id=db.base.claim_ids[0],
        texts=("alpha-or-gamma", "alpha-only", "missing"),
    )
    with connection.transaction():
        insert_published_group(connection, group=group, epoch_id=db.base.epoch_id)
        connection.execute(
            "INSERT INTO groundloop_document VALUES "
            "('b33-survivor','fixture://b33-survivor','test')"
        )
        connection.execute(
            "INSERT INTO groundloop_document_version VALUES "
            "('b33-survivor-v1','b33-survivor',%s,%s,NULL)",
            (sha("b33-survivor-source"), db.base.epoch_id),
        )
        remaining = []
        for ordinal, text in enumerate(("alpha", "gamma")):
            chunk = "b33-survivor-" + text
            connection.execute(
                "INSERT INTO groundloop_chunk_version VALUES "
                "(%s,'b33-survivor-v1',%s,%s,%s,'fixture-v1',%s,NULL)",
                (chunk, ordinal, text, sha(text), db.base.epoch_id),
            )
            connection.execute(
                "INSERT INTO groundloop_chunk_provenance "
                "(chunk_version_id,chunker_artifact_id,input_hash) "
                "VALUES (%s,'o33-chunker',%s)",
                (chunk, sha("b33-survivor-input")),
            )
            remaining.append(ActiveChunkSnapshotEntry(chunk, sha(text)))
        for observation, ordinal, chunk in (
            ("b33-withdrawn-r0", 0, db.base.chunk_ids[0]),
            ("b33-withdrawn-r1", 1, db.base.chunk_ids[0]),
            ("b33-survivor-r0-alpha", 0, "b33-survivor-alpha"),
            ("b33-survivor-r0-gamma", 0, "b33-survivor-gamma"),
        ):
            requirement = group.requirements[ordinal].requirement_version_id
            insert_observation(
                connection,
                observation_id=observation,
                subject_kind="requirement",
                subject_id=requirement,
                chunk_id=chunk,
                produced_epoch=db.base.epoch_id,
            )
            install_current_currency(
                connection,
                observation_id=observation,
                subject_kind="requirement",
                subject_id=requirement,
                chunk_id=chunk,
                task_type="verify_requirement_v1",
                epoch_id=db.base.epoch_id,
                revision=1,
            )
    db.group, db.remaining_chunks = group, tuple(remaining)
    # Rebuild from actual SQL after adding domain inputs; never mutate an old
    # cache/request to look like a native producer result.
    db.ports = PostgresM4ApplicationPorts(
        connection, structural_payloads={}, execution_mode=M4ExecutionMode.MEASURED
    )
    db.request = db.store.prepare_activation_request("b33-incomplete-activation")


def _image(connection: Any) -> dict[str, dict[tuple[Any, ...], tuple[Any, str]]]:
    result = {}
    for family, _, keys, _ in FAMILIES:
        rows = connection.execute(
            sql.SQL("SELECT to_jsonb(t),xmin::text FROM {} t").format(
                sql.Identifier("groundloop_m5_matching_" + family + "_current")
            )
        ).fetchall()
        result[family] = {
            tuple(row[k] for k in keys): (row, xmin) for row, xmin in rows
        }
    header = connection.execute(
        "SELECT to_jsonb(t),xmin::text FROM groundloop_m5_matching_image_current t"
    ).fetchall()
    assert len(header) == 1
    result["_header"] = {(True,): header[0]}
    return result


def _expected_image(db: o33.OpenDatabase, epoch: int) -> dict[str, Any]:
    """Full exhaustive raw-source oracle, never current/working matching input."""
    repo = o33._raw_python_point(db, epoch)
    return _matching_from_raw_repository(repo)


def _matching_from_raw_repository(repo: Any) -> dict[str, Any]:
    """Independent exhaustive model, including every group and matching key."""
    states = compute_reference_states(repo)
    witnesses = derive_active_requirement_witnesses(repo)
    observations, edges, masks, halls = {}, {}, {}, {}
    for witness in witnesses:
        requirement = repo.requirement(witness.requirement_version_id)
        common = dict(
            requirement_version_id=witness.requirement_version_id,
            group_version_id=requirement.group_version_id,
            requirement_ordinal=witness.requirement_ordinal,
            text_hash=witness.text_hash,
        )
        for observation in witness.active_observation_ids:
            observations[observation,] = dict(observation_id=observation, **common)
        edges[witness.requirement_version_id, witness.text_hash] = dict(
            **common, refcount=len(witness.active_observation_ids)
        )
        key = requirement.group_version_id, witness.text_hash
        masks[key] = masks.get(key, 0) | 1 << witness.requirement_ordinal
    for gid, state in states.groups.items():
        n = state.requirement_count
        histogram = [0] * (1 << n)
        for (group_id, _), mask in masks.items():
            if group_id == gid:
                histogram[mask] += 1
        neighbors = [
            sum(c for mask, c in enumerate(histogram) if mask & s)
            for s in range(1 << n)
        ]
        deficiencies = [s.bit_count() - neighbors[s] for s in range(1 << n)]
        halls[gid,] = dict(
            group_version_id=gid,
            requirement_count=n,
            mask_histogram=histogram,
            neighbor_counts=neighbors,
            deficiencies=deficiencies,
            maximum_deficiency=max(deficiencies),
            matching_size=n - max(deficiencies),
            distinct_hash_count=sum(histogram),
        )
    return dict(
        observation=observations,
        edge=edges,
        hash_mask={
            k: dict(group_version_id=k[0], text_hash=k[1], mask=v)
            for k, v in masks.items()
        },
        hall=halls,
    )


def _assert_complete_image(db: o33.OpenDatabase, epoch: int) -> None:
    expected = _expected_image(db, epoch)
    _assert_matching_payloads(db.connection, expected)


def _assert_matching_payloads(connection: Any, expected: dict[str, Any]) -> None:
    actual = _image(connection)
    for family in expected:
        assert {
            key: {
                k: v
                for k, v in row.items()
                if k not in {"installed_epoch_id", "installed_revision"}
            }
            for key, (row, _) in actual[family].items()
        } == expected[family]


def _recording_cursor(control: dict[str, Any]) -> Any:
    class RecordingCursor(psycopg.Cursor[Any]):
        def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
            result = super().execute(query, params, **kwargs)
            text = query if isinstance(query, str) else query.as_string(self.connection)
            normalized = " ".join(text.split())
            control.setdefault("all_sql", []).append(normalized)
            if normalized.startswith(
                (
                    "UPDATE groundloop_m5_matching_",
                    "INSERT INTO groundloop_m5_matching_",
                    "DELETE FROM groundloop_m5_matching_",
                )
            ):
                control.setdefault("mutations", []).append((normalized, self.rowcount))
                if control.get("cut") == len(control["mutations"]):
                    raise _B33Rollback("actual matching mutation cut")
            return result

    return RecordingCursor


def _assert_receipt_and_owner_journal(
    cursor: Any,
    before: Any,
    after: Any,
    receipt: Any,
    *,
    epoch: int,
    revision: int,
    inspect_journal: bool,
) -> None:
    """Independent working-key partition plus full physical before/final xmins."""
    expected_journal = {}
    for family, receipt_name, keys, _ in FAMILIES:
        prior, final = before[family], after[family]
        working = cursor.execute(
            sql.SQL("SELECT to_jsonb(w) FROM {} w WHERE epoch_id=%s").format(
                sql.Identifier("groundloop_m5_matching_" + family + "_working")
            ),
            (epoch,),
        ).fetchall()
        positive_column = {"edge": "refcount", "hash_mask": "mask"}.get(
            family, "present"
        )
        written = {
            tuple(row[k] for k in keys)
            for (row,) in working
            if row[positive_column] > 0
        }
        tombstones = {
            tuple(row[k] for k in keys)
            for (row,) in working
            if not row[positive_column]
        }
        deleted = tombstones & set(prior)
        assert set(final) == (set(prior) - deleted) | written
        for key in set(prior) - written - deleted:
            assert final[key] == prior[key]  # ALL untouched payloads/coordinates/xmin.
        for key in written & set(prior):
            assert final[key] != prior[key]  # Equal-payload carry must still UPDATE.
        assert getattr(receipt, receipt_name + "_deletes") == len(deleted)
        assert getattr(receipt, receipt_name + "_writes") == len(written)
        for key in written | deleted:
            old = prior.get(key, (None, None))[0]
            new = final.get(key, (None, None))[0]
            operation = (
                "DELETE" if new is None else "INSERT" if old is None else "UPDATE"
            )
            if new is not None:
                assert (new["installed_epoch_id"], new["installed_revision"]) == (
                    epoch,
                    revision,
                )
            expected_journal["groundloop_m5_matching_" + family + "_current", key] = (
                old,
                new,
                operation,
            )
    if not inspect_journal:
        return  # LOGIN legality is independently enforced by unchanged guards.
    rows = cursor.execute(
        "SELECT relation_name,key_preimage,first_old,final_new,"
        "first_operation,last_operation,"
        "mutation_count,saw_insert,saw_update,saw_delete "
        "FROM pg_temp.groundloop_m5_matching_promotion_journal"
    ).fetchall()
    seen = set()
    metadata = {
        "groundloop_m5_matching_" + f + "_current": keys for f, _, keys, _ in FAMILIES
    }
    header_count = 0
    for relation, key_bytes, old, new, first, last, count, ins, upd, dele in rows:
        if relation not in metadata:  # Header has its own one-row guard.
            assert relation == "groundloop_m5_matching_image_current"
            assert old == before["_header"][True,][0]
            assert new == after["_header"][True,][0]
            assert new["singleton"] is True
            assert (new["installed_epoch_id"], new["installed_revision"]) == (
                epoch,
                revision,
            )
            assert old != new
            fields = (relation, "true")
            assert bytes(key_bytes) == b"".join(
                len(field.encode()).to_bytes(8, "big") + field.encode()
                for field in fields
            )
            assert (first, last, count, ins, upd, dele) == (
                "UPDATE",
                "UPDATE",
                1,
                False,
                True,
                False,
            )
            header_count += 1
            continue
        value = new if new is not None else old
        identity = relation, tuple(value[k] for k in metadata[relation])
        fields = (relation, *identity[1])
        framed = b"".join(
            len(field.encode("utf8")).to_bytes(8, "big") + field.encode("utf8")
            for field in fields
        )
        assert bytes(key_bytes) == framed  # Independent exact key framing.
        assert identity not in seen
        seen.add(identity)
        expected_old, expected_new, operation = expected_journal[identity]
        assert (old, new, first, last, count, ins, upd, dele) == (
            expected_old,
            expected_new,
            operation,
            operation,
            1,
            operation == "INSERT",
            operation == "UPDATE",
            operation == "DELETE",
        )
    assert seen == set(expected_journal)
    assert header_count == 1


def _assert_driver_rowcounts(control: dict[str, Any], receipt: Any) -> None:
    """Actual driver DML rowcounts, separate from key-set/xmin/journal oracles."""
    rows = control["mutations"]
    assert len(rows) == 13
    assert rows[0][0].startswith("UPDATE groundloop_m5_matching_image_current")
    assert rows[0][1] == 1
    for index, (family, receipt_name, _, _) in enumerate(FAMILIES):
        relation = "groundloop_m5_matching_" + family + "_current"
        deletion, update, insertion = rows[1 + 3 * index : 4 + 3 * index]
        assert deletion[0].startswith("DELETE FROM " + relation)
        assert update[0].startswith("UPDATE " + relation)
        assert insertion[0].startswith("INSERT INTO " + relation)
        assert getattr(receipt, receipt_name + "_deletes") == deletion[1]
        assert getattr(receipt, receipt_name + "_writes") == update[1] + insertion[1]


@pytest.mark.parametrize("open_db", (False,), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_incomplete_document_existing_positive_journals_and_physical_receipt(
    open_db: o33.OpenDatabase,
    nonowner: bool,
) -> None:
    db = open_db
    _incomplete_foundation(db)
    plan, epoch, revision = j33._ready(db)
    _assert_complete_image(db, db.base.epoch_id)
    before = _image(db.connection)
    expected = _expected_image(db, epoch)
    group = db.group.group_version_id
    r0, r1, _ = (r.requirement_version_id for r in db.group.requirements)
    assert before["edge"][r0, sha("alpha")][0]["refcount"] == 2
    assert expected["edge"][r0, sha("alpha")]["refcount"] == 1
    assert (r1, sha("alpha")) not in expected["edge"]
    assert before["hash_mask"][group, sha("alpha")][0]["mask"] == 3
    assert expected["hash_mask"][group, sha("alpha")]["mask"] == 1
    assert before["hall"][group,][0]["matching_size"] == 2
    assert expected["hall"][group,]["matching_size"] == 1
    with j33._principal(db, nonowner) as ports:
        connection = ports.connection
        snapshot, cache = database_snapshot(connection), j33._cache(ports)
        control: dict[str, Any] = {}
        with pytest.raises(_B33Rollback, match="complete adapter image"):
            with (
                connection.transaction(),
                _recording_cursor(control)(connection) as cursor,
            ):
                ports_publication = j33.seal._stage_job_free_document_seal_local(
                    ports, cursor, epoch, revision, plan.direct_plan.update
                )
                assert ports_publication.epoch_id == epoch
                receipt = promote_matching_overlay(
                    cursor,
                    epoch_id=epoch,
                    expected_revision=revision,
                    sealed_revision=revision + 1,
                )
                after = _image(connection)
                _assert_receipt_and_owner_journal(
                    cursor,
                    before,
                    after,
                    receipt,
                    epoch=epoch,
                    revision=revision + 1,
                    inspect_journal=not nonowner,
                )
                assert (
                    after["edge"][r0, sha("gamma")] == before["edge"][r0, sha("gamma")]
                )
                assert (
                    after["hash_mask"][group, sha("gamma")]
                    == before["hash_mask"][group, sha("gamma")]
                )
                assert len(control["mutations"]) == 13
                for family in expected:
                    assert {
                        k: {
                            n: v
                            for n, v in row.items()
                            if n not in {"installed_epoch_id", "installed_revision"}
                        }
                        for k, (row, _) in after[family].items()
                    } == expected[family]
                raise _B33Rollback(
                    "complete adapter image; outer publication tested separately"
                )
        assert database_snapshot(connection) == snapshot
        assert j33._cache(ports) == cache


def _insert_row(cursor: Any, table: str, values: dict[str, Any]) -> None:
    """Controlled test-envelope INSERT; identities use native typed recipes."""
    cursor.execute(
        sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(",").join(map(sql.Identifier, values)),
            sql.SQL(",").join(sql.Placeholder() for _ in values),
        ),
        tuple(values.values()),
    )


def _publish_document_m5_state(cursor: Any, epoch: int, sealed: int) -> dict[str, int]:
    """Controlled SQL publication, NOT a production publisher or work oracle.

    Only genuine working keys and final requirement-currency holders publish.
    J33 already publishes M4 claim currency/structure/grounding/evaluation.
    """
    cursor.execute(
        "UPDATE groundloop_published_observation_currency p SET valid_to_epoch=%s "
        "FROM groundloop_m5_working_currency_history w WHERE w.epoch_id=%s "
        "AND w.valid_to_revision IS NULL "
        "AND w.subject_kind='requirement' AND p.subject_kind=w.subject_kind "
        "AND p.subject_id=w.subject_id AND p.chunk_version_id=w.chunk_version_id "
        "AND p.task_type=w.task_type AND p.valid_to_epoch IS NULL",
        (epoch, epoch),
    )
    cursor.execute(
        "DELETE FROM groundloop_observation_currency p "
        "USING groundloop_m5_working_currency_history w WHERE w.epoch_id=%s "
        "AND w.valid_to_revision IS NULL "
        "AND w.subject_kind='requirement' "
        "AND p.subject_kind=w.subject_kind AND p.subject_id=w.subject_id "
        "AND p.chunk_version_id=w.chunk_version_id "
        "AND p.task_type=w.task_type",
        (epoch,),
    )
    cursor.execute(
        "INSERT INTO groundloop_observation_currency ("
        "subject_kind,subject_id,chunk_version_id,task_type,observation_id,"
        "installed_revision) SELECT subject_kind,subject_id,chunk_version_id,"
        "task_type,observation_id,%s FROM groundloop_m5_working_currency_history "
        "WHERE epoch_id=%s AND valid_to_revision IS NULL "
        "AND subject_kind='requirement' AND observation_id IS NOT NULL",
        (sealed, epoch),
    )
    cursor.execute(
        "INSERT INTO groundloop_published_observation_currency ("
        "subject_kind,subject_id,chunk_version_id,task_type,observation_id,"
        "valid_from_epoch,valid_to_epoch) SELECT subject_kind,subject_id,"
        "chunk_version_id,task_type,observation_id,%s,NULL "
        "FROM groundloop_m5_working_currency_history WHERE epoch_id=%s "
        "AND valid_to_revision IS NULL AND subject_kind='requirement' "
        "AND observation_id IS NOT NULL",
        (epoch, epoch),
    )
    counts = {}
    for family, key in (
        ("requirement", "requirement_version_id"),
        ("group", "group_version_id"),
        ("claim", "claim_id"),
        ("answer", "answer_version_id"),
    ):
        working = sql.Identifier("groundloop_m5_working_" + family + "_state")
        published = sql.Identifier("groundloop_m5_published_" + family + "_state")
        materialized = sql.Identifier("groundloop_m5_" + family + "_state_materialized")
        identity = sql.Identifier(key)
        columns = [
            row[0]
            for row in cursor.execute(
                "SELECT attname FROM pg_catalog.pg_attribute "
                "WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped "
                "AND attname NOT IN ('epoch_id','updated_revision') ORDER BY attnum",
                ("groundloop_m5_working_" + family + "_state",),
            ).fetchall()
        ]
        col = sql.SQL(",").join(map(sql.Identifier, columns))
        count = cursor.execute(
            sql.SQL(
                "UPDATE {p} p SET valid_to_epoch=%s FROM {w} w "
                "WHERE w.epoch_id=%s AND p.{k}=w.{k} AND p.valid_to_epoch IS NULL"
            ).format(p=published, w=working, k=identity),
            (epoch, epoch),
        ).rowcount
        count += cursor.execute(
            sql.SQL(
                "DELETE FROM {m} m USING {w} w WHERE w.epoch_id=%s AND m.{k}=w.{k}"
            ).format(m=materialized, w=working, k=identity),
            (epoch,),
        ).rowcount
        count += cursor.execute(
            sql.SQL(
                "INSERT INTO {p} ({c},valid_from_epoch,valid_to_epoch,sealed_revision) "
                "SELECT {c},%s,NULL,%s FROM {w} WHERE epoch_id=%s"
            ).format(p=published, c=col, w=working),
            (epoch, sealed, epoch),
        ).rowcount
        count += cursor.execute(
            sql.SQL(
                "INSERT INTO {m} ({c},updated_epoch,updated_revision) "
                "SELECT {c},%s,%s FROM {w} WHERE epoch_id=%s"
            ).format(m=materialized, c=col, w=working),
            (epoch, sealed, epoch),
        ).rowcount
        counts[family] = count
    for family, key in (("group", "group_version_id"), ("claim", "claim_id")):
        binding = sql.Identifier(
            "groundloop_m5_published_" + family + "_certificate_binding"
        )
        state = sql.Identifier("groundloop_m5_working_" + family + "_state")
        working = sql.Identifier(
            "groundloop_m5_working_" + family + "_certificate_binding"
        )
        cursor.execute(
            sql.SQL(
                "UPDATE {b} b SET valid_to_epoch=%s FROM {s} s WHERE s.epoch_id=%s "
                "AND b.{k}=s.{k} AND b.valid_to_epoch IS NULL"
            ).format(b=binding, s=state, k=sql.Identifier(key)),
            (epoch, epoch),
        )
        cursor.execute(
            sql.SQL(
                "INSERT INTO {b} ({k},valid_from_epoch,valid_to_epoch,"
                "sealed_revision,certificate_digest) "
                "SELECT {k},%s,NULL,%s,certificate_digest FROM {w} "
                "WHERE epoch_id=%s AND valid_to_revision IS NULL"
            ).format(b=binding, w=working, k=sql.Identifier(key)),
            (epoch, sealed, epoch),
        )
    return counts


def _controlled_outer_document_seal(
    db: o33.OpenDatabase,
    ports: Any,
    plan: Any,
    epoch: int,
    revision: int,
    control: dict[str, Any],
    cut: Callable[[str], None] = lambda _: None,
) -> Any:
    """Full guarded TEST envelope; deliberately not C1 or full work measurement.

    Real committed R33 -> real cursor-local J33 -> real repaired promotion.
    M5 state/result/work/timing publication below is controlled SQL, bounded like
    the accepted group-seal diagnostic. Only its state rowcounts and seal S/K
    bytes are charged; J33/M4/reader production regions are NOT fully measured.
    No source staging, cache adoption or public coordinator follows. Genuine
    C2 observations, when present, were completed before this envelope.
    """
    connection, sealed = ports.connection, revision + 1
    before = _image(connection)
    with connection.transaction(), _recording_cursor(control)(connection) as cursor:
        for table in (
            "groundloop_runtime_mode",
            "groundloop_m4_publication_head",
            "groundloop_m5_publication_head",
        ):
            cursor.execute(
                sql.SQL("SELECT * FROM {} WHERE singleton FOR UPDATE").format(
                    sql.Identifier(table)
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
        cut("after_base_runtime_lock")
        if base == (sealed, "sealed") and runtime == (sealed, "sealed"):
            retained = _load_terminal_result(
                cursor,
                structural_event_id=plan.structural_event_id,
                payload_hash=plan.payload_hash,
            )
            if retained is None or retained.epoch_id != epoch:
                raise InvalidEventError("B33 controlled replay lacks retained result")
            return retained
        if base != (revision, "complete") or runtime != (revision, "semantic_complete"):
            raise InvalidEventError("B33 test envelope requires genuine readiness")
        for table, key in (
            ("groundloop_m5_owner_pending_counter", "owner_claim_id"),
            ("groundloop_m5_answer_pending_counter", "answer_version_id"),
        ):
            cursor.execute(
                sql.SQL(
                    'SELECT * FROM {} WHERE epoch_id=%s ORDER BY {} COLLATE "C" '
                    "FOR UPDATE"
                ).format(sql.Identifier(table), sql.Identifier(key)),
                (epoch,),
            ).fetchall()
        prior, timing = _lock_accounting(cursor, db.schema, epoch, revision)
        assert timing.has_pending_anchor
        pending = (
            timing.pending_contribution_kind,
            timing.pending_source_id,
            timing.pending_contribution_key_digest,
            timing.pending_anchor_revision,
        )
        assert (pending[0], pending[1], pending[3]) == (
            "semantic_readiness",
            "semantic_complete",
            revision,
        )
        cut("after_accounting_locks")
        publication_receipt = j33.seal._stage_job_free_document_seal_local(
            ports, cursor, epoch, revision, plan.direct_plan.update
        )
        cut("after_j33")
        promotion = promote_matching_overlay(
            cursor, epoch_id=epoch, expected_revision=revision, sealed_revision=sealed
        )
        _assert_receipt_and_owner_journal(
            cursor,
            before,
            _image(connection),
            promotion,
            epoch=epoch,
            revision=sealed,
            inspect_journal=not control.get("nonowner", False),
        )
        _assert_driver_rowcounts(control, promotion)
        # Matching promotion is publication, not a second D25 logical charge.
        assert _lock_accounting(cursor, db.schema, epoch, revision)[0] == prior
        cut("after_matching")
        counts = _publish_document_m5_state(cursor, epoch, sealed)
        for table in (
            "groundloop_m4_publication_head",
            "groundloop_m5_publication_head",
        ):
            suffix = (
                ",sealed_revision=%s" if table.endswith("m5_publication_head") else ""
            )
            cursor.execute(
                sql.SQL(
                    "UPDATE {} SET epoch_id=%s" + suffix + " WHERE singleton"
                ).format(sql.Identifier(table)),
                (epoch, sealed) if suffix else (epoch,),
            )
        assert (
            cursor.execute(
                "UPDATE groundloop_epoch SET revision=%s,semantic_status='sealed',"
                "evaluation_state='complete',publication_mode='strict',"
                "sealed_at=clock_timestamp() WHERE epoch_id=%s AND revision=%s",
                (sealed, epoch, revision),
            ).rowcount
            == 1
        )
        cut("after_half_terminal")
        pending_sql = (
            "SELECT pending_contribution_kind,pending_source_id,"
            "pending_contribution_key_digest,pending_anchor_revision "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s"
        )
        assert cursor.execute(pending_sql, (epoch,)).fetchone() == pending
        children = _prepare_preterminal_matching_publication_children(
            cursor, epoch_id=epoch, expected_revision=revision, sealed_revision=sealed
        )
        assert cursor.execute(pending_sql, (epoch,)).fetchone() == pending
        cut("after_preparation")
        publication = stable_m4_digest("m4-publication-v1", str(epoch))
        assert publication_receipt.publication_id == publication
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
        key_bytes = digests.stable_m5_preimage(
            "m5-runtime-work-contribution-key-v1",
            int_field(epoch),
            enum_field("seal"),
            text_field(plan.structural_event_id),
        )
        work = M5RuntimeWork(
            group_state_write_count=counts["group"],
            claim_state_write_count=counts["claim"],
            answer_state_write_count=counts["answer"],
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
        _insert_row(
            cursor,
            "groundloop_m5_runtime_work_contribution",
            dict(
                epoch_id=epoch,
                contribution_kind="seal",
                source_id=plan.structural_event_id,
                source_identity_hash=hashlib.sha256(source_bytes).hexdigest(),
                contribution_key_digest=hashlib.sha256(key_bytes).hexdigest(),
                applied_revision=sealed,
                **dict(zip(work.counter_names(), work.counter_values(), strict=True)),
                work_digest=work.work_digest,
            ),
        )
        cut("after_contribution")
        _resolve_prior_pending(cursor, db.schema, epoch, timing)
        cut("after_prior_missing")
        assert (
            cursor.execute(
                "UPDATE groundloop_m5_runtime_epoch "
                "SET revision=%s,runtime_state='sealed',"
                "terminal_at=clock_timestamp() WHERE epoch_id=%s AND revision=%s",
                (sealed, epoch, revision),
            ).rowcount
            == 1
        )
        for table in (
            "groundloop_m5_owner_pending_counter",
            "groundloop_m5_answer_pending_counter",
        ):
            cursor.execute(
                sql.SQL("UPDATE {} SET updated_revision=%s WHERE epoch_id=%s").format(
                    sql.Identifier(table)
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
                    "UPDATE groundloop_m5_runtime_work_accumulator "
                    "SET {},work_digest=%s,"
                    "updated_revision=%s,terminalized=true WHERE epoch_id=%s "
                    "AND updated_revision=%s AND work_digest=%s AND NOT terminalized"
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
            for suffix, amount in (("expected_count", 1), ("missing_count", 2)):
                column = sql.Identifier(prefix + "_" + suffix)
                increments.append(
                    sql.SQL("{}={}+{}").format(column, column, sql.Literal(amount))
                )
        assert (
            cursor.execute(
                sql.SQL(
                    "UPDATE groundloop_m5_runtime_timing_accumulator SET {},"
                    "updated_revision=%s,terminalized=true,pending_contribution_kind=NULL,"
                    "pending_source_id=NULL,pending_contribution_key_digest=NULL,"
                    "pending_anchor_revision=NULL "
                    "WHERE epoch_id=%s AND updated_revision=%s "
                    "AND NOT terminalized"
                ).format(sql.SQL(",").join(increments)),
                (sealed, epoch, revision),
            ).rowcount
            == 1
        )
        cut("after_timing_cas")
        timing_row = cursor.execute(
            "SELECT "
            + ",".join((*_TIMING_SUM_COLUMNS, *_TIMING_COVERAGE_COLUMNS))
            + ",updated_revision,terminalized,pending_contribution_kind,"
            "pending_source_id,pending_contribution_key_digest,pending_anchor_revision "
            "FROM groundloop_m5_runtime_timing_accumulator WHERE epoch_id=%s",
            (epoch,),
        ).fetchone()
        event_timing, coverage = _timing_accumulator_from_row(timing_row).project(
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
            publication_receipt=publication_receipt,
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
        _insert_row(
            cursor,
            "groundloop_m5_event_result",
            dict(
                structural_event_id=plan.structural_event_id,
                payload_hash=plan.payload_hash,
                epoch_id=epoch,
                outcome="sealed",
                original_open_receipt_binding_hash=digests.open_event_receipt_binding_digest(
                    epoch_id=epoch,
                    replayed=False,
                    already_sealed=False,
                    publication_id=None,
                    already_failed=False,
                    failure_reason=None,
                ),
                publication_id=publication,
                original_publication_receipt_binding_hash=digests.publication_receipt_binding_digest(
                    epoch_id=epoch, publication_id=publication, replayed=False
                ),
                event_work_kind="event",
                event_work_digest=total.work_digest,
                combined_status_delta_set_hash=combined,
                changed_state_set_hash=changed,
                failure_reason=None,
                logical_result_hash=result.logical_result_hash,
                delta_count=len(children.combined_deltas),
                state_reference_count=len(children.changed_state_references),
                **{n: getattr(event_timing, n) for n in _TIMING_SUM_COLUMNS},
            ),
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
        _insert_row(
            cursor,
            "groundloop_m5_event_timing_coverage",
            dict(
                structural_event_id=plan.structural_event_id,
                epoch_id=epoch,
                **{n: getattr(coverage, n) for n in _TIMING_COVERAGE_COLUMNS},
                terminal_client_roundtrip_included=False,
            ),
        )
        cut("after_result_children")
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        cut("after_constraints")
        return result


@pytest.mark.parametrize("open_db", (False,), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_document_controlled_seal_commits_reconnects_and_r33_replays(
    open_db: o33.OpenDatabase,
    nonowner: bool,
) -> None:
    db = open_db
    _incomplete_foundation(db)
    plan, epoch, revision = j33._ready(db)
    anchor = r33._edge(
        db.connection,
        epoch,
        plan.structural_event_id,
        "semantic_complete",
        revision - 1,
    ).anchor
    with j33._principal(db, nonowner) as ports:
        control: dict[str, Any] = {"nonowner": nonowner}
        result = _controlled_outer_document_seal(
            db, ports, plan, epoch, revision, control
        )
        assert len(control["mutations"]) == 13
        # The final server command checks all deferred constraints, then commits.
        assert control["all_sql"][-1] == "SET CONSTRAINTS ALL IMMEDIATE"
        _assert_native_readiness_replay(ports.connection, plan, epoch, revision, anchor)
    with psycopg.connect(db.dsn, autocommit=True) as reconnected:
        reconnected.execute(
            sql.SQL("SET search_path TO {},public").format(sql.Identifier(db.schema))
        )
        assert r33._point(reconnected, epoch)[:5] == (
            revision + 1,
            "sealed",
            "complete",
            "sealed",
            revision + 1,
        )
        _assert_native_readiness_replay(reconnected, plan, epoch, revision, anchor)
        _assert_committed_document(db, reconnected, epoch)
        columns = result.event_work.counter_names()
        totals = reconnected.execute(
            sql.SQL(
                "SELECT {} FROM groundloop_m5_runtime_work_contribution "
                "WHERE epoch_id=%s"
            ).format(
                sql.SQL(",").join(
                    sql.SQL("sum({})").format(sql.Identifier(n)) for n in columns
                )
            ),
            (epoch,),
        ).fetchone()
        assert totals == result.event_work.counter_values()
    # A genuinely new LOGIN connection replays the sealed readiness anchor too.
    if nonowner:
        with r33._runtime_login(
            SimpleNamespace(connection=db.connection, dsn=db.dsn, schema_name=db.schema)
        ) as login:
            assert (
                login.execute("SELECT session_user,current_user").fetchone()[0]
                != db.connection.execute("SELECT current_user").fetchone()[0]
            )
            # Identity SELECT must not own the native replay transaction.
            login.commit()
            _assert_native_readiness_replay(login, plan, epoch, revision, anchor)
    _assert_complete_image(db, epoch)
    o33._assert_complete_point(db, epoch)


def _assert_native_readiness_replay(
    connection: Any,
    plan: Any,
    epoch: int,
    revision: int,
    anchor: Any = None,
) -> None:
    before = database_snapshot(connection)
    records: dict[str, Any] = {"records": [], "target": None}
    factory = connection.cursor_factory
    connection.cursor_factory = r33._r33_mutation_cursor(records)
    try:
        replay = r33._edge(
            connection,
            epoch,
            plan.structural_event_id,
            "semantic_complete",
            revision - 1,
        )
    finally:
        connection.cursor_factory = factory
    assert replay.replay and replay.anchor.anchor_revision == revision
    if anchor is not None:
        assert replay.anchor == anchor
    assert records["records"] == []
    assert database_snapshot(connection) == before


def _assert_committed_document(
    db: o33.OpenDatabase, connection: Any, epoch: int
) -> None:
    """Independent committed global currency + complete materialized/history image."""
    expected_currency = {
        tuple(row[:4]): row[4]
        for row in connection.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_published_observation_currency WHERE valid_from_epoch<=%s "
            "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
            (db.base.epoch_id, db.base.epoch_id),
        ).fetchall()
    }
    for row in connection.execute(
        "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
        "FROM groundloop_m5_working_currency_history WHERE epoch_id=%s "
        "AND valid_to_revision IS NULL",
        (epoch,),
    ).fetchall():
        expected_currency[tuple(row[:4])] = row[4]
    expected_currency = {k: v for k, v in expected_currency.items() if v is not None}
    for query in (
        "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
        "FROM groundloop_observation_currency",
        "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
        "FROM groundloop_published_observation_currency WHERE valid_to_epoch IS NULL",
    ):
        assert {
            tuple(row[:4]): row[4] for row in connection.execute(query)
        } == expected_currency
    assert connection.execute(
        "SELECT valid_to_epoch FROM groundloop_published_observation_currency "
        "WHERE chunk_version_id=ANY(%s)",
        (list(db.base.chunk_ids),),
    ).fetchall() == [(epoch,), (epoch,)]
    states = compute_reference_states(o33._raw_python_point(db, epoch))
    for family, key, expected in (
        ("requirement", "requirement_version_id", states.requirements),
        ("group", "group_version_id", states.groups),
        ("claim", "claim_id", states.claims),
        ("answer", "answer_version_id", states.answers),
    ):
        for table, predicate, arguments in (
            ("groundloop_m5_" + family + "_state_materialized", "", ()),
            (
                "groundloop_m5_published_" + family + "_state",
                " WHERE valid_from_epoch<=%s AND "
                "(valid_to_epoch IS NULL OR %s<valid_to_epoch)",
                (epoch, epoch),
            ),
        ):
            rows = connection.execute(
                sql.SQL("SELECT to_jsonb(t) FROM {} t" + predicate).format(
                    sql.Identifier(table)
                ),
                arguments,
            ).fetchall()
            actual = {
                row[key]: {n: row[n] for n in asdict(expected[row[key]])}
                for (row,) in rows
            }
            assert actual == {k: o33._wire(asdict(v)) for k, v in expected.items()}
    assert not any(asdict(read_m5_mismatch_counts(connection)).values())
    assert connection.execute(
        "SELECT lifecycle_state,default_evaluation_state,confirmed_as_of_epoch,"
        "open_discovery_scope_count,revision "
        "FROM groundloop_m4_evaluation_epoch_counter WHERE epoch_id=%s",
        (epoch,),
    ).fetchone() == ("sealed", "complete", epoch, 0, 2)
    assert connection.execute(
        "SELECT transition_id,transition_kind,from_revision,to_revision,"
        "override_rows_written FROM groundloop_m4_evaluation_counter_transition "
        "WHERE epoch_id=%s",
        (epoch,),
    ).fetchall() == [
        (stable_m4_digest("m4-evaluation-seal-v1", str(epoch)), "seal", 1, 2, 0)
    ]


@pytest.mark.parametrize("open_db", (False,), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_document_each_actual_promotion_and_outer_cut_rolls_back_then_retries(
    open_db: o33.OpenDatabase,
    nonowner: bool,
) -> None:
    db = open_db
    _incomplete_foundation(db)
    plan, epoch, revision = j33._ready(db)
    with j33._principal(db, nonowner) as ports:
        connection = ports.connection
        baseline, cache = database_snapshot(connection), j33._cache(ports)
        for number in range(1, 14):
            control: dict[str, Any] = {"cut": number, "nonowner": nonowner}
            with pytest.raises(_B33Rollback, match="actual matching mutation cut"):
                _controlled_outer_document_seal(
                    db, ports, plan, epoch, revision, control
                )
            assert len(control["mutations"]) == number
            assert database_snapshot(connection) == baseline
            assert j33._cache(ports) == cache
        for point in (
            "after_accounting_locks",
            "after_j33",
            "after_matching",
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

            def fail(current: str, expected: str = point) -> None:
                if current == expected:
                    raise _B33Rollback("actual outer cut " + current)

            with pytest.raises(_B33Rollback, match="actual outer cut " + point):
                _controlled_outer_document_seal(
                    db, ports, plan, epoch, revision, {"nonowner": nonowner}, fail
                )
            assert database_snapshot(connection) == baseline
            assert j33._cache(ports) == cache
        _controlled_outer_document_seal(
            db, ports, plan, epoch, revision, {"nonowner": nonowner}
        )
    _assert_complete_image(db, epoch)


@pytest.mark.parametrize("open_db", (False,), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_document_wrong_stale_duplicate_and_unauthorized_dml_are_zero_write(
    open_db: o33.OpenDatabase,
    nonowner: bool,
) -> None:
    db = open_db
    _incomplete_foundation(db)
    plan, epoch, revision = j33._ready(db)
    with j33._principal(db, nonowner) as ports:
        connection = ports.connection
        baseline, cache = database_snapshot(connection), j33._cache(ports)
        for bad_epoch, bad_revision in (
            (epoch + 1000, revision),
            (epoch, revision - 1),
        ):
            with pytest.raises((psycopg.Error, ValidationError)):
                with connection.transaction(), connection.cursor() as cursor:
                    promote_matching_overlay(
                        cursor,
                        epoch_id=bad_epoch,
                        expected_revision=bad_revision,
                        sealed_revision=bad_revision + 1,
                    )
            assert database_snapshot(connection) == baseline
        with pytest.raises(psycopg.Error):
            with connection.transaction(), connection.cursor() as cursor:
                j33.seal._stage_job_free_document_seal_local(
                    ports, cursor, epoch, revision, plan.direct_plan.update
                )
                for _ in range(2):
                    promote_matching_overlay(
                        cursor,
                        epoch_id=epoch,
                        expected_revision=revision,
                        sealed_revision=revision + 1,
                    )
        assert database_snapshot(connection) == baseline
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute(
                    "UPDATE groundloop_m5_matching_hall_current "
                    "SET installed_revision=installed_revision"
                )
        assert database_snapshot(connection) == baseline
        assert j33._cache(ports) == cache


@pytest.mark.parametrize("open_db", (False,), indirect=True)
@pytest.mark.parametrize("leader_index", (0, 1))
def test_genuine_document_both_observed_serializer_orders_and_zero_write_loser(
    open_db: o33.OpenDatabase,
    leader_index: int,
) -> None:
    db = open_db
    _incomplete_foundation(db)
    plan, epoch, revision = j33._ready(db)
    anchor = r33._edge(
        db.connection,
        epoch,
        plan.structural_event_id,
        "semantic_complete",
        revision - 1,
    ).anchor
    with (
        j33._principal(db, True) as login_ports,
        psycopg.connect(db.dsn, autocommit=True) as monitor,
    ):
        monitor.execute(
            sql.SQL("SET search_path TO {},public").format(sql.Identifier(db.schema))
        )
        ports = (db.ports, login_ports)
        leader, follower = ports[leader_index], ports[1 - leader_index]
        for port in ports:
            port.connection.execute("SET statement_timeout='25s'")
        held, release, acquired, finish = (threading.Event() for _ in range(4))
        controls: list[dict[str, Any]] = [{"nonowner": False}, {"nonowner": True}]

        def invoke(index: int) -> Any:
            def pause(point: str) -> None:
                if index == leader_index and point == "after_accounting_locks":
                    held.set()
                    assert release.wait(20), "B33 winner was not released"
                if index != leader_index and point == "after_base_runtime_lock":
                    acquired.set()
                    assert finish.wait(20), "B33 locked loser was not released"

            return _controlled_outer_document_seal(
                db, ports[index], plan, epoch, revision, controls[index], pause
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            winner_future = pool.submit(invoke, leader_index)
            try:
                assert held.wait(15)
                loser_future = pool.submit(invoke, 1 - leader_index)
                deadline = time.monotonic() + 10
                while True:
                    row = monitor.execute(
                        "SELECT pg_blocking_pids(pid),wait_event_type,query "
                        "FROM pg_stat_activity WHERE pid=%s",
                        (follower.connection.info.backend_pid,),
                    ).fetchone()
                    ungranted = monitor.execute(
                        "SELECT locktype FROM pg_locks WHERE pid=%s AND NOT granted",
                        (follower.connection.info.backend_pid,),
                    ).fetchall()
                    if (
                        row
                        and leader.connection.info.backend_pid in row[0]
                        and row[1] == "Lock"
                        and ungranted
                    ):
                        assert "groundloop_runtime_mode" in row[2]
                        assert "FOR UPDATE" in row[2]
                        break
                    assert time.monotonic() < deadline, "B33 blocking not observed"
                    time.sleep(0.01)
                release.set()
                winner = winner_future.result(timeout=15)
                assert acquired.wait(10)
                committed = database_snapshot(monitor)
                finish.set()
                loser = loser_future.result(timeout=15)
                assert loser.state is M5RunState.REPLAYED
                assert loser.logical_result_hash == winner.logical_result_hash
                assert loser.event_work == winner.event_work
                assert not any(loser.call_work.counter_values())
                assert controls[1 - leader_index].get("mutations", []) == []
                assert not any(
                    q.startswith(("INSERT ", "UPDATE ", "DELETE "))
                    for q in controls[1 - leader_index]["all_sql"]
                )
                assert database_snapshot(monitor) == committed
                _assert_native_readiness_replay(
                    follower.connection, plan, epoch, revision, anchor
                )
                assert database_snapshot(monitor) == committed
            finally:
                release.set()
                finish.set()
    _assert_complete_image(db, epoch)


@pytest.mark.parametrize("action", ("REPLACE", "RETIRE"))
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_group_new_hall_and_retired_keys_exact_journals_and_receipt(
    readiness_db: Any,
    action: str,
    nonowner: bool,
) -> None:
    db = readiness_db
    expected_currency = {
        tuple(row[:4]): row[4]
        for row in db.connection.execute(
            "SELECT subject_kind,subject_id,chunk_version_id,task_type,observation_id "
            "FROM groundloop_observation_currency"
        ).fetchall()
    }
    # These raw foundation facts predate OPEN. Never construct expected state
    # from the working patches, promotion receipt or postseal target tables.
    assert expected_currency == {}
    adapter = SimpleNamespace(
        connection=db.connection, schema=db.schema_name, group=db.group
    )
    expected_repo = c2_history._raw_python_point(
        adapter, db.base.epoch_id, 0, expected_currency
    )
    db.connection.commit()
    opened = group_seal._open_pending_matching_group(db, db.connection, action)
    with group_seal._seal_runtime_session(
        db.connection, db.schema_name, nonowner=nonowner
    ) as connection:
        plan, epoch, anchor = group_seal._genuine_pending_ready(
            db, connection, action, opened=opened
        )
        while expected_repo.current_epoch < epoch:
            expected_repo.begin_event_epoch()
        if action == "REPLACE":
            expected_repo.replace_group(
                plan.event.old_group_version_id, plan.event.successor, epoch
            )
        else:
            expected_repo.retire_group(
                plan.event.group_version_id, epoch, plan.event.event_id
            )
        before = _image(connection)
        connection.commit()
        observed = []
        trace: dict[str, Any] = {}

        def inspect(cursor: Any, receipt: Any) -> None:
            after = _image(connection)
            _assert_receipt_and_owner_journal(
                cursor,
                before,
                after,
                receipt,
                epoch=epoch,
                revision=anchor.anchor_revision + 1,
                inspect_journal=not nonowner,
            )
            _assert_driver_rowcounts(trace, receipt)
            assert set(before["hall"]) - set(after["hall"])
            assert bool(set(after["hall"]) - set(before["hall"])) is (
                action == "REPLACE"
            )
            observed.append(receipt)

        original_factory = connection.cursor_factory
        connection.cursor_factory = _recording_cursor(trace)
        try:
            group_seal._sql_pending_seal_boundary(
                db,
                connection,
                plan,
                epoch,
                anchor.anchor_revision,
                promotion_observer=inspect,
            )
        finally:
            connection.cursor_factory = original_factory
        assert len(observed) == 1
        connection.commit()
    with db.reconnect() as reader:
        reader.autocommit = True
        _assert_published_raw_images(
            reader, expected_repo, epoch, expected_currency, anchor.anchor_revision + 1
        )
        sql_adapter = SimpleNamespace(
            connection=reader, schema=db.schema_name, group=db.group
        )
        _assert_independent_sql_states(
            sql_adapter, expected_repo, epoch, expected_currency
        )
        validity = reader.execute(
            "SELECT group_version_id,valid_to_epoch FROM groundloop_m5_group_validity "
            "ORDER BY group_version_id"
        ).fetchall()
        assert dict(validity) == {
            db.group.group_version_id: epoch,
            **(
                {plan.event.successor.group_version_id: None}
                if action == "REPLACE"
                else {}
            ),
        }
        _assert_retained_tombstone_absence(reader, epoch, required_family="hall")
        _assert_native_readiness_replay(
            reader, plan, epoch, anchor.anchor_revision, anchor
        )


def _assert_published_raw_images(
    connection: Any,
    repo: Any,
    epoch: int,
    expected_currency: dict[tuple[str, ...], str | None],
    sealed_revision: int,
) -> None:
    """Complete COMMITTED images against raw-before/event/validated-output model."""
    live_currency = {
        key: value for key, value in expected_currency.items() if value is not None
    }
    for suffix, parameters in (
        ("groundloop_observation_currency", ()),
        (
            "groundloop_published_observation_currency WHERE valid_from_epoch<=%s "
            "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
            (epoch, epoch),
        ),
    ):
        assert {
            tuple(row[:4]): row[4]
            for row in connection.execute(
                "SELECT subject_kind,subject_id,chunk_version_id,task_type,"
                "observation_id FROM " + suffix,
                parameters,
            ).fetchall()
        } == live_currency
    _assert_matching_payloads(connection, _matching_from_raw_repository(repo))
    assert connection.execute(
        "SELECT singleton,decision_policy_version,installed_epoch_id,"
        "installed_revision "
        "FROM groundloop_m5_matching_image_current"
    ).fetchall() == [
        (True, repo.base.current_policy().policy_version, epoch, sealed_revision)
    ]
    states = compute_reference_states(repo)
    for family, key, expected in (
        ("requirement", "requirement_version_id", states.requirements),
        ("group", "group_version_id", states.groups),
        ("claim", "claim_id", states.claims),
        ("answer", "answer_version_id", states.answers),
    ):
        for table, predicate, arguments in (
            (f"groundloop_m5_{family}_state_materialized", "", ()),
            (
                f"groundloop_m5_published_{family}_state",
                " WHERE valid_from_epoch<=%s "
                "AND (valid_to_epoch IS NULL OR %s<valid_to_epoch)",
                (epoch, epoch),
            ),
        ):
            rows = connection.execute(
                sql.SQL("SELECT to_jsonb(t) FROM {} t" + predicate).format(
                    sql.Identifier(table)
                ),
                arguments,
            ).fetchall()
            actual = {row[key]: row for (row,) in rows}
            assert set(actual) == set(expected)
            assert {
                identity: {field: actual[identity][field] for field in asdict(value)}
                for identity, value in expected.items()
            } == {
                identity: o33._wire(asdict(value))
                for identity, value in expected.items()
            }
    assert not any(asdict(read_m5_mismatch_counts(connection)).values())


def _assert_retained_tombstone_absence(
    connection: Any, epoch: int, *, required_family: str
) -> None:
    """POSTseal retained absence, not a fresh DELETE of an already-absent key.

    A second matching publication is unauthorized. Native readiness replay is
    tested separately and must leave these retained tombstones/current absences
    unchanged, with zero DML. Never manufacture a fresh missing-current key.
    """
    counts = {}
    for family, _, keys, _ in FAMILIES:
        positive = {"edge": "refcount", "hash_mask": "mask"}.get(family, "present")
        columns = sql.SQL(",").join(map(sql.Identifier, keys))
        tombstones = connection.execute(
            sql.SQL("SELECT {c} FROM {t} WHERE epoch_id=%s AND NOT ({p}>0)").format(
                c=columns,
                t=sql.Identifier("groundloop_m5_matching_" + family + "_working"),
                p=(
                    sql.SQL("{}::integer").format(sql.Identifier(positive))
                    if positive == "present"
                    else sql.Identifier(positive)
                ),
            ),
            (epoch,),
        ).fetchall()
        current = connection.execute(
            sql.SQL("SELECT {c} FROM {t}").format(
                c=columns,
                t=sql.Identifier("groundloop_m5_matching_" + family + "_current"),
            )
        ).fetchall()
        assert not set(tombstones) & set(current)
        counts[family] = len(tombstones)
    assert counts[required_family] > 0


def _assert_independent_sql_states(
    db: Any, repo: Any, epoch: int, currency: dict[tuple[str, ...], str | None]
) -> None:
    states = compute_reference_states(repo)
    sql_rows = c2_history._independent_sql_point(db, epoch, currency)
    for family, key in (
        ("requirements", "requirement_version_id"),
        ("groups", "group_version_id"),
        ("claims", "claim_id"),
        ("answers", "answer_version_id"),
    ):
        assert {row[key]: row for row in sql_rows[family]} == {
            identity: o33._wire(asdict(value))
            for identity, value in getattr(states, family).items()
        }
    raw = _matching_from_raw_repository(repo)
    assert {
        (row["requirement_version_id"], row["text_hash"]): {
            key: row[key]
            for key in (
                "requirement_version_id",
                "group_version_id",
                "requirement_ordinal",
                "text_hash",
            )
        }
        | {"refcount": len(row["active_observation_ids"])}
        for row in sql_rows["edges"]
    } == raw["edge"]
    assert {
        (observation,): {
            key: row[key]
            for key in (
                "requirement_version_id",
                "group_version_id",
                "requirement_ordinal",
                "text_hash",
            )
        }
        | {"observation_id": observation}
        for row in sql_rows["edges"]
        for observation in row["active_observation_ids"]
    } == raw["observation"]
    sql_masks: dict[tuple[str, str], int] = {}
    for row in sql_rows["edges"]:
        key = row["group_version_id"], row["text_hash"]
        sql_masks[key] = sql_masks.get(key, 0) | 1 << row["requirement_ordinal"]
    assert {
        key: {"group_version_id": key[0], "text_hash": key[1], "mask": mask}
        for key, mask in sql_masks.items()
    } == raw["hash_mask"]
    assert {
        (row["group_version_id"], row["subset_mask"]): row for row in sql_rows["hall"]
    } == {
        (group_id, subset): {
            "group_version_id": group_id,
            "requirement_count": hall["requirement_count"],
            "subset_mask": subset,
            "subset_size": subset.bit_count(),
            "neighbor_count": hall["neighbor_counts"][subset],
            "deficiency": hall["deficiencies"][subset],
        }
        for (group_id,), hall in raw["hall"].items()
        for subset in range(1, 1 << hall["requirement_count"])
    }


@contextmanager
def _c2_seal_principal(db: Any, nonowner: bool) -> Any:
    def ports(connection: Any) -> Any:
        result = PostgresM4ApplicationPorts(
            connection, structural_payloads={}, execution_mode=M4ExecutionMode.MEASURED
        )
        # Accepted reconnect/hydration path, never manual cache-field assignment.
        result._hydrate_direct_working_cache(db.runtime.epoch_id)
        return result

    if not nonowner:
        yield ports(db.connection)
    else:
        address = SimpleNamespace(
            connection=db.connection, dsn=db.dsn, schema_name=db.schema
        )
        with r33._runtime_login(address) as connection:
            connection.autocommit = True
            yield ports(connection)


def _assert_c2_promotion_and_outer_cut_retries(
    db: Any, ports: Any, plan: Any, epoch: int, revision: int, nonowner: bool
) -> None:
    """Nonzero C2 INSERT/carry mutations must roll back, not only old zero paths."""
    connection = ports.connection
    baseline, cache = database_snapshot(connection), j33._cache(ports)
    for number in range(1, 14):
        control: dict[str, Any] = {"cut": number, "nonowner": nonowner}
        with pytest.raises(_B33Rollback, match="actual matching mutation cut"):
            _controlled_outer_document_seal(db, ports, plan, epoch, revision, control)
        assert len(control["mutations"]) == number
        assert database_snapshot(connection) == baseline
        assert j33._cache(ports) == cache
    for point in (
        "after_accounting_locks",
        "after_j33",
        "after_matching",
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

        def fail(current: str, expected: str = point) -> None:
            if current == expected:
                raise _B33Rollback("actual C2 outer cut " + current)

        with pytest.raises(_B33Rollback, match="actual C2 outer cut " + point):
            _controlled_outer_document_seal(
                db, ports, plan, epoch, revision, {"nonowner": nonowner}, fail
            )
        assert database_snapshot(connection) == baseline
        assert j33._cache(ports) == cache


@pytest.mark.parametrize("c2_db", (None, "representative"), indirect=True)
@pytest.mark.parametrize("nonowner", (False, True))
def test_genuine_c2_new_positive_and_equal_value_carry_commit_full_images(
    c2_db: Any, nonowner: bool
) -> None:
    """Real unique C2 completions, real R33/J33 and unchanged native SQL guards."""
    db = c2_db
    c2_history._seal_genuine_baseline(db)
    before = _image(db.connection)
    expected = c2_history.expected_currency_from_sources(db)
    baseline_currency = dict(expected)
    representative = db.chunks[2] != db.base.chunk_ids[0]
    c2_history._open_genuine_delete_fallback(
        db,
        requirement_ordinal=int(representative),
        selected_chunks=("c2-D",) if representative else None,
    )
    epoch, plan = db.runtime.epoch_id, db.runtime.plan
    withdrawn = next(key for key in expected if key[2] == db.chunks[2])
    expected[withdrawn] = None
    c2_history.assert_currency_sources_and_raw_history(db, expected)
    c2_history._assert_complete_point(db, epoch, db.revision(), expected)
    changes = (
        (("c2-D", "SUPPORT"),)
        if representative
        else tuple(zip(db.chunks[:2], ("NEUTRAL", "SUPPORT"), strict=True))
    )
    new_observations = []
    for chunk, label in changes:
        output = db.build(chunk, label, "b33-final")
        receipt = db.complete(output)
        assert not receipt.exact_replay
        observation = output.verifier_artifact.to_semantic_observation()
        expected[
            "requirement", observation.subject_id, chunk, observation.task_type
        ] = observation.observation_id
        new_observations.append(observation)
        c2_history.assert_currency_sources_and_raw_history(db, expected)
        c2_history._assert_complete_point(db, epoch, db.revision(), expected)
    raw_repo = c2_history._raw_python_point(db, epoch, db.revision(), expected)
    raw_image = _matching_from_raw_repository(raw_repo)
    if representative:
        observation = new_observations[0]
        assert (observation.observation_id,) not in before["observation"]
        requirement = db.group.requirements[1].requirement_version_id
        assert (requirement, sha("beta")) not in before["edge"]
        assert (db.group.group_version_id, sha("beta")) not in before["hash_mask"]
        assert (requirement, sha("beta")) in raw_image["edge"]
        assert (db.group.group_version_id, sha("beta")) in raw_image["hash_mask"]
    else:
        assert db.chunks[0] != db.chunks[1]
        for family in ("edge", "hash_mask", "hall"):
            baseline = {
                key: {
                    name: value
                    for name, value in row.items()
                    if name not in {"installed_epoch_id", "installed_revision"}
                }
                for key, (row, _) in before[family].items()
            }
            assert raw_image[family] == baseline
            # Each semantically equal carried key really is a positive working
            # row; an empty overlay cannot satisfy the UPDATE obligation.
            assert db.connection.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE epoch_id=%s").format(
                    sql.Identifier("groundloop_m5_matching_" + family + "_working")
                ),
                (epoch,),
            ).fetchone() == (len(baseline),)
    ready = r33._edge(
        db.connection,
        epoch,
        plan.structural_event_id,
        "semantic_complete",
        db.revision(),
    )
    revision = ready.anchor.anchor_revision
    with _c2_seal_principal(db, nonowner) as ports:
        _assert_c2_promotion_and_outer_cut_retries(
            db, ports, plan, epoch, revision, nonowner
        )
        control = {"nonowner": nonowner}
        result = _controlled_outer_document_seal(
            db, ports, plan, epoch, revision, control
        )
        assert result.state is M5RunState.SEALED
        assert len(control["mutations"]) == 13
        assert control["all_sql"][-1] == "SET CONSTRAINTS ALL IMMEDIATE"
        operations = dict(control["mutations"])
        if representative:
            for family in ("observation", "edge", "hash_mask"):
                assert (
                    sum(
                        count
                        for query, count in operations.items()
                        if query.startswith(
                            "INSERT INTO groundloop_m5_matching_" + family + "_current"
                        )
                    )
                    == 1
                )
        else:
            for family in ("edge", "hash_mask", "hall"):
                assert (
                    sum(
                        count
                        for query, count in operations.items()
                        if query.startswith(
                            "UPDATE groundloop_m5_matching_" + family + "_current"
                        )
                    )
                    == 1
                )
        _assert_native_readiness_replay(
            ports.connection, plan, epoch, revision, ready.anchor
        )
    with db.runtime.reconnect() as reader:
        reader.autocommit = True
        _assert_published_raw_images(reader, raw_repo, epoch, expected, revision + 1)
        adapter = SimpleNamespace(connection=reader, schema=db.schema, group=db.group)
        _assert_independent_sql_states(adapter, raw_repo, epoch, expected)
        _assert_native_readiness_replay(reader, plan, epoch, revision, ready.anchor)
        for key, old in baseline_currency.items():
            assert reader.execute(
                "SELECT valid_to_epoch FROM groundloop_published_observation_currency "
                "WHERE observation_id=%s",
                (old,),
            ).fetchall() == [(epoch if expected[key] != old else None,)]
        assert reader.execute(
            "SELECT installed_epoch_id,installed_revision "
            "FROM groundloop_m5_matching_image_current"
        ).fetchone() == (epoch, revision + 1)
        after = _image(reader)
        if not representative:
            _assert_retained_tombstone_absence(
                reader, epoch, required_family="observation"
            )
            for family in ("edge", "hash_mask", "hall"):
                for key in before[family]:
                    assert before[family][key] != after[family][key]
                    assert (
                        after[family][key][0]["installed_epoch_id"],
                        after[family][key][0]["installed_revision"],
                    ) == (epoch, revision + 1)
