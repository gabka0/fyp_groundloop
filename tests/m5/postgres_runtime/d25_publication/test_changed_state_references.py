"""Store-derived event-result reference boundary tests."""

from __future__ import annotations

import hashlib
import importlib
from typing import Any, cast

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.postgres_matching_publication import (
    build_matching_publication_children,
    prepare_matching_publication_children,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _envelope(*, update_kind: str = "observe_requirement") -> tuple[object, ...]:
    return (
        "event-9",
        _sha("payload"),
        9,
        4,
        "committed",
        "sealed",
        "complete",
        "strict",
        True,
        "event-9",
        "sealed",
        True,
        4,
        8,
        8,
        update_kind,
        9,
        9,
        4,
        None,
        None,
        None,
        None,
        0,
        3,
        "policy-v1",
    )


def _event_result(*, publication_id: str | None = None) -> tuple[object, ...]:
    event_id = "event-9"
    epoch_id = 9
    payload_hash = _sha("payload")
    publication = publication_id or stable_m4_digest("m4-publication-v1", "9")
    combined = digests.combined_status_delta_set_digest(())
    changed = digests.changed_state_set_digest(())
    open_binding = digests.open_event_receipt_binding_digest(
        epoch_id=epoch_id,
        replayed=False,
        already_sealed=False,
        publication_id=None,
        already_failed=False,
        failure_reason=None,
    )
    publication_binding = digests.publication_receipt_binding_digest(
        epoch_id=epoch_id,
        publication_id=publication,
        replayed=False,
    )
    event_work = _sha("event-work")
    logical = digests.event_run_logical_result_digest(
        event_id=event_id,
        payload_hash=payload_hash,
        epoch_id=epoch_id,
        sealed_or_failed_outcome="sealed",
        original_open_receipt_binding_hash=open_binding,
        original_publication_receipt_binding_hash=publication_binding,
        event_work_digest=event_work,
        combined_status_delta_set_hash=combined,
        changed_state_set_hash=changed,
        failure_reason=None,
    )
    return (
        event_id,
        payload_hash,
        epoch_id,
        "sealed",
        publication,
        combined,
        changed,
        0,
        0,
        None,
        open_binding,
        publication_binding,
        event_work,
        logical,
    )


class _RowsCursor:
    def __init__(
        self,
        *,
        envelope: tuple[object, ...],
        present: list[tuple[object, ...]] | None = None,
        closed: list[tuple[object, ...]] | None = None,
        event_result: tuple[object, ...] | None = None,
    ) -> None:
        self.envelope = envelope
        self.present = present or []
        self.closed = closed or []
        self.event_result = event_result
        self.rows: list[tuple[object, ...]] = []

    def execute(self, query: str, params: object = None) -> _RowsCursor:
        del params
        if "FROM groundloop_epoch AS epoch" in query:
            self.rows = [self.envelope]
        elif "AS published" in query:
            self.rows = list(self.present)
        elif "AS closed" in query:
            self.rows = list(self.closed)
        elif "FROM groundloop_m5_event_result" in query:
            self.rows = [] if self.event_result is None else [self.event_result]
        else:  # pragma: no cover - a new query is an intentional test failure.
            raise AssertionError(query)
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


def test_empty_preparation_then_exact_result_binding_is_deterministic() -> None:
    cursor = _RowsCursor(
        envelope=_envelope(),
        event_result=_event_result(),
    )
    prepared = prepare_matching_publication_children(
        cast(Any, cursor), epoch_id=9, sealed_revision=4
    )
    assert prepared.combined_deltas == ()
    assert prepared.changed_state_references == ()
    assert (
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )
        == prepared
    )


def test_result_binding_rejects_wrong_publication_identity() -> None:
    cursor = _RowsCursor(
        envelope=_envelope(),
        event_result=_event_result(publication_id="wrong-publication"),
    )
    with pytest.raises(ValidationError, match="exact sealed event result"):
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )


def test_absence_candidate_still_requires_replace_or_retire() -> None:
    cursor = _RowsCursor(
        envelope=_envelope(),
        closed=[("requirement_state", "requirement-1")],
    )
    with pytest.raises(ValidationError, match="replace_group or retire_group"):
        prepare_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )


@pytest.mark.parametrize("action", ("REPLACE", "RETIRE"))
def test_live_d26_children_rederive_exact_sealed_result(action: str) -> None:
    migration017 = cast(
        Any,
        importlib.import_module("tests.m5.postgres_runtime.test_migration_017"),
    )

    prefix = f"lane-b-publication-{action.lower()}"
    with migration017._pre017_schema() as (connection, _):
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
            group_id=snapshot.complete_group_id,
            base=snapshot.epoch_id,
        )
        sealed = migration017._seal_d26_event(
            connection,
            prefix=prefix,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            predecessor=predecessor,
            action=action,
            retire_current_physical=True,
        )
        children = build_matching_publication_children(
            connection.cursor(),
            epoch_id=sealed.epoch_id,
            sealed_revision=sealed.revision,
        )
        assert {
            (
                reference.kind.value,
                reference.object_id,
                reference.state_artifact_hash,
                reference.reference_digest,
            )
            for reference in children.changed_state_references
        } == set(sealed.references)
        connection.execute(
            "ALTER TABLE groundloop_m5_event_result DISABLE TRIGGER USER"
        )
        assert (
            connection.execute(
                "UPDATE groundloop_m5_event_result SET publication_id=%s "
                "WHERE structural_event_id=%s",
                ("0" * 64, sealed.event_id),
            ).rowcount
            == 1
        )
        connection.execute("ALTER TABLE groundloop_m5_event_result ENABLE TRIGGER USER")
        with pytest.raises(ValidationError, match="exact sealed event result"):
            build_matching_publication_children(
                connection.cursor(),
                epoch_id=sealed.epoch_id,
                sealed_revision=sealed.revision,
            )
        connection.rollback()
