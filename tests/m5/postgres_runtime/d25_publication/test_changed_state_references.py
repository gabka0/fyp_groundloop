"""Store-derived event-result reference boundary tests."""

from __future__ import annotations

import hashlib
import importlib
from typing import Any, cast

import pytest
from psycopg import sql

from groundloop.errors import ValidationError
from groundloop.m4.contracts import stable_m4_digest
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import M5RuntimeWork
from groundloop.m5.runtime.postgres_matching_publication import (
    M5MatchingPublicationChildren,
    _prepare_preterminal_matching_publication_children,
    build_matching_publication_children,
    prepare_matching_publication_children,
)
from groundloop.postgres.migrations import (
    M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256,
    M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_BUNDLE_SHA256,
    M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_MIGRATION_SHA256,
    M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID,
    M5_PRETERMINAL_SEAL_CONTEXT_ORACLE_SHA256,
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


def _event_work_row(
    *, event_id: str = "event-9", epoch_id: int = 9, work: M5RuntimeWork | None = None
) -> tuple[object, ...]:
    exact_work = M5RuntimeWork() if work is None else work
    return (
        event_id,
        epoch_id,
        "event",
        exact_work.work_digest,
        *exact_work.counter_values(),
    )


def _event_result(
    *,
    publication_id: str | None = None,
    event_work_digest: str | None = None,
    children: M5MatchingPublicationChildren | None = None,
) -> tuple[object, ...]:
    event_id = "event-9"
    epoch_id = 9
    payload_hash = _sha("payload")
    publication = publication_id or stable_m4_digest("m4-publication-v1", "9")
    exact_children = children or M5MatchingPublicationChildren((), ())
    combined = digests.combined_status_delta_set_digest(exact_children.combined_deltas)
    changed = digests.changed_state_set_digest(
        reference.reference_digest
        for reference in exact_children.changed_state_references
    )
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
    event_work = event_work_digest or M5RuntimeWork().work_digest
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
        len(exact_children.combined_deltas),
        len(exact_children.changed_state_references),
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
        event_work: tuple[object, ...] | None = _event_work_row(),
        certificate: tuple[object, ...] | None = None,
    ) -> None:
        self.envelope = envelope
        self.present = present or []
        self.closed = closed or []
        self.event_result = event_result
        self.event_work = event_work
        self.certificate = certificate
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
        elif "FROM groundloop_m5_runtime_work" in query:
            self.rows = [] if self.event_work is None else [self.event_work]
        elif query.startswith("SELECT certificate_digest FROM"):
            self.rows = [] if self.certificate is None else [self.certificate]
        else:  # pragma: no cover - a new query is an intentional test failure.
            raise AssertionError(query)
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def fetchone(self) -> tuple[object, ...] | None:
        return None if not self.rows else self.rows[0]


def _preterminal_context() -> tuple[object, ...]:
    return (
        "policy-v1",
        8,
        8,
        3,
        1,
        3,
        "predecessor-sealed-at",
        "policy-v1",
    )


def _preterminal_ledger() -> tuple[object, ...]:
    return (
        M5_PRETERMINAL_SEAL_CONTEXT_BUNDLE_ID,
        M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_BUNDLE_SHA256,
        M5_ACCEPTED_PRETERMINAL_SEAL_CONTEXT_MIGRATION_SHA256,
        M5_PRETERMINAL_SEAL_CONTEXT_ORACLE_SHA256,
        M5_ACCEPTED_BOUNDED_DOCUMENT_WITHDRAWAL_BUNDLE_SHA256,
    )


def _preterminal_envelope() -> tuple[object, ...]:
    return (
        "event-9",
        _sha("payload"),
        9,
        4,
        "committed",
        "sealed",
        "complete",
        "strict",
        "event-sealed-at",
        "event-9",
        "semantic_complete",
        None,
        3,
        8,
        0,
        0,
        0,
        8,
        "observe_requirement",
        "policy-v1",
        3,
        "committed",
        "sealed",
        "complete",
        "strict",
        "predecessor-sealed-at",
        9,
        9,
        4,
        "policy-v1",
        9,
        4,
        8,
        3,
        "policy-v1",
        3,
        3,
        3,
        False,
        3,
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        0,
        0,
        0,
        0,
        1,
        ["policy-v1"],
        ["policy-v1"],
    )


class _PreterminalRowsCursor(_RowsCursor):
    def __init__(
        self,
        *,
        schema: str | None = "selected-schema",
        ledger_catalog: tuple[object, ...] | None = ("r", "p"),
        ledger: tuple[object, ...] | None = _preterminal_ledger(),
        context: tuple[object, ...] | None = _preterminal_context(),
        preterminal: tuple[object, ...] | None = _preterminal_envelope(),
        present: list[tuple[object, ...]] | None = None,
        certificate: tuple[object, ...] | None = None,
    ) -> None:
        super().__init__(
            envelope=_envelope(),
            present=present,
            certificate=certificate,
        )
        self.schema = schema
        self.ledger_catalog = ledger_catalog
        self.ledger = ledger
        self.context = context
        self.preterminal = preterminal
        self.calls: list[str] = []

    def execute(
        self, query: str | sql.Composable, params: object = None
    ) -> _PreterminalRowsCursor:
        query = query if isinstance(query, str) else query.as_string(None)
        normalized = " ".join(query.split())
        self.calls.append(normalized)
        # Preserve the original closed-template fake's response mapping, while
        # recording the actual safely quoted namespace for exact assertions.
        if self.schema is not None:
            qualifier = sql.Identifier(self.schema).as_string(None) + "."
            query = query.replace(qualifier, "").replace('"', "")
        if normalized == "SELECT pg_catalog.current_schema()":
            self.rows = [(self.schema,)]
        elif "FROM pg_catalog.pg_class AS relation" in query:
            self.rows = [] if self.ledger_catalog is None else [self.ledger_catalog]
        elif "FROM groundloop_m5_schema_bundle" in query:
            self.rows = [] if self.ledger is None else [self.ledger]
        elif "FROM groundloop_m5_matching_read_preterminal_seal_context" in query:
            self.rows = [] if self.context is None else [self.context]
        elif "JOIN groundloop_m5_matching_image_current" in query:
            self.rows = [] if self.preterminal is None else [self.preterminal]
        else:
            super().execute(query, params)
        return self


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


@pytest.mark.parametrize(
    ("epoch_id", "expected_revision", "sealed_revision", "message"),
    (
        (True, 3, 4, "epoch_id must be an integer"),
        (9, False, 4, "expected_revision must be an integer"),
        (9, 3, "4", "sealed_revision must be an integer"),
        (9, 3, 5, r"expected_revision \+ 1"),
    ),
)
def test_preterminal_preparation_rejects_bad_coordinates_before_sql(
    epoch_id: object,
    expected_revision: object,
    sealed_revision: object,
    message: str,
) -> None:
    cursor = _PreterminalRowsCursor()
    with pytest.raises(ValidationError, match=message):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor),
            epoch_id=cast(Any, epoch_id),
            expected_revision=cast(Any, expected_revision),
            sealed_revision=cast(Any, sealed_revision),
        )
    assert cursor.calls == []


@pytest.mark.parametrize(
    ("section", "index", "value"),
    (
        ("context", 0, "wrong-policy"),
        ("context", 1, 7),
        ("context", 2, 7),
        ("context", 3, 2),
        ("context", 4, 2),
        ("context", 6, "wrong-seal-time"),
        ("preterminal", 20, 2),
        ("preterminal", 26, 8),
        ("preterminal", 28, 3),
        ("preterminal", 30, 8),
        ("preterminal", 10, "sealed"),
        ("preterminal", 12, 2),
        ("preterminal", 37, 2),
        ("preterminal", 38, True),
        ("preterminal", 39, 2),
        ("preterminal", 40, True),
        ("preterminal", 50, 1),
        ("preterminal", 51, 1),
        ("preterminal", 52, 1),
    ),
    ids=(
        "wrong-policy",
        "wrong-anchor-m4",
        "wrong-anchor-m5",
        "wrong-anchor-revision",
        "wrong-anchor-activation",
        "wrong-anchor-seal-time",
        "wrong-predecessor",
        "wrong-m4-head",
        "wrong-m5-head",
        "pre-promotion-image",
        "terminal-runtime",
        "wrong-runtime-revision",
        "wrong-work-accumulator-revision",
        "terminal-work-accumulator",
        "wrong-timing-accumulator-revision",
        "terminal-timing-accumulator",
        "existing-result",
        "existing-result-delta",
        "existing-child",
    ),
)
def test_preterminal_preparation_fails_closed_on_envelope_drift(
    section: str, index: int, value: object
) -> None:
    context = list(_preterminal_context())
    preterminal = list(_preterminal_envelope())
    selected = {
        "context": context,
        "preterminal": preterminal,
    }[section]
    selected[index] = value
    cursor = _PreterminalRowsCursor(
        context=tuple(context),
        preterminal=tuple(preterminal),
    )
    with pytest.raises(ValidationError, match="preterminal"):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor),
            epoch_id=9,
            expected_revision=3,
            sealed_revision=4,
        )


@pytest.mark.parametrize("position", range(8))
def test_preterminal_rejects_each_null_attested_coordinate(position: int) -> None:
    context = list(_preterminal_context())
    context[position] = None
    cursor = _PreterminalRowsCursor(context=tuple(context))
    with pytest.raises(ValidationError, match="context coordinates"):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor), epoch_id=9, expected_revision=3, sealed_revision=4
        )


@pytest.mark.parametrize(
    ("position", "value"),
    (
        (0, "different-event"),
        (1, "malformed-payload"),
        (2, 10),
        (3, 5),
        (4, "uncommitted"),
        (5, "complete"),
        (6, "pending"),
        (7, "relaxed"),
        (8, None),
        (9, "different-event"),
        (11, "premature-terminal"),
        (13, 7),
        (14, 1),
        (15, 1),
        (16, 1),
        (17, 7),
        (19, "wrong-policy"),
        (21, "uncommitted"),
        (22, "complete"),
        (23, "pending"),
        (24, "relaxed"),
        (25, "wrong-predecessor-time"),
        (27, 8),
        (29, "wrong-policy"),
        (31, 3),
        (32, 7),
        (33, 2),
        (34, "wrong-policy"),
        (35, 4),
        (36, 4),
        (41, "seal"),
        (42, "premature-source"),
        (43, _sha("premature-key")),
        (44, 4),
        (45, "wrong-deactivation-event"),
        (49, 2),
        (53, 2),
        (54, ["wrong-anchor-policy"]),
        (55, ["wrong-current-policy"]),
    ),
)
def test_preterminal_independent_envelope_coordinates_are_all_checked(
    position: int, value: object
) -> None:
    image = list(_preterminal_envelope())
    image[position] = value
    cursor = _PreterminalRowsCursor(preterminal=tuple(image))
    with pytest.raises(ValidationError):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor), epoch_id=9, expected_revision=3, sealed_revision=4
        )


def test_preterminal_preparation_rejects_absent_private_context() -> None:
    cursor = _PreterminalRowsCursor(context=None)
    with pytest.raises(ValidationError, match="context cardinality is invalid"):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor),
            epoch_id=9,
            expected_revision=3,
            sealed_revision=4,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("schema", None, "current schema"),
        ("ledger_catalog", None, "permanent ledger"),
        ("ledger_catalog", ("v", "p"), "permanent ledger"),
        ("ledger_catalog", ("r", "t"), "permanent ledger"),
        ("ledger", None, "exact migration 019"),
        *(
            (
                "ledger",
                tuple(
                    "wrong" if index == position else value
                    for index, value in enumerate(_preterminal_ledger())
                ),
                "exact migration 019",
            )
            for position in range(5)
        ),
    ),
)
def test_preterminal_schema_and_ledger_fail_before_accessor(
    field: str, value: object, message: str
) -> None:
    cursor = _PreterminalRowsCursor(**cast(Any, {field: value}))
    with pytest.raises(ValidationError, match=message):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor), epoch_id=9, expected_revision=3, sealed_revision=4
        )
    assert not any("read_preterminal_seal_context" in call for call in cursor.calls)


def test_preterminal_capture_and_accessor_once_with_quoted_schema() -> None:
    cursor = _PreterminalRowsCursor(schema='selected"schema')
    _prepare_preterminal_matching_publication_children(
        cast(Any, cursor), epoch_id=9, expected_revision=3, sealed_revision=4
    )
    assert cursor.calls.count("SELECT pg_catalog.current_schema()") == 1
    assert sum("read_preterminal_seal_context" in call for call in cursor.calls) == 1
    assert all("pg_temp" not in call for call in cursor.calls)
    qualified_calls = [call for call in cursor.calls if '"groundloop_' in call]
    assert qualified_calls
    assert all('"selected""schema"."groundloop_' in call for call in qualified_calls)


def test_preterminal_certificate_only_children_match_terminal_builder() -> None:
    certificate = (_sha("group-certificate"),)
    preterminal_cursor = _PreterminalRowsCursor(
        present=[("group_certificate", "group-1")],
        certificate=certificate,
    )
    prepared = _prepare_preterminal_matching_publication_children(
        cast(Any, preterminal_cursor),
        epoch_id=9,
        expected_revision=3,
        sealed_revision=4,
    )
    assert prepared.combined_deltas == ()
    assert tuple(
        (value.kind.value, value.object_id, value.state_artifact_hash)
        for value in prepared.changed_state_references
    ) == (("group_certificate", "group-1", certificate[0]),)

    terminal_cursor = _RowsCursor(
        envelope=_envelope(),
        present=[("group_certificate", "group-1")],
        certificate=certificate,
        event_result=_event_result(children=prepared),
    )
    assert (
        build_matching_publication_children(
            cast(Any, terminal_cursor), epoch_id=9, sealed_revision=4
        )
        == prepared
    )


def test_preterminal_preparation_rejects_missing_promoted_binding() -> None:
    cursor = _PreterminalRowsCursor(
        present=[("group_certificate", "group-1")],
        certificate=None,
    )
    with pytest.raises(ValidationError, match="lacks its published binding"):
        _prepare_preterminal_matching_publication_children(
            cast(Any, cursor),
            epoch_id=9,
            expected_revision=3,
            sealed_revision=4,
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


def test_result_binding_rejects_missing_exact_event_work() -> None:
    cursor = _RowsCursor(
        envelope=_envelope(),
        event_result=_event_result(),
        event_work=None,
    )
    with pytest.raises(ValidationError, match="one exact event work row"):
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )


def test_result_binding_rejects_arbitrary_parent_event_work_digest() -> None:
    cursor = _RowsCursor(
        envelope=_envelope(),
        event_result=_event_result(event_work_digest=_sha("arbitrary-parent-work")),
    )
    with pytest.raises(ValidationError, match="does not bind exact event work"):
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )


@pytest.mark.parametrize(
    "event_work",
    (
        _event_work_row(event_id="wrong-event"),
        _event_work_row(epoch_id=8),
        (
            "event-9",
            9,
            "call",
            M5RuntimeWork().work_digest,
            *M5RuntimeWork().counter_values(),
        ),
    ),
    ids=("event", "epoch", "kind"),
)
def test_result_binding_rejects_mismatched_event_work_authority(
    event_work: tuple[object, ...],
) -> None:
    cursor = _RowsCursor(
        envelope=_envelope(),
        event_result=_event_result(),
        event_work=event_work,
    )
    with pytest.raises(ValidationError, match="does not bind exact event work"):
        build_matching_publication_children(
            cast(Any, cursor), epoch_id=9, sealed_revision=4
        )


def test_result_binding_rejects_wrong_event_work_public_delta_count() -> None:
    event_work = M5RuntimeWork(public_delta_count=1)
    cursor = _RowsCursor(
        envelope=_envelope(),
        event_result=_event_result(event_work_digest=event_work.work_digest),
        event_work=_event_work_row(work=event_work),
    )
    with pytest.raises(ValidationError, match="does not bind exact event work"):
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
