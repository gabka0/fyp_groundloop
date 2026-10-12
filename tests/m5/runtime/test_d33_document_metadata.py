"""Pure M33 metadata authority tests; not public M5 composition evidence."""

from __future__ import annotations

import inspect
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from m5.runtime.test_d29_hydration_terminal_cutoff import _document_case

from groundloop.errors import EventConflictError
from groundloop.m4.application import ApplicationExecutionPolicy
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import postgres_withdrawal as withdrawal


class MetadataCursor:
    """Strict SELECT-only adapter for the three metadata point reads."""

    def __init__(self, event: Any, source_uri: str | None, authority: str) -> None:
        self.event = event
        self.epoch = event.expected_previous_published_epoch_id + 1
        self.owner: tuple[Any, ...] | None = (
            event.event.document_id,
            source_uri,
            authority,
            1,
            self.epoch,
        )
        self.overlay = [(self.epoch, event.event.document_id, source_uri, authority)]
        payload = withdrawal._structural_payload_from_event(event)
        assert payload.inserted is not None
        resolved = replace(
            payload,
            inserted=replace(
                payload.inserted, source_uri=source_uri, authority_class=authority
            ),
        )
        self.manifest: Any = {
            withdrawal._M4_RUNTIME_MANIFEST_KEY: {
                "event_manifest": resolved.manifest,
                "scope_claim_ids": {},
                "failure_reason": None,
            }
        }
        self.queries: list[str] = []
        self.rows: list[Any] = []

    def execute(self, statement: str, params: tuple[Any, ...]) -> MetadataCursor:
        self.queries.append(statement)
        assert statement.lstrip().startswith("SELECT")
        assert "FOR UPDATE" not in statement
        assert "publication_head" not in statement
        assert "current_version" not in statement
        if "JOIN groundloop_document AS document" in statement:
            assert params == (self.event.event.old_document_version_id,)
            self.rows = [] if self.owner is None else [self.owner]
        elif "FROM groundloop_m4_document_metadata_overlay" in statement:
            assert params == (self.epoch,)
            self.rows = self.overlay
        elif "SELECT manifest FROM groundloop_m4_update" in statement:
            assert params == (self.epoch,)
            self.rows = [(self.manifest,)]
        else:
            raise AssertionError(f"unexpected metadata query: {statement}")
        return self

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[Any]:
        return self.rows


def _replacement() -> Any:
    return _document_case("replace", "d33-pure-metadata")[1]


@pytest.mark.parametrize("source_uri", [None, "", " ", "https://例子.hk/α?x=é"])
@pytest.mark.parametrize("authority", ["journal", " 政府 "])
def test_replacement_exact_metadata_and_unchanged_legacy_identity(
    source_uri: str | None, authority: str
) -> None:
    event = _replacement()
    before = legacy_event_payload_digest(event.event)
    cursor = MetadataCursor(event, source_uri, authority)
    preview = withdrawal._document_structural_payload(cursor, event)
    assert preview.inserted is not None
    assert (preview.inserted.source_uri, preview.inserted.authority_class) == (
        source_uri,
        authority,
    )
    assert len(cursor.queries) == 1
    retained = withdrawal._document_structural_payload(
        cursor, event, epoch_id=cursor.epoch
    )
    assert retained == preview
    assert before == legacy_event_payload_digest(event.event) == event.payload_hash
    direct = withdrawal._direct_open_from_withdrawal(
        event,
        ApplicationExecutionPolicy("a" * 64, "b" * 64, "c" * 64),
        withdrawal.StructuralWithdrawal(
            withdrawal.WithdrawalPlan((), (), (), (), (), 0, 0, 0), ()
        ),
        structural_payload=retained,
    )
    declaration = withdrawal._document_declaration_manifest(direct)
    nested = declaration[withdrawal._DOCUMENT_DECLARATION_KEY]
    assert set(nested) == {
        "direct_root_job_ids",
        "direct_scope_root_job_ids",
        "direct_fallback_claim_ids",
    }
    assert direct.structural_payload == retained


@pytest.mark.parametrize("kind", ["insert", "delete"])
def test_insert_delete_remain_exact_and_issue_no_metadata_queries(kind: str) -> None:
    event = _document_case(kind, "d33-nonreplacement")[1]
    cursor = SimpleNamespace(
        execute=lambda *_args: pytest.fail("unexpected metadata read")
    )
    result = withdrawal._document_structural_payload(cursor, event)
    assert result == withdrawal._structural_payload_from_event(event)
    if kind == "insert":
        assert result.inserted is not None
        assert (result.inserted.source_uri, result.inserted.authority_class) == (
            None,
            "dynamic",
        )
    else:
        assert result.inserted is None


def test_insert_does_not_inherit_replacement_historical_interval_allowance() -> None:
    event = _document_case("insert", "d33-unchanged-insert")[1]
    payload = withdrawal._structural_payload_from_event(event)
    assert payload.inserted is not None
    inserted = payload.inserted
    epoch = event.expected_previous_published_epoch_id + 1
    chunk = inserted.chunks[0]

    class InsertCursor:
        rows: list[Any]

        def execute(self, source: str, *_args: Any) -> Any:
            assert source.lstrip().startswith("SELECT")
            if "FROM groundloop_m4_structural_deactivation" in source:
                self.rows = []
            elif "FROM groundloop_m4_document_metadata_overlay" in source:
                self.rows = [(epoch, inserted.version.document_id, None, "dynamic")]
            elif "JOIN groundloop_chunk_version AS chunk" in source:
                self.rows = [
                    (
                        inserted.version.document_version_id,
                        inserted.version.document_id,
                        inserted.version.content_hash,
                        epoch,
                        epoch + 1,
                        chunk.chunk_version_id,
                        chunk.chunk_index,
                        chunk.text,
                        chunk.text_hash,
                        inserted.chunker_version,
                        epoch,
                        epoch + 1,
                    )
                ]
            elif "FROM groundloop_document" in source:
                self.rows = [(inserted.version.document_id, None, "dynamic")]
            else:
                raise AssertionError(source)
            return self

        def fetchone(self) -> Any:
            return self.rows[0] if self.rows else None

        def fetchall(self) -> list[Any]:
            return self.rows

    closure = SimpleNamespace(
        epoch_id=epoch, previous_epoch_id=epoch - 1, runtime_state="sealed"
    )
    with pytest.raises(EventConflictError, match="historical interval"):
        withdrawal._validated_structural_source_chunks(
            InsertCursor(), event, closure, lock_authority=False, event_local_only=True
        )


@pytest.mark.parametrize(
    "field,value",
    [
        (0, "other-document"),
        (1, 123),
        (2, None),
        (2, " "),
        (3, 0),
        (3, True),
        (3, 999),
        (4, 1),
        (4, True),
    ],
)
def test_wrong_owner_metadata_or_predecessor_is_rejected(
    field: int, value: Any
) -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    assert cursor.owner is not None
    changed = list(cursor.owner)
    changed[field] = value
    cursor.owner = tuple(changed)
    with pytest.raises(EventConflictError, match="owning document"):
        withdrawal._document_structural_payload(cursor, event)


def test_missing_old_version_or_owner_is_rejected() -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    cursor.owner = None
    with pytest.raises(EventConflictError, match="owning document"):
        withdrawal._document_structural_payload(cursor, event)


@pytest.mark.parametrize("location", ["manifest", "overlay", "public"])
@pytest.mark.parametrize("field", ["source_uri", "authority_class"])
def test_each_retained_metadata_conflict_is_rejected(location: str, field: str) -> None:
    event = _replacement()
    cursor = MetadataCursor(event, "https://例子.hk/α", "journal")
    field_index = 0 if field == "source_uri" else 1
    if location == "manifest":
        cursor.manifest[withdrawal._M4_RUNTIME_MANIFEST_KEY]["event_manifest"][
            field
        ] = "wrong"
    elif location == "overlay":
        row = list(cursor.overlay[0])
        row[2 + field_index] = "wrong"
        cursor.overlay = [tuple(row)]
    else:
        assert cursor.owner is not None
        row = list(cursor.owner)
        row[1 + field_index] = "wrong"
        cursor.owner = tuple(row)
    with pytest.raises(EventConflictError, match="metadata|manifest"):
        withdrawal._document_structural_payload(cursor, event, epoch_id=cursor.epoch)


def test_coherent_overlay_and_manifest_lie_cannot_override_public_owner() -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    cursor.overlay[0] = (cursor.epoch, event.event.document_id, "lie", "journal")
    cursor.manifest[withdrawal._M4_RUNTIME_MANIFEST_KEY]["event_manifest"][
        "source_uri"
    ] = "lie"
    with pytest.raises(EventConflictError, match="differs from owner"):
        withdrawal._document_structural_payload(cursor, event, epoch_id=cursor.epoch)


@pytest.mark.parametrize("mode", ["missing", "extra", "wrong-document", "wrong-epoch"])
def test_exact_retained_overlay_closure(mode: str) -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    if mode == "missing":
        cursor.overlay = []
    elif mode == "extra":
        cursor.overlay.append((cursor.epoch, "extra", None, "journal"))
    elif mode == "wrong-document":
        cursor.overlay = [(cursor.epoch, "other", None, "journal")]
    else:
        cursor.overlay = [(cursor.epoch + 1, event.event.document_id, None, "journal")]
    with pytest.raises(EventConflictError, match="differs from owner"):
        withdrawal._document_structural_payload(cursor, event, epoch_id=cursor.epoch)


@pytest.mark.parametrize(
    "mode", ["missing", "extra-wrapper", "extra-runtime", "different-content"]
)
def test_exact_retained_manifest_closure(mode: str) -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    if mode == "missing":
        cursor.manifest = None
    elif mode == "extra-wrapper":
        cursor.manifest["extra"] = {}
    elif mode == "extra-runtime":
        cursor.manifest[withdrawal._M4_RUNTIME_MANIFEST_KEY]["extra"] = None
    else:
        cursor.manifest[withdrawal._M4_RUNTIME_MANIFEST_KEY]["event_manifest"][
            "inserted_content_hash"
        ] = "0" * 64
    with pytest.raises(EventConflictError, match="event manifest changed"):
        withdrawal._document_structural_payload(cursor, event, epoch_id=cursor.epoch)


@pytest.mark.parametrize("epoch", [True, 0, 1])
def test_retained_epoch_is_exact_and_after_predecessor(epoch: Any) -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    with pytest.raises(EventConflictError, match="metadata epoch"):
        withdrawal._document_structural_payload(cursor, event, epoch_id=epoch)


def test_replacement_manifest_validation_requires_derived_payload() -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    with pytest.raises(EventConflictError, match="store-derived metadata"):
        withdrawal._validate_direct_update_manifest(
            cursor.manifest, event, direct_scope_root_job_ids=(), runtime_state="open"
        )
    payload = withdrawal._document_structural_payload(
        cursor, event, epoch_id=cursor.epoch
    )
    withdrawal._validate_direct_update_manifest(
        cursor.manifest,
        event,
        direct_scope_root_job_ids=(),
        runtime_state="open",
        structural_payload=payload,
    )


@pytest.mark.parametrize("field", ["source_uri", "authority_class"])
def test_prepared_snapshot_binds_metadata_and_rejects_private_substitution(
    monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    payload = withdrawal._document_structural_payload(cursor, event)
    policy = ApplicationExecutionPolicy("a" * 64, "b" * 64, "c" * 64)
    binding = withdrawal._D29DocumentOpenBinding(
        id(cursor),
        7,
        11,
        "groundloop",
        cursor.epoch,
        event.structural_event_id,
        event.payload_hash,
        event.expected_previous_published_epoch_id,
        ("old-chunk",),
    )
    monkeypatch.setattr(
        withdrawal, "_capture_d29_document_open_binding", lambda *_args: binding
    )
    prepared = withdrawal._PreparedDocumentOpen(
        binding,
        0,
        event,
        policy,
        SimpleNamespace(),
        ("old-chunk",),
        SimpleNamespace(),
        (),
        (),
        payload,
    )
    prepared.prepared_identity = id(prepared)
    withdrawal._seal_prepared_document_open(prepared)
    withdrawal._validate_prepared_document_open(
        cursor,
        event,
        prepared,
        execution_policy=policy,
        phase=withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
    )
    assert prepared.authority_snapshot is not None
    original = deepcopy(prepared.authority_snapshot.structural_payload)
    assert payload.inserted is not None
    prepared.structural_payload = replace(
        payload, inserted=replace(payload.inserted, **{field: "wrong"})
    )
    assert prepared.authority_snapshot.structural_payload == original
    with pytest.raises(EventConflictError, match="changed|substituted"):
        withdrawal._validate_prepared_document_open(
            cursor,
            event,
            prepared,
            execution_policy=policy,
            phase=withdrawal._DocumentOpenPhase.LOCATORS_GATHERED,
        )


@pytest.mark.parametrize("field", ["source_uri", "authority_class", "missing"])
def test_continuation_rederives_metadata_before_tier_eight(
    monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    """Unit ordering only: no fabricated SQL authority is called a live gate."""
    event = _replacement()
    cursor = MetadataCursor(event, None, "journal")
    payload = withdrawal._document_structural_payload(cursor, event)
    policy = ApplicationExecutionPolicy("a" * 64, "b" * 64, "c" * 64)
    binding = withdrawal._D29DocumentOpenBinding(
        id(cursor),
        7,
        11,
        "groundloop",
        cursor.epoch,
        event.structural_event_id,
        event.payload_hash,
        event.expected_previous_published_epoch_id,
        ("old-chunk",),
    )
    closure = SimpleNamespace(epoch_id=cursor.epoch)
    assert payload.inserted is not None
    prepared_payload = (
        None
        if field == "missing"
        else replace(
            payload, inserted=replace(payload.inserted, **{field: "stale-preview"})
        )
    )
    prepared = withdrawal._PreparedDocumentOpen(
        binding,
        0,
        event,
        policy,
        closure,
        ("old-chunk",),
        SimpleNamespace(),
        (),
        (),
        prepared_payload,
    )
    prepared.prepared_identity = id(prepared)
    withdrawal._seal_prepared_document_open(prepared)
    monkeypatch.setattr(
        withdrawal, "_capture_d29_document_open_binding", lambda *_args: binding
    )
    monkeypatch.setattr(
        withdrawal, "_read_existing_document_closure", lambda *_args: closure
    )
    monkeypatch.setattr(
        withdrawal,
        "_validated_structural_source_chunks",
        lambda *_args, **_kwargs: ("old-chunk",),
    )
    monkeypatch.setattr(
        withdrawal,
        "_lock_d29_touched_membership",
        lambda *_args, **_kwargs: pytest.fail("tier eight reached"),
    )
    with pytest.raises(EventConflictError, match="prepared document metadata changed"):
        withdrawal._continue_locked_document_open(
            cursor,
            event,
            prepared,
            execution_policy=policy,
            supplied_direct_open=None,
            supplied_requirement_withdrawal=None,
            supplied_requirement_roots=(),
            supplied_requirement_root_set_hash="0" * 64,
        )
    assert prepared.phase is withdrawal._DocumentOpenPhase.LOCATORS_GATHERED


def test_store_metadata_is_routed_through_all_private_composition_paths() -> None:
    for function in (
        withdrawal._preview_document_direct_open,
        withdrawal._prepare_locked_document_open,
        withdrawal._continue_locked_document_open,
        withdrawal._gather_d29_locator_authority,
        withdrawal._stored_direct_open,
        withdrawal._read_existing_document_closure,
        withdrawal._validated_structural_source_chunks,
    ):
        assert "_document_structural_payload(" in inspect.getsource(function)
    source = inspect.getsource(withdrawal._continue_locked_document_open)
    assert source.index("prepared document metadata changed") < source.index(
        "_lock_d29_touched_membership("
    )
    assert source.index("supplied_direct_open != direct_open") > source.index(
        "structural_payload=structural_payload"
    )


@pytest.mark.parametrize("field", ["source_uri", "authority_class"])
def test_real_continuation_comparison_rejects_stale_metadata_proposal(
    monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    """Exercise the actual final comparison; mocked SQL tiers are pure only."""
    harness, event, _ = _document_case("replace", "d33-stale-proposal")
    cursor = MetadataCursor(event, None, "journal")
    payload = withdrawal._document_structural_payload(cursor, event)
    policy = ApplicationExecutionPolicy("a" * 64, "b" * 64, "c" * 64)
    source_chunks = event.direct_plan.deactivated_chunk_version_ids
    direct_withdrawal = withdrawal._locked_direct_withdrawal(
        source_chunks=source_chunks, observations=(), candidates=()
    )
    direct = withdrawal._direct_open_from_withdrawal(
        event, policy, direct_withdrawal, structural_payload=payload
    )
    requirement = withdrawal.plan_requirement_withdrawal(
        event_id=event.structural_event_id,
        deactivated_chunk_version_ids=source_chunks,
        candidate_edges=(),
        observation_edges=(),
        cancelled_job_ids=(),
        active_requirement_version_ids=(),
    )
    roots = withdrawal._document_requirement_roots(
        event, harness.world.manifest, requirement
    )
    root_hash = withdrawal.digests.requirement_root_set_digest(
        root.job.logical_job_id for root in roots
    )
    arrays = withdrawal._validate_document_manifest(
        withdrawal._document_declaration_manifest(direct)
    )
    closure = SimpleNamespace(
        epoch_id=cursor.epoch,
        previous_epoch_id=event.expected_previous_published_epoch_id,
        candidate_policy=harness.world.manifest,
        requirement_root_set_hash=root_hash,
        direct_root_job_ids=arrays[0],
        direct_scope_root_job_ids=arrays[1],
        direct_fallback_claim_ids=arrays[2],
    )
    locator = SimpleNamespace(
        predecessor_snapshots=None,
        direct_observation_source_rows=(),
        admitted_locator_keys=(),
        qualifying_admitted_pair_digests=(),
        prospective_coordinates=withdrawal._document_declaration_coordinates(
            direct, roots
        ),
    )
    binding = withdrawal._D29DocumentOpenBinding(
        id(cursor),
        7,
        11,
        "groundloop",
        cursor.epoch,
        event.structural_event_id,
        event.payload_hash,
        event.expected_previous_published_epoch_id,
        source_chunks,
    )
    monkeypatch.setattr(
        withdrawal, "_capture_d29_document_open_binding", lambda *_a: binding
    )
    monkeypatch.setattr(
        withdrawal, "_read_existing_document_closure", lambda *_a: closure
    )
    monkeypatch.setattr(
        withdrawal,
        "_validated_structural_source_chunks",
        lambda *_a, **_k: source_chunks,
    )
    for name in (
        "_validate_d29_event_snapshot_image",
        "_lock_d29_touched_membership",
        "_lock_d29_jobs_and_reserve",
        "_lock_d29_attempt_outputs",
        "_lock_d29_candidate_source_closure",
    ):
        monkeypatch.setattr(withdrawal, name, lambda *_a, **_k: None)
    for name in (
        "_lock_d29_scopes_and_reserve",
        "_candidate_edges",
        "_lock_d29_verifier_provenance",
    ):
        monkeypatch.setattr(withdrawal, name, lambda *_a, **_k: ())
    for name in ("_lock_d29_tier_10_authority", "_lock_d29_observation_authority"):
        monkeypatch.setattr(withdrawal, name, lambda *_a, **_k: ((), ()))
    for name in (
        "_lock_d30_dynamic_owner_topology",
        "_build_d29_direct_matching_authority",
    ):
        monkeypatch.setattr(withdrawal, name, lambda *_a, **_k: SimpleNamespace())

    def prepared() -> Any:
        result = withdrawal._PreparedDocumentOpen(
            binding, 0, event, policy, closure, source_chunks, locator, (), (), payload
        )
        result.prepared_identity = id(result)
        withdrawal._seal_prepared_document_open(result)
        return result

    def continue_with(proposal: Any) -> Any:
        return withdrawal._continue_locked_document_open(
            cursor,
            event,
            prepared(),
            execution_policy=policy,
            supplied_direct_open=proposal,
            supplied_requirement_withdrawal=requirement,
            supplied_requirement_roots=roots,
            supplied_requirement_root_set_hash=root_hash,
        )

    assert continue_with(direct).direct_open == direct
    assert payload.inserted is not None
    stale = replace(
        direct,
        structural_payload=replace(
            payload, inserted=replace(payload.inserted, **{field: "stale-preview"})
        ),
    )
    with pytest.raises(EventConflictError, match="compare-only open proposal changed"):
        continue_with(stale)
