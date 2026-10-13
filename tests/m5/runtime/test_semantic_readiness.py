"""Independent framing/accounting checks for the private D32 producer."""

from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
from typing import Any

import pytest

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime import postgres_readiness as readiness
from groundloop.m5.runtime.contracts import (
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
)
from groundloop.m5.runtime.postgres_readiness import (
    _check_epoch_identity,
    _readiness_artifact,
    _require_legacy_lookup_context,
    _validate_replay,
)


def _frame(*fields: str) -> bytes:
    return b"".join(
        len(encoded).to_bytes(8, "big") + encoded
        for encoded in (field.encode("utf-8") for field in fields)
    )


def _arguments() -> dict[str, Any]:
    return {
        "epoch_id": 31,
        "structural_event_id": "更新\N{SNOWMAN}",
        "event_payload_hash": "a" * 64,
        "requirement_root_set_hash": "b" * 64,
        "target": "semantic_pending",
        "expected_revision": 1,
    }


@pytest.mark.parametrize("schema", ("public", "captured_installation"))
def test_context_predicate_deduplicates_captured_public_without_claiming_authority(
    schema: str,
) -> None:
    """Only test the pure context predicate, not a valid public 020 catalog."""

    class ContextCursor:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, query: str, params: object = None) -> Any:
            self.calls.append(query)
            return self

        def fetchone(self) -> Any:
            if len(self.calls) == 1:
                return (list(dict.fromkeys(("pg_catalog", schema, "public"))),)
            return (False,)

    cursor = ContextCursor()
    _require_legacy_lookup_context(cursor, schema)  # type: ignore[arg-type]
    assert len(cursor.calls) == 2
    assert all(not query.startswith("SET") for query in cursor.calls)


@pytest.mark.parametrize(
    ("target", "predecessor", "revision"),
    (
        ("semantic_pending", "structural_committed", 1),
        ("semantic_complete", "semantic_pending", 2),
        ("semantic_complete", "semantic_pending", 812),
    ),
)
def test_independent_source_key_and_exact_byte_only_work(
    target: str, predecessor: str, revision: int
) -> None:
    args = _arguments() | {"target": target, "expected_revision": revision}
    source = _frame(
        "m5-semantic-readiness-transition-v1",
        "int",
        "31",
        "text",
        args["structural_event_id"],
        "sha256",
        "a" * 64,
        "sha256",
        "b" * 64,
        "enum",
        predecessor,
        "enum",
        target,
        "int",
        str(revision),
        "int",
        str(revision + 1),
    )
    key = _frame(
        "m5-runtime-work-contribution-key-v1",
        "int",
        "31",
        "enum",
        "semantic_readiness",
        "text",
        target,
    )
    artifact = _readiness_artifact(**args)
    assert artifact.source_identity_hash == hashlib.sha256(source).hexdigest()
    assert artifact.anchor.contribution_key_digest == hashlib.sha256(key).hexdigest()
    assert artifact.anchor.anchor_revision == revision + 1
    assert artifact.anchor.terminal_transition is False
    counters = dict(
        zip(artifact.work.counter_names(), artifact.work.counter_values(), strict=True)
    )
    assert counters.pop("bytes_hashed") == len(source) + len(key)
    assert counters.pop("bytes_serialized") == len(source) + len(key)
    assert set(counters.values()) == {0}


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("epoch_id", True),
        ("epoch_id", 0),
        ("epoch_id", "31"),
        ("structural_event_id", ""),
        ("structural_event_id", None),
        ("event_payload_hash", "A" * 64),
        ("event_payload_hash", "a" * 63),
        ("requirement_root_set_hash", None),
        ("target", "sealed"),
        ("target", None),
        ("expected_revision", True),
        ("expected_revision", 0),
        ("expected_revision", 2),
        ("expected_revision", "1"),
    ),
)
def test_invalid_producer_coordinates_fail_closed(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        _readiness_artifact(**(_arguments() | {field: value}))


def test_complete_does_not_skip_structural_start() -> None:
    with pytest.raises(ValidationError, match="revisions"):
        _readiness_artifact(**(_arguments() | {"target": "semantic_complete"}))


def test_source_changes_with_each_immutable_coordinate() -> None:
    args = _arguments() | {"target": "semantic_complete", "expected_revision": 7}
    original = _readiness_artifact(**args)
    for field, value in (
        ("epoch_id", 32),
        ("structural_event_id", "other"),
        ("event_payload_hash", "c" * 64),
        ("requirement_root_set_hash", "d" * 64),
        ("expected_revision", 8),
    ):
        assert _readiness_artifact(**(args | {field: value})).source_identity_hash != (
            original.source_identity_hash
        )


def test_producer_source_encode_occurs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    original = digests.semantic_readiness_transition_preimage
    calls: list[dict[str, Any]] = []

    def capture(**kwargs: Any) -> bytes:
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(digests, "semantic_readiness_transition_preimage", capture)
    _readiness_artifact(**_arguments())
    assert len(calls) == 1


@pytest.mark.parametrize(
    "state,statuses,sealed,terminal",
    (
        ("structural_committed", ("committed", "pending", "pending"), False, False),
        ("semantic_pending", ("committed", "pending", "pending"), False, False),
        ("semantic_complete", ("committed", "complete", "complete"), False, False),
        ("sealed", ("committed", "sealed", "complete"), True, True),
        ("failed", ("failed", "failed", "failed"), False, True),
    ),
)
def test_real_terminal_and_nonterminal_header_shapes(
    state: str, statuses: tuple[str, ...], sealed: bool, terminal: bool
) -> None:
    base = dict(
        epoch_id=31,
        event_id="event",
        revision=3,
        structural_status=statuses[0],
        semantic_status=statuses[1],
        evaluation_state=statuses[2],
        sealed_at=object() if sealed else None,
        payload_hash="a" * 64,
    )
    runtime = dict(
        epoch_id=31,
        structural_event_id="event",
        revision=3,
        runtime_state=state,
        terminal_at=object() if terminal else None,
        requirement_root_set_hash="b" * 64,
    )
    _check_epoch_identity(base, runtime, structural_event_id="event")
    with pytest.raises(ValidationError):
        _check_epoch_identity(
            base | {"sealed_at": None if sealed else object()},
            runtime,
            structural_event_id="event",
        )
    with pytest.raises(EventConflictError):
        _check_epoch_identity(
            base, runtime | {"epoch_id": 32}, structural_event_id="event"
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("epoch_id", 32),
        ("contribution_kind", "seal"),
        ("source_id", "semantic_complete"),
        ("applied_revision", 3),
        ("source_identity_hash", "f" * 64),
        ("contribution_key_digest", "c" * 64),
    ),
)
def test_replay_checks_every_original_artifact_coordinate(
    field: str, value: Any
) -> None:
    artifact = _readiness_artifact(**_arguments())
    row = dict(
        epoch_id=31,
        contribution_kind="semantic_readiness",
        source_id=artifact.anchor.source_id,
        applied_revision=2,
        source_identity_hash=artifact.source_identity_hash,
        contribution_key_digest=artifact.anchor.contribution_key_digest,
        work_digest=artifact.work.work_digest,
        **dict(
            zip(
                artifact.work.counter_names(),
                artifact.work.counter_values(),
                strict=True,
            )
        ),
    )
    receipt = _validate_replay(row, {"epoch_id": 31, "revision": 8}, artifact)
    assert receipt.replay and receipt.anchor == artifact.anchor
    wrong_work = M5RuntimeWork(bytes_hashed=artifact.work.bytes_hashed + 1)
    wrong = row | dict(
        zip(wrong_work.counter_names(), wrong_work.counter_values(), strict=True)
    )
    with pytest.raises(EventConflictError):
        _validate_replay(
            wrong | {"work_digest": wrong_work.work_digest},
            {"epoch_id": 31, "revision": 8},
            artifact,
        )
    with pytest.raises(ValidationError):
        _validate_replay(
            row | {"work_digest": "f" * 64}, {"epoch_id": 31, "revision": 8}, artifact
        )
    with pytest.raises(EventConflictError):
        _validate_replay(
            row | {field: value}, {"epoch_id": 31, "revision": 8}, artifact
        )


def test_source_and_key_producer_boundaries_encode_and_hash_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encode = digests.stable_m5_preimage
    sha = hashlib.sha256
    encoded: list[tuple[str, bytes]] = []
    hashed: list[bytes] = []

    def capture_encode(domain: str, *fields: tuple[str, ...]) -> bytes:
        value = encode(domain, *fields)
        encoded.append((domain, value))
        return value

    def capture_sha(data: bytes = b"", **kwargs: Any) -> Any:
        hashed.append(data)
        return sha(data, **kwargs)

    monkeypatch.setattr(digests, "stable_m5_preimage", capture_encode)
    monkeypatch.setattr(hashlib, "sha256", capture_sha)
    _readiness_artifact(**_arguments())
    assert [domain for domain, _ in encoded] == [
        "m5-semantic-readiness-transition-v1",
        "m5-runtime-work-contribution-key-v1",
    ]
    assert all(hashed.count(value) == 1 for _, value in encoded)


def test_r33_preserves_every_unmodified_original_body_and_producer_recipe() -> None:
    tree = ast.parse(inspect.getsource(readiness))
    changed = {
        "_check_declaration",
        "_read_header_pair",
        "_advance_semantic_readiness_atomically",
    }
    tree.body = [
        node
        for node in tree.body
        if not isinstance(node, (ast.Import, ast.ImportFrom))
        and not (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and (node.name in changed or node.name.startswith("_r33_"))
        )
        and not (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id.startswith("_R33_")
                for target in node.targets
            )
        )
    ]
    assert hashlib.sha256(ast.dump(tree).encode()).hexdigest() == (
        "01458a4c1e95efa6702259543aa7b9c10921dede08715e0a20f4554296949596"
    )


@pytest.mark.parametrize("present,bad", ((False, False), (True, False), (True, True)))
def test_r33_exact_authority_selection_never_falls_back_from_bad_021(
    monkeypatch: pytest.MonkeyPatch, present: bool, bad: bool
) -> None:
    calls = []
    monkeypatch.setattr(
        readiness, "_m5_readiness_bundle_row", lambda *a, **k: () if present else None
    )

    def check_021(*args: Any, **kwargs: Any) -> None:
        calls.append(("021", kwargs))
        if bad:
            raise ValidationError("bad present021")

    def check_020(*args: Any, **kwargs: Any) -> None:
        calls.append(("020", kwargs))

    monkeypatch.setattr(
        readiness, "require_m5_document_update_completion_bundle", check_021
    )
    monkeypatch.setattr(readiness, "require_m5_semantic_readiness_bundle", check_020)
    if bad:
        with pytest.raises(ValidationError, match="bad present021"):
            readiness._r33_require_authority(None, "captured")  # type: ignore[arg-type]
    else:
        assert readiness._r33_require_authority(None, "captured") is present  # type: ignore[arg-type]
    assert calls == [("021" if present else "020", {"schema_name": "captured"})]


def _r33_counter_fixture(sealed: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    """Independent frozen M4 JSON and framing recipes; controlled unit, not J33."""

    def old_json(value: Any) -> str:
        return hashlib.sha256(
            json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()

    base = {"epoch_id": 31}
    declaration = old_json(
        {
            "epoch_id": 31,
            "revision": 1,
            "confirmed_as_of_epoch": 7,
            "open_discovery_scope_count": 0,
        }
    )
    seal_id = hashlib.sha256(_frame("m4-evaluation-seal-v1", "31")).hexdigest()
    seal_hash = old_json(
        {
            "kind": "seal",
            "expected_revision": 1,
            "scope_delta": 0,
            "claim_job_deltas": [],
        }
    )
    runtime = {
        "runtime_state": "sealed" if sealed else "semantic_complete",
        "open_work_count": 0,
        "open_scope_count": 0,
        "blocking_failure_count": 0,
        "_r33_document_point": {
            "transition_count": int(sealed),
            "direct_history": False,
            "owner_counter_bad": False,
            "answer_counter_bad": False,
            "counter": dict(
                zip(
                    readiness._R33_COUNTER_FIELDS,
                    (
                        31,
                        declaration,
                        "sealed" if sealed else "active",
                        "complete",
                        31 if sealed else 7,
                        0,
                        2 if sealed else 1,
                    ),
                    strict=True,
                )
            ),
            "seal": dict(
                zip(
                    readiness._R33_SEAL_FIELDS,
                    (seal_id, seal_hash, "seal", 1, 2, 0) if sealed else (None,) * 6,
                    strict=True,
                )
            ),
        },
    }
    return base, runtime


@pytest.mark.parametrize("sealed", (False, True))
def test_r33_controlled_counter_vectors_use_distinct_m4_and_m5_clocks(
    sealed: bool,
) -> None:
    base, runtime = _r33_counter_fixture(sealed)
    # The M5 coordination revision may be much later than the native M4 1->2.
    base["revision"] = 812
    runtime["revision"] = 812
    readiness._r33_counter_point(base, runtime, 7)


@pytest.mark.parametrize("sealed", (False, True))
@pytest.mark.parametrize(
    "field,value",
    (
        ("transition_count", True),
        ("transition_count", 3),
        ("direct_history", True),
        ("owner_counter_bad", True),
        ("answer_counter_bad", True),
        ("counter.epoch_id", 30),
        ("counter.revision", 812),
        ("counter.revision", True),
        ("counter.confirmed_as_of_epoch", None),
        ("counter.declaration_hash", "f" * 64),
        ("counter.lifecycle_state", "failed"),
        ("counter.default_evaluation_state", "pending"),
        ("counter.open_discovery_scope_count", 1),
        ("counter", None),
        ("seal", {}),
    ),
)
def test_r33_controlled_malformed_counter_points_reject(
    sealed: bool, field: str, value: Any
) -> None:
    base, runtime = _r33_counter_fixture(sealed)
    point = runtime["_r33_document_point"]
    parent, _, child = field.partition(".")
    if child:
        point[parent][child] = value
    else:
        point[parent] = value
    with pytest.raises((EventConflictError, InvalidEventError, ValidationError)):
        readiness._r33_counter_point(base, runtime, 7)


@pytest.mark.parametrize("field", readiness._R33_SEAL_FIELDS)
def test_r33_controlled_noncanonical_seal_and_preseal_phantom_reject(
    field: str,
) -> None:
    base, sealed = _r33_counter_fixture(True)
    sealed["_r33_document_point"]["seal"][field] = None
    with pytest.raises(EventConflictError):
        readiness._r33_counter_point(base, sealed, 7)
    base, active = _r33_counter_fixture()
    active["_r33_document_point"]["seal"][field] = "phantom"
    with pytest.raises(EventConflictError):
        readiness._r33_counter_point(base, active, 7)


@pytest.mark.parametrize("shape", (None, {}, {"counter": {}}, {"unknown": True}))
def test_r33_controlled_counter_snapshot_shape_rejects(shape: Any) -> None:
    base, runtime = _r33_counter_fixture()
    runtime["_r33_document_point"] = copy.deepcopy(shape)
    with pytest.raises(EventConflictError):
        readiness._r33_counter_point(base, runtime, 7)


def test_r33_terminal_reader_runs_first_and_propagates_native_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    monkeypatch.setattr(
        readiness.document_source,
        "_validate_document_manifest",
        lambda value: ((), (), ()),
    )

    def terminal(*args: Any) -> None:
        calls.append("terminal")
        raise EventConflictError("native terminal reader")

    monkeypatch.setattr(readiness, "_r33_terminal_result", terminal)
    monkeypatch.setattr(
        readiness, "_r33_counter_point", lambda *args: calls.append("counter")
    )
    with pytest.raises(EventConflictError, match="native terminal reader"):
        readiness._r33_document_declaration(
            None,
            "captured",
            {"epoch_id": 31},
            {"runtime_state": "sealed"},  # type: ignore[arg-type]
            {"previous_published_epoch_id": 7, "manifest": {}},
            replay=True,
        )
    assert calls == ["terminal"]


@pytest.mark.parametrize(
    "fault",
    ("none", "key", "work_digest", "foreign_work", "missing", "extra", "revision"),
)
def test_r33_original_open_proof_validates_key_digest_and_kind_owned_work(
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    anchor = M5TransitionTimingAnchor.build(
        epoch_id=31,
        contribution_kind=M5RuntimeWorkContributionKind.STRUCTURAL_OPEN,
        source_id="更新☃",
        anchor_revision=1,
        terminal_transition=False,
    )
    work = M5RuntimeWork(
        deactivated_chunk_count=2, bytes_hashed=13, bytes_serialized=13
    )
    row = {
        "epoch_id": 31,
        "source_id": anchor.source_id,
        "applied_revision": 1,
        "source_identity_hash": "a" * 64,
        "contribution_key_digest": anchor.contribution_key_digest,
        "work_digest": work.work_digest,
        **dict(zip(work.counter_names(), work.counter_values(), strict=True)),
    }
    if fault == "key":
        row["contribution_key_digest"] = "f" * 64
    elif fault == "work_digest":
        row["work_digest"] = "f" * 64
    elif fault == "revision":
        row["applied_revision"] = 2
    elif fault == "foreign_work":
        bad = M5RuntimeWork(public_delta_count=1)
        row.update(dict(zip(bad.counter_names(), bad.counter_values(), strict=True)))
        row["work_digest"] = bad.work_digest
    rows = [] if fault == "missing" else [row, row] if fault == "extra" else [row]
    monkeypatch.setattr(readiness, "_r33_records", lambda *args: rows)
    args = (
        None,
        "captured",
        {"epoch_id": 31, "payload_hash": "a" * 64},
        {"structural_event_id": anchor.source_id},
    )
    if fault == "none":
        readiness._r33_original_contribution(*args)  # type: ignore[arg-type]
    else:
        with pytest.raises((EventConflictError, ValidationError)):
            readiness._r33_original_contribution(*args)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "field,value",
    (
        ("transition_id", "f" * 64),
        ("payload_hash", "f" * 64),
        ("transition_kind", "delta"),
        ("from_revision", 2),
        ("to_revision", 3),
        ("override_rows_written", 1),
    ),
)
def test_r33_controlled_well_typed_wrong_canonical_seal_rejects(
    field: str, value: Any
) -> None:
    base, runtime = _r33_counter_fixture(True)
    runtime["_r33_document_point"]["seal"][field] = value
    with pytest.raises(EventConflictError):
        readiness._r33_counter_point(base, runtime, 7)


@pytest.mark.parametrize("sealed", (False, True))
def test_r33_controlled_wrong_but_valid_counter_confirmation_rejects(
    sealed: bool,
) -> None:
    base, runtime = _r33_counter_fixture(sealed)
    runtime["_r33_document_point"]["counter"]["confirmed_as_of_epoch"] = (
        7 if sealed else 31
    )
    with pytest.raises(EventConflictError):
        readiness._r33_counter_point(base, runtime, 7)
