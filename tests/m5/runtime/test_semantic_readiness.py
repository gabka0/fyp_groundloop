"""Independent framing/accounting checks for the private D32 producer."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from groundloop.errors import EventConflictError, ValidationError
from groundloop.m5.runtime import digests
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
