"""D35 pure administrative vectors/projection invariants, not C2 evidence."""

from __future__ import annotations

import hashlib
import re
from typing import Any, cast

import pytest

from groundloop.m5.runtime.postgres_matching import _STAGE_JOURNAL_RELATIONS
from groundloop.postgres import migrations as m


@pytest.mark.parametrize("source", [b"", b"\xff\x00", b"SELECT 1;", "α".encode()])
def test_identity_describes_bytes_without_content_authority(source: bytes) -> None:
    digest = hashlib.sha256(source).hexdigest()
    fields = (
        "m5-transition-context-access-schema-bundle-v1",
        "text",
        "migrations/022_m5_transition_context_access.sql",
        "sha256",
        digest,
        "sha256",
        "893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5",
    )
    framed = b"".join(
        len(value.encode()).to_bytes(8, "big") + value.encode() for value in fields
    )
    identity = m.m5_transition_context_access_bundle_identity(migration_bytes=source)
    assert identity.bundle_sha256 == hashlib.sha256(framed).hexdigest()
    assert identity.migration_sha256 == digest
    assert identity.prerequisite_sha256 == fields[-1]
    assert identity.oracle_sha256 == hashlib.sha256(b"").hexdigest()


def test_exact_sql_surface_and_native_relation_inventory() -> None:
    source = m.M5_TRANSITION_CONTEXT_ACCESS_MIGRATION_PATH.read_text()
    names = m._m5_transition_access_names
    assert re.findall(r"^CREATE FUNCTION (\w+)", source, re.M) == list(names)
    assert source.count("-- M5-D35-STATEMENT: ") == 6
    assert source.count("REVOKE ALL ON FUNCTION") == 2
    assert source.count("GRANT EXECUTE ON FUNCTION") == 2
    assert source.count("CALLED ON NULL INPUT SECURITY DEFINER") == 2
    assert source.count("PARALLEL UNSAFE NOT LEAKPROOF") == 2
    assert "pg_current_xact_id()" not in source
    assert "pg_current_xact_id_if_assigned()" in source
    assert "SET CONSTRAINTS" not in source
    assert "FROM pg_temp.groundloop_m5_matching_expected_changes" not in source
    relations = set(re.findall(r"'(groundloop_m5_\w+)'::pg_catalog.text", source))
    assert relations == set(_STAGE_JOURNAL_RELATIONS)
    for operation in ("INSERT INTO", "UPDATE ", "DELETE FROM", "LOCK TABLE"):
        assert operation not in source


class _RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any]] = []
        self.cursor = object()

    def execute(self, query: Any, params: Any = None) -> Any:
        self.calls.append((query, params))
        return self.cursor


def test_projection_intercepts_only_two_exact_queries_and_forwards_real_cursor() -> (
    None
):
    executor = _RecordingExecutor()
    projection = m._m5_transition_access_projection(
        cast(Any, executor), "installation", (111, 222)
    )
    queries = (
        m._m5_transition_access_identity_query,
        m._m5_transition_access_sources_query,
    )
    for query in queries:
        assert projection.execute(query, ("installation",)) is executor.cursor
        actual, params = executor.calls[-1]
        assert params == ("installation", [111, 222])
        assert actual.count("p.oid OPERATOR(pg_catalog.<>) ALL") == 1
        if query == queries[1]:
            assert actual.endswith(' ORDER BY p.proname COLLATE "C"')
    assert projection.intercepted == list(queries)
    for query, params in (
        ("SELECT p.proname,p.prosrc", ("installation",)),
        (queries[0] + " ", ("installation",)),
        ("SELECT real_dependency FROM pg_catalog.pg_depend", None),
    ):
        assert projection.execute(query, params) is executor.cursor
        assert executor.calls[-1] == (query, params)
    assert projection.intercepted == list(queries)


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("params", [None, (), ("foreign",), ("installation", 123)])
def test_projection_rejects_schema_parameter_drift(index: int, params: Any) -> None:
    executor = _RecordingExecutor()
    projection = m._m5_transition_access_projection(
        cast(Any, executor), "installation", (111, 222)
    )
    query = (
        m._m5_transition_access_identity_query,
        m._m5_transition_access_sources_query,
    )[index]
    with pytest.raises(m.M5TransitionContextAccessBundleError, match="schema"):
        projection.execute(query, params)
    assert not executor.calls


@pytest.mark.parametrize("source", [b"\xff", b"SELECT 1;", b"-- comment\n"])
def test_statement_decoder_rejects_malformed_surface(source: bytes) -> None:
    with pytest.raises(m.M5TransitionContextAccessBundleError):
        m._m5_transition_access_statements(source)


def test_administrative_frame_is_typed_not_repr_or_json() -> None:
    frame = m._m5_transition_access_frame
    assert len({frame(None), frame(False), frame(0), frame("0"), frame(b"0")}) == 5
    assert frame([1, "α", None]) == frame((1, "α", None))
    with pytest.raises(m.M5TransitionContextAccessBundleError):
        frame({"not": "a supported catalog field"})


@pytest.mark.parametrize("value", [bytearray(b"SQL"), memoryview(b"SQL"), "SQL"])
def test_identity_rejects_mutable_or_nonbytes_envelope(value: Any) -> None:
    with pytest.raises(m.M5TransitionContextAccessBundleError, match="immutable bytes"):
        m.m5_transition_context_access_bundle_identity(migration_bytes=value)


def test_reviewed_h35_literal_identity() -> None:
    identity = m.m5_transition_context_access_bundle_identity()
    assert m._m5_transition_access_ledger(identity) == (
        "m5-transition-context-access-schema-bundle-v1",
        "44990a145629abbf4212a58b8d235631799cbf8dd56b1dc4ed012bdc2e3ab1d7",
        "f6de8e14635addb4ac61c266fd10f0a5bbc1d163858bbffb77e39daf3edf484e",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "893baf9d50cbb84c27b356cbc329f97bc714ca8c280778ece920b5a4dc728eb5",
    )
