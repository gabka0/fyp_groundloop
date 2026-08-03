from __future__ import annotations

import hashlib
import math

import pytest

from groundloop.domain import normalized_text_hash
from groundloop.errors import ValidationError
from groundloop.m5.digests import (
    bool_field,
    enum_field,
    f64_field,
    hash_field,
    int_field,
    normalize_text_v1,
    normalized_text_hash_v1,
    null_field,
    option_field,
    sequence_field,
    stable_m5_digest,
    text_field,
    whitespace_codepoints_v1,
)


def _manual_digest(*fields: str) -> str:
    digest = hashlib.sha256()
    for field in fields:
        encoded = field.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def test_exact_frozen_whitespace_predicate_and_unicode_nonmembers() -> None:
    expected = (
        *range(0x0009, 0x000E),
        *range(0x001C, 0x0021),
        0x0085,
        0x00A0,
        0x1680,
        *range(0x2000, 0x200B),
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
    )
    assert len(expected) == 29
    assert whitespace_codepoints_v1() == expected
    for codepoint in expected:
        character = chr(codepoint)
        assert normalize_text_v1(f"{character}a{character}{character}b{character}") == (
            "a b"
        )

    # These Unicode characters are deliberately content, not whitespace.
    assert normalize_text_v1("\ufeffa\u200bb\u180e") == "\ufeffa\u200bb\u180e"
    assert normalize_text_v1("  Καλημέρα\t世界  ") == "Καλημέρα 世界"


def test_m5_normalization_is_frozen_independently_of_legacy_regex() -> None:
    text = "\u001calpha\u205f\u205fbeta\u3000"
    expected_hash = hashlib.sha256(b"alpha beta").hexdigest()
    assert normalized_text_hash_v1(text) == expected_hash
    # This documents equality for the frozen members without coupling the new
    # implementation to Python's version-dependent regex category forever.
    assert normalized_text_hash(text) == expected_hash
    assert normalize_text_v1("é") != normalize_text_v1("e\u0301")
    assert normalized_text_hash_v1("é") != normalized_text_hash_v1("e\u0301")


def test_length_prefixes_remove_boundary_ambiguity() -> None:
    left = stable_m5_digest("d", text_field("ab"), text_field("c"))
    right = stable_m5_digest("d", text_field("a"), text_field("bc"))
    assert left != right
    assert left == _manual_digest("d", "text", "ab", "text", "c")
    assert right == _manual_digest("d", "text", "a", "text", "bc")


def test_typed_null_sequence_boolean_enum_integer_and_hash_are_byte_total() -> None:
    zero_hash = "0" * 64
    digest = stable_m5_digest(
        "typed-vector-v1",
        null_field(),
        option_field(text_field("x")),
        sequence_field((text_field("a"), text_field("b"))),
        bool_field(False),
        bool_field(True),
        enum_field("support"),
        int_field(-7),
        hash_field(zero_hash),
    )
    assert digest == _manual_digest(
        "typed-vector-v1",
        "null",
        "text",
        "x",
        "sequence",
        "int",
        "2",
        "text",
        "a",
        "text",
        "b",
        "bool",
        "0",
        "bool",
        "1",
        "enum",
        "support",
        "int",
        "-7",
        "sha256",
        zero_hash,
    )
    assert sequence_field((text_field("a"), text_field("b"))) != sequence_field(
        (text_field("b"), text_field("a"))
    )
    assert sequence_field(()) != null_field()


def test_f64_is_exact_ieee_big_endian_and_rejects_nonfinite() -> None:
    assert f64_field(0.0) == ("f64", "0000000000000000")
    assert f64_field(-0.0) == ("f64", "8000000000000000")
    assert stable_m5_digest("f", f64_field(0.0)) != stable_m5_digest(
        "f", f64_field(-0.0)
    )
    for invalid in (math.nan, math.inf, -math.inf):
        with pytest.raises(ValidationError):
            f64_field(invalid)


@pytest.mark.parametrize(
    "operation",
    (
        lambda: text_field(""),
        lambda: hash_field("A" * 64),
        lambda: hash_field("0" * 63),
        lambda: int_field(True),
        lambda: bool_field(1),
        lambda: enum_field(""),
        lambda: stable_m5_digest(""),
    ),
)
def test_invalid_typed_values_are_rejected(operation: object) -> None:
    with pytest.raises(ValidationError):
        operation()  # type: ignore[operator]
