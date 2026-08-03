"""Byte-exact digest and normalization primitives for GroundLoop M5.

The encoding follows ``docs/m5_design_freeze.md`` M5-D1.  A digest consists
of ordered UTF-8 fields, each framed by an unsigned eight-byte big-endian
length.  Typed values expand into ordinary fields before framing.
"""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable
from enum import Enum

from groundloop.errors import ValidationError

TypedFields = tuple[str, ...]

_WHITESPACE_CODEPOINTS = frozenset(
    (
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
)


def normalize_text_v1(value: str) -> str:
    """Return the frozen cross-language whitespace normalization.

    Exactly the 29 code points in M5-D1 are whitespace.  Boundary runs are
    removed and internal runs become one ASCII space.  No Unicode
    normalization, case folding, or punctuation rewriting occurs.
    """

    output: list[str] = []
    pending_space = False
    for character in value:
        if ord(character) in _WHITESPACE_CODEPOINTS:
            if output:
                pending_space = True
            continue
        if pending_space:
            output.append(" ")
            pending_space = False
        output.append(character)
    return "".join(output)


def normalized_text_hash_v1(value: str) -> str:
    """SHA-256 of normalization-v1 UTF-8 bytes."""

    return hashlib.sha256(normalize_text_v1(value).encode("utf-8")).hexdigest()


def text_field(value: str) -> TypedFields:
    if not isinstance(value, str) or not value:
        raise ValidationError("TEXT values must be nonempty strings")
    return ("text", value)


def null_field() -> TypedFields:
    return ("null",)


def int_field(value: int) -> TypedFields:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError("INT values must be integers, not booleans")
    return ("int", str(value))


def bool_field(value: bool) -> TypedFields:
    if not isinstance(value, bool):
        raise ValidationError("BOOL values must be booleans")
    return ("bool", "1" if value else "0")


def enum_field(value: str | Enum) -> TypedFields:
    wire_value = value.value if isinstance(value, Enum) else value
    if not isinstance(wire_value, str) or not wire_value:
        raise ValidationError("ENUM values must have a nonempty string wire value")
    return ("enum", wire_value)


def hash_field(value: str) -> TypedFields:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValidationError("HASH values must be 64 lowercase hexadecimal digits")
    return ("sha256", value)


def f64_field(value: float) -> TypedFields:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValidationError("F64 values must be finite numbers")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValidationError("F64 values must be finite numbers")
    return ("f64", struct.pack(">d", numeric).hex())


def sequence_field(values: Iterable[TypedFields]) -> TypedFields:
    materialized = tuple(values)
    flattened: list[str] = ["sequence", *int_field(len(materialized))]
    for value in materialized:
        if not isinstance(value, tuple) or not all(
            isinstance(field, str) for field in value
        ):
            raise ValidationError("SEQ members must already be typed field tuples")
        flattened.extend(value)
    return tuple(flattened)


def option_field(value: TypedFields | None) -> TypedFields:
    return null_field() if value is None else value


def stable_m5_digest(domain_tag: str, *values: TypedFields) -> str:
    """Hash one domain tag and its already typed, flattened values."""

    if not isinstance(domain_tag, str) or not domain_tag:
        raise ValidationError("digest domain tags must be nonempty strings")
    fields: list[str] = [domain_tag]
    for value in values:
        if not isinstance(value, tuple) or not all(
            isinstance(field, str) for field in value
        ):
            raise ValidationError("digest values must be typed field tuples")
        fields.extend(value)

    digest = hashlib.sha256()
    for field in fields:
        encoded = field.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
        digest.update(encoded)
    return digest.hexdigest()


def whitespace_codepoints_v1() -> tuple[int, ...]:
    """Expose the frozen set for cross-language golden-vector tests."""

    return tuple(sorted(_WHITESPACE_CODEPOINTS))


__all__ = [
    "TypedFields",
    "bool_field",
    "enum_field",
    "f64_field",
    "hash_field",
    "int_field",
    "normalize_text_v1",
    "normalized_text_hash_v1",
    "null_field",
    "option_field",
    "sequence_field",
    "stable_m5_digest",
    "text_field",
    "whitespace_codepoints_v1",
]
