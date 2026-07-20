"""Small fail-closed JSON and digest helpers for M4.13 artifacts."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError

_HEX = frozenset("0123456789abcdef")


def require_sha256(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")
    return value


def require_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be non-empty text")
    return value


def require_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    return value


def require_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValidationError(f"{name} must be finite")
    return result


def require_object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be a JSON object")
    return cast(Mapping[str, object], value)


def require_array(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise ValidationError(f"{name} must be a JSON array")
    return cast(Sequence[object], value)


def canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValidationError("artifact is not canonical-JSON serializable") from error


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        raise ValidationError(f"cannot read artifact: {path}") from error
    return digest.hexdigest()


def load_json(path: Path, name: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError(f"cannot read {name}: {path}") from error
    return require_object(value, name)


def load_jsonl(path: Path, name: str) -> tuple[Mapping[str, object], ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ValidationError(f"cannot read {name}: {path}") from error
    rows: list[Mapping[str, object]] = []
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValidationError(
                f"malformed {name} row at {path}:{line_number}"
            ) from error
        rows.append(require_object(value, f"{name} row {line_number}"))
    if not rows:
        raise ValidationError(f"{name} must contain at least one row")
    return tuple(rows)


def write_canonical_json(path: Path, value: object) -> str:
    payload = canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def write_canonical_jsonl(path: Path, rows: Sequence[object]) -> str:
    payload = b"".join(canonical_bytes(row) + b"\n" for row in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()
