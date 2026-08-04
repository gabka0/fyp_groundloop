"""Load and verify the immutable WiCE source manifest before parsing rows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

from groundloop.errors import ValidationError
from groundloop.m5.evaluation.records import (
    LicenseRecord,
    SourceFileSpec,
    SupplementaryFileSpec,
    VerifiedSourceFile,
    WiceRowKind,
    WiceSourceManifest,
    WiceSplit,
)

MANIFEST_SCHEMA = "groundloop-wice-source-manifest-v1"
WICE_ADAPTER_VERSION = "wice-evidence-unit-v1"
WICE_OFFICIAL_COMMIT = "ddeb6c183665e2a20c5f03c5aa07f03888b9870f"


class _DuplicateKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _mapping(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValidationError(f"{context} must be a JSON object")
    return cast(dict[str, object], value)


def _sequence(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        raise ValidationError(f"{context} must be a JSON array")
    return cast(list[object], value)


def _string(mapping: dict[str, object], name: str, context: str) -> str:
    value = mapping.get(name)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{context}.{name} must be a nonempty string")
    return value


def _integer(mapping: dict[str, object], name: str, context: str) -> int:
    value = mapping.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{context}.{name} must be a nonnegative integer")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def load_source_manifest(path: Path) -> WiceSourceManifest:
    """Load a checked-in manifest and derive its canonical content hash."""

    raw = path.read_bytes()
    try:
        decoded = json.loads(raw, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateKey) as error:
        raise ValidationError(f"invalid source manifest: {error}") from error
    root = _mapping(decoded, "manifest")
    schema = _string(root, "schema", "manifest")
    adapter_version = _string(root, "adapter_version", "manifest")
    official_commit = _string(root, "official_commit", "manifest")
    if schema != MANIFEST_SCHEMA:
        raise ValidationError(f"unsupported source manifest schema {schema!r}")
    if adapter_version != WICE_ADAPTER_VERSION:
        raise ValidationError(f"unsupported adapter version {adapter_version!r}")
    if official_commit != WICE_OFFICIAL_COMMIT:
        raise ValidationError(
            f"WiCE source revision must be the frozen commit {WICE_OFFICIAL_COMMIT}"
        )

    source_files: list[SourceFileSpec] = []
    for index, item in enumerate(
        _sequence(root.get("source_files"), "manifest.source_files")
    ):
        record = _mapping(item, f"manifest.source_files[{index}]")
        try:
            split = WiceSplit(_string(record, "split", "source file"))
            kind = WiceRowKind(_string(record, "kind", "source file"))
        except ValueError as error:
            raise ValidationError(f"invalid WiCE source split/kind: {error}") from error
        source_files.append(
            SourceFileSpec(
                split=split,
                kind=kind,
                relative_path=_string(record, "relative_path", "source file"),
                rows=_integer(record, "rows", "source file"),
                byte_count=_integer(record, "bytes", "source file"),
                sha256=_string(record, "sha256", "source file"),
            )
        )

    supplementary_files: list[SupplementaryFileSpec] = []
    for index, item in enumerate(
        _sequence(root.get("supplementary_files"), "manifest.supplementary_files")
    ):
        record = _mapping(item, f"manifest.supplementary_files[{index}]")
        supplementary_files.append(
            SupplementaryFileSpec(
                role=_string(record, "role", "supplementary file"),
                relative_path=_string(record, "relative_path", "supplementary file"),
                byte_count=_integer(record, "bytes", "supplementary file"),
                sha256=_string(record, "sha256", "supplementary file"),
            )
        )

    license_json = _mapping(root.get("license"), "manifest.license")
    terms = tuple(
        _string({"value": value}, "value", "underlying text term")
        for value in _sequence(
            license_json.get("underlying_text_terms"),
            "manifest.license.underlying_text_terms",
        )
    )
    license_record = LicenseRecord(
        annotation_license=_string(
            license_json, "annotation_license", "manifest.license"
        ),
        underlying_text_terms=terms,
        source_url=_string(license_json, "source_url", "manifest.license"),
    )
    manifest_hash = hashlib.sha256(_canonical_json_bytes(root)).hexdigest()
    return WiceSourceManifest(
        schema=schema,
        dataset=_string(root, "dataset", "manifest"),
        repository_url=_string(root, "repository_url", "manifest"),
        official_commit=official_commit,
        adapter_version=adapter_version,
        source_files=tuple(source_files),
        supplementary_files=tuple(supplementary_files),
        license=license_record,
        manifest_hash=manifest_hash,
    )


def _safe_source_path(source_root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValidationError(f"unsafe source path {relative_path!r}")
    root = source_root.resolve()
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root):
        raise ValidationError(f"source path escapes root: {relative_path!r}")
    return candidate


def verify_source_files(
    source_root: Path, manifest: WiceSourceManifest
) -> tuple[VerifiedSourceFile, ...]:
    """Hash every consumed file before any JSON row is parsed."""

    verified: list[VerifiedSourceFile] = []
    for source_spec in manifest.source_files:
        path = _safe_source_path(source_root, source_spec.relative_path)
        try:
            raw = path.read_bytes()
        except OSError as error:
            raise ValidationError(
                f"cannot read pinned source file {path}: {error}"
            ) from error
        digest = hashlib.sha256(raw).hexdigest()
        row_count = len(raw.splitlines())
        if len(raw) != source_spec.byte_count:
            raise ValidationError(
                f"byte-count mismatch for {source_spec.relative_path}: "
                f"expected {source_spec.byte_count}, got {len(raw)}"
            )
        if digest != source_spec.sha256:
            raise ValidationError(
                f"SHA-256 mismatch for {source_spec.relative_path}: "
                f"expected {source_spec.sha256}, got {digest}"
            )
        if row_count != source_spec.rows:
            raise ValidationError(
                f"row-count mismatch for {source_spec.relative_path}: "
                f"expected {source_spec.rows}, got {row_count}"
            )
        verified.append(
            VerifiedSourceFile(
                relative_path=source_spec.relative_path,
                byte_count=len(raw),
                row_count=row_count,
                sha256=digest,
            )
        )

    for supplementary_spec in manifest.supplementary_files:
        path = _safe_source_path(source_root, supplementary_spec.relative_path)
        try:
            raw = path.read_bytes()
        except OSError as error:
            raise ValidationError(
                f"cannot read pinned source file {path}: {error}"
            ) from error
        digest = hashlib.sha256(raw).hexdigest()
        if len(raw) != supplementary_spec.byte_count:
            raise ValidationError(
                f"byte-count mismatch for {supplementary_spec.relative_path}: "
                f"expected {supplementary_spec.byte_count}, got {len(raw)}"
            )
        if digest != supplementary_spec.sha256:
            raise ValidationError(
                f"SHA-256 mismatch for {supplementary_spec.relative_path}: "
                f"expected {supplementary_spec.sha256}, got {digest}"
            )
        verified.append(
            VerifiedSourceFile(
                relative_path=supplementary_spec.relative_path,
                byte_count=len(raw),
                row_count=None,
                sha256=digest,
            )
        )
    return tuple(sorted(verified, key=lambda item: item.relative_path))


def source_path(source_root: Path, relative_path: str) -> Path:
    """Return one manifest-relative path after traversal validation."""

    return _safe_source_path(source_root, relative_path)


__all__ = [
    "MANIFEST_SCHEMA",
    "WICE_ADAPTER_VERSION",
    "WICE_OFFICIAL_COMMIT",
    "load_source_manifest",
    "source_path",
    "verify_source_files",
]
