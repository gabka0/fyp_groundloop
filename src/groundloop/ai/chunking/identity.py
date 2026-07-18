"""Stable directory-ingestion identities for the static M3 corpus."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from groundloop.ai.chunking.fixed_char import FixedCharChunker
from groundloop.ai.contracts import ChunkDraft, stable_digest
from groundloop.errors import ValidationError


def normalize_relative_path(relative_path: str | Path) -> str:
    """Return an NFC-normalized, platform-independent safe relative path."""
    raw = str(relative_path).replace("\\", "/")
    path = PurePosixPath(raw)
    if path.is_absolute() or not path.parts:
        raise ValidationError("document path must be a non-empty relative path")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValidationError(
            "document path cannot contain empty, dot, or parent parts"
        )
    return unicodedata.normalize("NFC", path.as_posix())


@dataclass(frozen=True, slots=True)
class DocumentIdentity:
    corpus_namespace: str
    relative_path: str
    raw_content_hash: str
    document_id: str
    document_version_id: str


@dataclass(frozen=True, slots=True)
class PreparedDocument:
    identity: DocumentIdentity
    text: str
    chunks: tuple[ChunkDraft, ...]


def prepare_document(
    *,
    corpus_namespace: str,
    relative_path: str | Path,
    raw_content: bytes,
    chunker: FixedCharChunker,
) -> PreparedDocument:
    """Decode one UTF-8 document and derive all frozen M3 identities."""
    namespace = corpus_namespace.strip()
    if not namespace:
        raise ValidationError("corpus_namespace must be non-empty")
    normalized_path = normalize_relative_path(relative_path)
    try:
        text = raw_content.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ValidationError(
            f"document {normalized_path} is not valid UTF-8"
        ) from error
    raw_content_hash = hashlib.sha256(raw_content).hexdigest()
    document_id = stable_digest("document-v1", namespace, normalized_path)
    document_version_id = stable_digest(
        "document-version-v1",
        document_id,
        raw_content_hash,
        chunker.artifact_id,
    )
    identity = DocumentIdentity(
        corpus_namespace=namespace,
        relative_path=normalized_path,
        raw_content_hash=raw_content_hash,
        document_id=document_id,
        document_version_id=document_version_id,
    )
    return PreparedDocument(
        identity=identity,
        text=text,
        chunks=chunker.chunk(document_version_id, text),
    )


def prepare_directory(
    root: Path,
    *,
    corpus_namespace: str,
    chunker: FixedCharChunker,
) -> tuple[PreparedDocument, ...]:
    """Read every regular file below ``root`` in normalized path order."""
    if not root.is_dir():
        raise ValidationError(f"corpus root is not a directory: {root}")
    documents = [
        prepare_document(
            corpus_namespace=corpus_namespace,
            relative_path=path.relative_to(root),
            raw_content=path.read_bytes(),
            chunker=chunker,
        )
        for path in root.rglob("*")
        if path.is_file()
    ]
    return tuple(sorted(documents, key=lambda item: item.identity.relative_path))
