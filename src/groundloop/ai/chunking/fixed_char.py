"""Frozen ``fixed-char-v1`` paragraph-aware chunking."""

from __future__ import annotations

import re
from dataclasses import dataclass

from groundloop.ai.contracts import ChunkDraft, stable_digest
from groundloop.domain import normalized_text_hash
from groundloop.errors import ValidationError

FIXED_CHAR_V1_ARTIFACT_ID = "fixed-char-v1-1200-no-overlap"
FIXED_CHAR_V1_MAX_CHARACTERS = 1_200

_BLANK_LINES = re.compile(r"\n[ \t\f\v]*\n(?:[ \t\f\v]*\n)*")


def normalize_line_endings(text: str) -> str:
    """Normalize CRLF and bare CR to LF without Unicode normalization."""
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _split_long_paragraph(paragraph: str, limit: int) -> tuple[str, ...]:
    pieces: list[str] = []
    remaining = paragraph
    while len(remaining) > limit:
        boundary = limit
        whitespace = max(
            (index for index in range(1, limit + 1) if remaining[index - 1].isspace()),
            default=0,
        )
        if whitespace:
            boundary = whitespace - 1
            if boundary == 0:
                boundary = limit
        piece = remaining[:boundary].rstrip()
        if not piece:
            piece = remaining[:limit]
            boundary = limit
        pieces.append(piece)
        remaining = remaining[boundary:].lstrip()
    if remaining:
        pieces.append(remaining)
    return tuple(pieces)


@dataclass(frozen=True, slots=True)
class FixedCharChunker:
    """Paragraph packer implementing the frozen M3-2 contract exactly."""

    artifact_id: str = FIXED_CHAR_V1_ARTIFACT_ID
    max_characters: int = FIXED_CHAR_V1_MAX_CHARACTERS

    def __post_init__(self) -> None:
        if not self.artifact_id.strip():
            raise ValidationError("chunker artifact_id must be non-empty")
        if self.max_characters <= 0:
            raise ValidationError("max_characters must be positive")

    def chunk(self, document_version_id: str, text: str) -> tuple[ChunkDraft, ...]:
        if not document_version_id.strip():
            raise ValidationError("document_version_id must be non-empty")
        normalized = normalize_line_endings(text).strip()
        if not normalized:
            return ()

        paragraphs = tuple(
            paragraph.strip()
            for paragraph in _BLANK_LINES.split(normalized)
            if paragraph.strip()
        )
        chunk_texts: list[str] = []
        packed: list[str] = []
        packed_length = 0

        def flush() -> None:
            nonlocal packed_length
            if packed:
                chunk_texts.append("\n\n".join(packed))
                packed.clear()
                packed_length = 0

        for paragraph in paragraphs:
            if len(paragraph) > self.max_characters:
                flush()
                chunk_texts.extend(
                    _split_long_paragraph(paragraph, self.max_characters)
                )
                continue
            added_length = len(paragraph) + (2 if packed else 0)
            if packed and packed_length + added_length > self.max_characters:
                flush()
                added_length = len(paragraph)
            packed.append(paragraph)
            packed_length += added_length
        flush()

        return tuple(
            self._draft(document_version_id, index, chunk_text)
            for index, chunk_text in enumerate(chunk_texts)
        )

    def _draft(
        self, document_version_id: str, chunk_index: int, text: str
    ) -> ChunkDraft:
        text_hash = normalized_text_hash(text)
        chunk_version_id = stable_digest(
            "chunk-version-v1",
            document_version_id,
            str(chunk_index),
            self.artifact_id,
            text_hash,
        )
        return ChunkDraft(
            chunk_version_id=chunk_version_id,
            document_version_id=document_version_id,
            chunk_index=chunk_index,
            text=text,
            text_hash=text_hash,
            chunker_artifact_id=self.artifact_id,
        )
