from __future__ import annotations

import pytest

from groundloop.ai.chunking import FixedCharChunker, prepare_document
from groundloop.errors import ValidationError


@pytest.mark.parametrize("path", ["/absolute.txt", "../escape.txt", "a/../b.txt"])
def test_unsafe_relative_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValidationError):
        prepare_document(
            corpus_namespace="ns",
            relative_path=path,
            raw_content=b"text",
            chunker=FixedCharChunker(),
        )


def test_non_utf8_document_is_rejected() -> None:
    with pytest.raises(ValidationError, match="valid UTF-8"):
        prepare_document(
            corpus_namespace="ns",
            relative_path="bad.txt",
            raw_content=b"\xff",
            chunker=FixedCharChunker(),
        )
