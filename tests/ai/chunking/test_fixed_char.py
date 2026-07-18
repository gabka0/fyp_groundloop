from __future__ import annotations

from groundloop.ai.chunking import FixedCharChunker, prepare_document
from groundloop.ai.chunking.fixed_char import normalize_line_endings


def test_empty_document_produces_no_chunks() -> None:
    chunker = FixedCharChunker()
    assert chunker.chunk("document-version", "") == ()
    assert chunker.chunk("document-version", " \r\n\t") == ()


def test_crlf_is_normalized_and_paragraphs_are_greedily_packed() -> None:
    chunks = FixedCharChunker(max_characters=24).chunk(
        "document-version",
        " First line\r\ncontinued.\r\n\r\nSecond.\rThird. ",
    )
    assert tuple(chunk.text for chunk in chunks) == (
        "First line\ncontinued.",
        "Second.\nThird.",
    )
    assert normalize_line_endings("a\r\nb\rc") == "a\nb\nc"


def test_unicode_text_is_preserved_without_canonical_normalization() -> None:
    chunker = FixedCharChunker()
    composed = chunker.chunk("document-version", "Caf\u00e9")
    decomposed = chunker.chunk("document-version", "Cafe\u0301")
    assert composed[0].text == "Caf\u00e9"
    assert decomposed[0].text == "Cafe\u0301"
    assert composed[0].text_hash != decomposed[0].text_hash
    assert composed[0].chunk_version_id != decomposed[0].chunk_version_id


def test_long_paragraph_splits_at_whitespace_or_hard_limit_without_overlap() -> None:
    chunker = FixedCharChunker(max_characters=12)
    whitespace = chunker.chunk("dv-space", "alpha beta gamma delta")
    no_whitespace = chunker.chunk("dv-hard", "x" * 29)

    assert tuple(chunk.text for chunk in whitespace) == (
        "alpha beta",
        "gamma delta",
    )
    assert tuple(len(chunk.text) for chunk in no_whitespace) == (12, 12, 5)
    assert "".join(chunk.text for chunk in no_whitespace) == "x" * 29
    assert all(0 < len(chunk.text) <= 12 for chunk in (*whitespace, *no_whitespace))


def test_repeated_text_has_same_hash_but_index_distinguishes_chunk_ids() -> None:
    paragraph = "x" * 12
    chunks = FixedCharChunker(max_characters=12).chunk(
        "document-version", f"{paragraph}\n\n{paragraph}"
    )
    assert len(chunks) == 2
    assert chunks[0].text_hash == chunks[1].text_hash
    assert chunks[0].chunk_version_id != chunks[1].chunk_version_id
    assert tuple(chunk.chunk_index for chunk in chunks) == (0, 1)


def test_document_and_version_identity_cover_namespace_path_content_and_chunker() -> (
    None
):
    chunker = FixedCharChunker()
    first = prepare_document(
        corpus_namespace="software-docs",
        relative_path="guides/setup.txt",
        raw_content=b"Install Nimbus.",
        chunker=chunker,
    )
    replay = prepare_document(
        corpus_namespace="software-docs",
        relative_path="guides/setup.txt",
        raw_content=b"Install Nimbus.",
        chunker=chunker,
    )
    duplicate_content = prepare_document(
        corpus_namespace="software-docs",
        relative_path="archive/setup.txt",
        raw_content=b"Install Nimbus.",
        chunker=chunker,
    )
    changed_content = prepare_document(
        corpus_namespace="software-docs",
        relative_path="guides/setup.txt",
        raw_content=b"Install Nimbus 2.",
        chunker=chunker,
    )
    changed_chunker = prepare_document(
        corpus_namespace="software-docs",
        relative_path="guides/setup.txt",
        raw_content=b"Install Nimbus.",
        chunker=FixedCharChunker(artifact_id="fixed-char-v1-test-change"),
    )

    assert replay == first
    assert duplicate_content.identity.document_id != first.identity.document_id
    assert (
        duplicate_content.identity.raw_content_hash == first.identity.raw_content_hash
    )
    assert changed_content.identity.document_id == first.identity.document_id
    assert (
        changed_content.identity.document_version_id
        != first.identity.document_version_id
    )
    assert changed_chunker.identity.document_id == first.identity.document_id
    assert (
        changed_chunker.identity.document_version_id
        != first.identity.document_version_id
    )
    assert (
        changed_chunker.chunks[0].chunk_version_id != first.chunks[0].chunk_version_id
    )


def test_relative_path_identity_normalizes_separators_and_unicode_to_nfc() -> None:
    chunker = FixedCharChunker()
    windows = prepare_document(
        corpus_namespace="ns",
        relative_path="caf\u00e9\\guide.txt",
        raw_content=b"text",
        chunker=chunker,
    )
    decomposed = prepare_document(
        corpus_namespace="ns",
        relative_path="cafe\u0301/guide.txt",
        raw_content=b"text",
        chunker=chunker,
    )
    assert windows.identity.relative_path == "caf\u00e9/guide.txt"
    assert decomposed.identity.relative_path == windows.identity.relative_path
    assert decomposed.identity.document_id == windows.identity.document_id
