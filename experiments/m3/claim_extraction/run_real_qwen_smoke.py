"""Explicit opt-in pinned-Qwen generation and extraction smoke.

Do not run this command while another real-model workload owns the host. The
coordinator must release the serialized workload window first.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from groundloop.ai.claim_extraction import QwenClaimExtractor
from groundloop.ai.contracts import (
    ChunkDraft,
    EvidencePassage,
    QueryKind,
    RetrievalCandidate,
)
from groundloop.ai.generation import QwenAnswerGenerator, QwenCompletionBackend
from groundloop.domain import normalized_text_hash


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-real-model",
        action="store_true",
        help="required acknowledgement that this loads and executes Qwen",
    )
    parser.add_argument(
        "--allow-download",
        action="store_true",
        help="permit the exact pinned revision to be downloaded if absent",
    )
    args = parser.parse_args()
    if not args.run_real_model:
        parser.error("pass --run-real-model after coordinator approval")

    text = (
        "Nimbus 4.0 requires Python 3.12 or later. "
        "Python 3.10 and 3.11 are no longer supported."
    )
    chunk = ChunkDraft(
        chunk_version_id="smoke-chunk-nimbus-python",
        document_version_id="smoke-document-nimbus-v4",
        chunk_index=0,
        text=text,
        text_hash=normalized_text_hash(text),
        chunker_artifact_id="fixed-char-v1-1200-no-overlap",
    )
    candidate = RetrievalCandidate(
        candidate_id="smoke-candidate-1",
        query_kind=QueryKind.QUESTION,
        query_id="smoke-question-1",
        chunk_version_id=chunk.chunk_version_id,
        score=1.0,
        rank=1,
        embedding_model_artifact_id="bge-small-en-v1.5-pinned",
        method_version="bge-cosine-v1",
    )
    evidence = (EvidencePassage(candidate=candidate, chunk=chunk),)
    backend = QwenCompletionBackend(allow_download=args.allow_download)
    generation = QwenAnswerGenerator(backend=backend).generate_with_provenance(
        "Which Python versions does Nimbus 4.0 support?",
        evidence,
    )
    extraction = QwenClaimExtractor(backend=backend).extract_with_provenance(
        generation.answer,
        evidence,
    )
    print(
        json.dumps(
            {
                "answer": asdict(generation.answer),
                "generation_provenance": asdict(generation.provenance),
                "claim_extraction": asdict(extraction.result),
                "claim_extraction_provenance": asdict(extraction.provenance),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
