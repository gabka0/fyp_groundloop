"""Explicit local-only smoke for the frozen BGE artifact."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.contracts import QueryKind
from groundloop.ai.embeddings import BGE_REVISION, BgeSmallEmbedder
from groundloop.ai.embeddings.common import ArtifactUnavailableError


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run BGE only from an already-populated local cache."
    )
    parser.add_argument("--cache-dir", required=True, type=Path)
    args = parser.parse_args()
    embedder = BgeSmallEmbedder(cache_dir=args.cache_dir)
    chunk = FixedCharChunker().chunk(
        "real-smoke-document-version",
        "Nimbus 3 supports Python 3.11 and Python 3.12.",
    )
    try:
        passage = embedder.embed(chunk)[0]
        query = embedder.embed_query(
            "Which Python versions does Nimbus 3 support?",
            QueryKind.QUESTION,
        )
    except ArtifactUnavailableError as error:
        raise SystemExit(f"REAL_MODEL_ARTIFACT_MISSING: {error}") from error
    print(
        json.dumps(
            {
                "model_revision": BGE_REVISION,
                "passage_dimension": len(passage.vector),
                "passage_norm": math.sqrt(sum(x * x for x in passage.vector)),
                "query_dimension": len(query.vector),
                "query_norm": math.sqrt(sum(x * x for x in query.vector)),
                "query_truncated": query.audit.truncated,
                "status": "ok",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
