from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from groundloop.ai.chunking import FixedCharChunker
from groundloop.ai.embeddings.bge import BGE_MODEL_ID, BGE_QUERY_PREFIX, BGE_REVISION
from groundloop.errors import ArtifactConflictError
from groundloop.m4.contracts import PairKey
from groundloop.m4.models import (
    ClaimEmbeddingInput,
    PairVerificationInput,
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
    inspect_local_artifacts,
)

CONFIG = Path("configs/m4/models/m3_reuse_v1.json")


def test_frozen_reuse_config_loads_exact_m3_identities() -> None:
    config = PinnedM3ReuseConfig.load(CONFIG)
    assert config.embedding_model_id == BGE_MODEL_ID
    assert config.embedding_revision == BGE_REVISION
    assert config.embedding_tokenizer_revision == BGE_REVISION
    assert config.embedding_query_prefix == BGE_QUERY_PREFIX
    assert config.verifier_checkpoint_tree_sha256 == (
        "81870b683cec57eff82665103fcff3a35f45b9c9be0e18b53dcd40f485bfa4cf"
    )
    assert config.calibration_version.startswith("temperature-v1:")
    assert config.decision_policy.support_threshold == 0.8


def test_artifact_inspection_reports_explicit_absent_paths(tmp_path: Path) -> None:
    availability = inspect_local_artifacts(
        PinnedM3ReuseConfig.load(CONFIG), artifact_root=tmp_path
    )
    assert not availability.available
    assert len(availability.problems) == 3
    assert all("absent" in problem for problem in availability.problems)


def test_configuration_rejects_model_revision_drift(tmp_path: Path) -> None:
    payload = json.loads(CONFIG.read_text(encoding="utf-8"))
    payload["embedding"]["revision"] = "main"
    changed = tmp_path / "drifted.json"
    changed.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ArtifactConflictError, match="BGE revision drift"):
        PinnedM3ReuseConfig.load(changed)


def test_pinned_local_artifact_smoke() -> None:
    """Opt-in; after opt-in, skip only when a named local artifact is absent."""
    if os.environ.get("GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE") != "1":
        pytest.skip("set GROUNDLOOP_RUN_M4_REAL_MODEL_SMOKE=1 for local model smoke")
    artifact_root = Path(
        os.environ.get("GROUNDLOOP_M3_ARTIFACT_ROOT", "/home/kassym/Desktop/groundloop")
    )
    config = PinnedM3ReuseConfig.load(CONFIG)
    availability = inspect_local_artifacts(config, artifact_root=artifact_root)
    if not availability.available:
        pytest.skip("; ".join(availability.problems))

    bundle = build_pinned_m3_adapters(config, artifact_root=artifact_root)
    claim_text = "GroundLoop exactness begins after semantic observations are stored."
    chunk_text = (
        "GroundLoop exactly maintains structured state relative to stored model "
        "judgments; it does not guarantee objective truth."
    )
    chunk = FixedCharChunker().chunk("m4-real-smoke-document-version", chunk_text)[0]
    claim = bundle.embeddings.embed_claims(
        (ClaimEmbeddingInput("m4-real-smoke-claim", claim_text),)
    )[0]
    passage = bundle.embeddings.embed_chunks((chunk,))[0]
    pair_input = PairVerificationInput(
        pair=PairKey("m4-real-smoke-claim", chunk.chunk_version_id),
        claim_text=claim_text,
        claim_required=True,
        claim_cited_chunk_version_ids=(),
        document_version_id=chunk.document_version_id,
        chunk_index=chunk.chunk_index,
        chunk_text=chunk.text,
        chunk_text_hash=chunk.text_hash,
        chunker_artifact_id=chunk.chunker_artifact_id,
    )
    first = bundle.verifier.verify_pairs((pair_input,))[0]
    second = bundle.verifier.verify_pairs((pair_input,))[0]

    assert len(claim.vector.vector) == 384
    assert len(passage.vector.vector) == 384
    assert first == second
    assert bundle.verifier.backend_pair_calls == 1
    assert first.result.raw_logits is not None
    assert sum(
        (
            first.result.scores.support,
            first.result.scores.refute,
            first.result.scores.neutral,
        )
    ) == pytest.approx(1.0)
