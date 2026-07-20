from __future__ import annotations

from dataclasses import replace

import pytest

from groundloop.errors import ArtifactConflictError, ValidationError
from groundloop.m4.admission import (
    LexicalV1Config,
    fuse_admission_channels,
    lineage_channel_hits,
)
from groundloop.m4.models.contracts import sha256_text
from groundloop.m4.contracts import AdmissionChannel, ChannelHit, PairKey

from .conftest import HASH, policy_manifest


def _hit(
    channel: AdmissionChannel,
    claim_id: str,
    *,
    chunk_id: str = "chunk-1",
    rank: int,
    score: float | None,
    epoch_id: int = 1,
) -> ChannelHit:
    return ChannelHit(
        epoch_id=epoch_id,
        pair=PairKey(claim_id, chunk_id),
        candidate_policy_id="policy-1",
        channel=channel,
        rank=rank,
        score=score,
        channel_artifact_hash=(claim_id[0] * 64 if claim_id[0] in "abcdef" else HASH),
    )


def test_round_robin_dedup_and_lineage_excess_accounting(
    lexical_config: LexicalV1Config,
) -> None:
    manifest = policy_manifest(lexical_config, cap=3)
    vector = (
        _hit(AdmissionChannel.VECTOR, "a-vector", rank=1, score=0.9),
        _hit(AdmissionChannel.VECTOR, "b-shared", rank=2, score=0.8),
    )
    lexical = (
        _hit(AdmissionChannel.LEXICAL, "b-shared", rank=1, score=0.7),
        _hit(AdmissionChannel.LEXICAL, "c-lexical", rank=2, score=0.6),
    )
    lineage = lineage_channel_hits(
        epoch_id=1,
        candidate_policy_id=manifest.policy_id,
        pairs=(PairKey("a-vector", "chunk-1"), PairKey("d-lineage", "chunk-1")),
    )

    result = fuse_admission_channels(
        inserted_chunk_ids=("chunk-1",),
        manifest=manifest,
        vector_hits=vector,
        lexical_hits=lexical,
        lineage_hits=lineage,
    )

    assert tuple(item.pair.claim_id for item in result.admitted_pairs) == (
        "a-vector",
        "b-shared",
        "c-lexical",
        "d-lineage",
    )
    by_claim = {item.pair.claim_id: item for item in result.admitted_pairs}
    assert by_claim["a-vector"].reasons == (
        AdmissionChannel.LINEAGE,
        AdmissionChannel.VECTOR,
    )
    assert by_claim["b-shared"].reasons == (
        AdmissionChannel.LEXICAL,
        AdmissionChannel.VECTOR,
    )
    assert result.accounting.approximate_pair_count == 3
    assert result.accounting.mandatory_lineage_pair_count == 2
    assert result.accounting.lineage_excess_pair_count == 1
    assert result.accounting.admitted_pair_count == 4
    assert result.accounting.verifier_call_upper_bound == 4
    assert len(result.channel_hits) == 6


def test_pair_dedup_is_event_pair_not_claim_only(
    lexical_config: LexicalV1Config,
) -> None:
    manifest = policy_manifest(lexical_config, cap=1)
    result = fuse_admission_channels(
        inserted_chunk_ids=("chunk-1", "chunk-2"),
        manifest=manifest,
        vector_hits=(
            _hit(AdmissionChannel.VECTOR, "a-claim", rank=1, score=1.0),
            _hit(
                AdmissionChannel.VECTOR,
                "a-claim",
                chunk_id="chunk-2",
                rank=1,
                score=1.0,
            ),
        ),
        lexical_hits=(),
    )
    assert len(result.admitted_pairs) == 2
    assert result.accounting.approximate_pair_count == 2
    assert result.accounting.verifier_call_upper_bound == 2


def test_empty_channels_remain_within_cap_bound(
    lexical_config: LexicalV1Config,
) -> None:
    manifest = policy_manifest(lexical_config, cap=5)
    result = fuse_admission_channels(
        inserted_chunk_ids=("chunk-1", "chunk-2"),
        manifest=manifest,
        vector_hits=(),
        lexical_hits=(),
    )
    assert result.admitted_pairs == ()
    assert result.accounting.verifier_call_upper_bound == 10


def test_conflicting_channel_payload_and_mixed_epochs_fail(
    lexical_config: LexicalV1Config,
) -> None:
    manifest = policy_manifest(lexical_config)
    hit = _hit(AdmissionChannel.VECTOR, "a-claim", rank=1, score=1.0)
    with pytest.raises(ArtifactConflictError, match="conflicting"):
        fuse_admission_channels(
            inserted_chunk_ids=("chunk-1",),
            manifest=manifest,
            vector_hits=(hit, replace(hit, score=0.5)),
            lexical_hits=(),
        )

    with pytest.raises(ValidationError, match="mix epochs"):
        fuse_admission_channels(
            inserted_chunk_ids=("chunk-1",),
            manifest=manifest,
            vector_hits=(hit,),
            lexical_hits=(
                _hit(
                    AdmissionChannel.LEXICAL,
                    "b-claim",
                    rank=1,
                    score=0.5,
                    epoch_id=2,
                ),
            ),
        )


def test_ranks_policy_and_lineage_mode_are_checked(
    lexical_config: LexicalV1Config,
) -> None:
    manifest = policy_manifest(lexical_config)
    with pytest.raises(ValidationError, match="contiguous"):
        fuse_admission_channels(
            inserted_chunk_ids=("chunk-1",),
            manifest=manifest,
            vector_hits=(
                _hit(AdmissionChannel.VECTOR, "a-claim", rank=2, score=1.0),
            ),
            lexical_hits=(),
        )

    wrong_policy = replace(
        _hit(AdmissionChannel.VECTOR, "a-claim", rank=1, score=1.0),
        candidate_policy_id="other-policy",
    )
    with pytest.raises(ValidationError, match="different candidate policy"):
        fuse_admission_channels(
            inserted_chunk_ids=("chunk-1",),
            manifest=manifest,
            vector_hits=(wrong_policy,),
            lexical_hits=(),
        )

    no_lineage = policy_manifest(lexical_config, lineage=False)
    with pytest.raises(ValidationError, match="safety override"):
        fuse_admission_channels(
            inserted_chunk_ids=("chunk-1",),
            manifest=no_lineage,
            vector_hits=(),
            lexical_hits=(),
            lineage_hits=lineage_channel_hits(
                epoch_id=1,
                candidate_policy_id=no_lineage.policy_id,
                pairs=(PairKey("a-claim", "chunk-1"),),
            ),
        )


def test_manifest_hash_binds_roles_configs_and_index_kind(
    lexical_config: LexicalV1Config,
) -> None:
    exact = policy_manifest(lexical_config)
    assert exact.claim_role_template_hash == sha256_text(
        "Represent this sentence for searching relevant passages: {claim}"
    )
    assert exact.chunk_role_template_hash == sha256_text("{chunk}")
    changed_search = policy_manifest(
        lexical_config,
        vector_search=(("distance", "cosine"), ("ef_search", "40")),
    )
    assert exact.policy_hash != changed_search.policy_hash

    with pytest.raises(ValidationError, match="sorted"):
        policy_manifest(
            lexical_config,
            vector_search=(("z", "1"), ("a", "2")),
        )
