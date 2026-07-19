from __future__ import annotations

import json
from pathlib import Path

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.admission import (
    DeterministicFakeLexemeAnalyzer,
    DeterministicFakeLexicalBackend,
    LexicalRawHit,
    LexicalRegistrySnapshot,
    LexicalV1Config,
    LexicalV1Policy,
)
from groundloop.m4.contracts import CandidatePolicyManifest

from .conftest import policy_manifest


def _registry() -> LexicalRegistrySnapshot:
    return LexicalRegistrySnapshot(
        "registry-1",
        (
            ("claim-a", ("alpha", "common")),
            ("claim-b", ("beta", "common")),
            ("claim-c", ("common",)),
            ("claim-d", ("delta",)),
        ),
    )


def _policy(
    config: LexicalV1Config,
) -> tuple[LexicalV1Policy, CandidatePolicyManifest]:
    registry = _registry()
    manifest = policy_manifest(config, claim_count=registry.claim_count)
    return (
        LexicalV1Policy(
            config=config,
            registry=registry,
            analyzer=DeterministicFakeLexemeAnalyzer(),
            backend=DeterministicFakeLexicalBackend(registry),
        ),
        manifest,
    )


def test_checked_in_lexical_config_is_exact_and_content_addressed(
    lexical_config: LexicalV1Config,
) -> None:
    assert lexical_config.max_query_lexemes == 32
    assert lexical_config.ranking_normalization_mask == 32
    assert lexical_config.postgres_regconfig == "simple"
    assert len(lexical_config.config_hash) == 64


def test_config_rejects_semantic_drift(
    tmp_path: Path, lexical_config: LexicalV1Config
) -> None:
    del lexical_config
    source = (
        Path(__file__).resolve().parents[3] / "configs/m4/impact/lexical_v1.json"
    )
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["max_query_lexemes"] = 31
    changed = tmp_path / "lexical.json"
    changed.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValidationError, match="frozen"):
        LexicalV1Config.load(changed)


def test_idf_selection_and_fake_search_are_deterministic(
    lexical_config: LexicalV1Config,
) -> None:
    policy, manifest = _policy(lexical_config)
    query = policy.prepare_query("Common alpha gamma alpha")
    assert query.lexemes == ("gamma", "alpha", "common")
    assert query.idf_by_lexeme[0][1] > query.idf_by_lexeme[1][1]
    assert query.idf_by_lexeme[1][1] > query.idf_by_lexeme[2][1]

    hits = policy.search(
        epoch_id=1,
        manifest=manifest,
        chunk_version_id="chunk-1",
        chunk_text="Common alpha gamma alpha",
    )
    replay = policy.search(
        epoch_id=1,
        manifest=manifest,
        chunk_version_id="chunk-1",
        chunk_text="Common alpha gamma alpha",
    )
    assert replay == hits
    assert tuple(hit.pair.claim_id for hit in hits) == (
        "claim-a",
        "claim-b",
        "claim-c",
    )
    assert tuple(hit.rank for hit in hits) == (1, 2, 3)
    assert hits[1].score == hits[2].score


def test_stop_word_only_and_empty_text_are_not_special_cased(
    lexical_config: LexicalV1Config,
) -> None:
    policy, manifest = _policy(lexical_config)
    assert policy.prepare_query("the and").lexemes == ("and", "the")
    assert (
        policy.search(
            epoch_id=1,
            manifest=manifest,
            chunk_version_id="chunk",
            chunk_text="!!! ___",
        )
        == ()
    )


def test_query_cap_uses_idf_then_lexeme_order(lexical_config: LexicalV1Config) -> None:
    policy, _manifest = _policy(lexical_config)
    text = " ".join(f"term{index:02d}" for index in range(40, -1, -1))
    query = policy.prepare_query(text)
    assert len(query.lexemes) == 32
    assert query.lexemes == tuple(f"term{index:02d}" for index in range(32))


class _UnsortedBackend:
    artifact_id = "unsorted-test-backend"

    def search(
        self,
        *,
        query_lexemes: tuple[str, ...],
        limit: int,
        normalization_mask: int,
    ) -> tuple[LexicalRawHit, ...]:
        del query_lexemes, limit, normalization_mask
        return (
            LexicalRawHit("claim-c", 0.2),
            LexicalRawHit("claim-b", 0.8),
            LexicalRawHit("claim-a", 0.8),
        )


def test_policy_owns_score_then_claim_tie_order(
    lexical_config: LexicalV1Config,
) -> None:
    registry = _registry()
    policy = LexicalV1Policy(
        config=lexical_config,
        registry=registry,
        analyzer=DeterministicFakeLexemeAnalyzer(),
        backend=_UnsortedBackend(),
    )
    hits = policy.search(
        epoch_id=1,
        manifest=policy_manifest(lexical_config, claim_count=registry.claim_count),
        chunk_version_id="chunk",
        chunk_text="alpha",
    )
    assert tuple(hit.pair.claim_id for hit in hits) == (
        "claim-a",
        "claim-b",
        "claim-c",
    )


def test_manifest_must_match_config_and_registry(
    lexical_config: LexicalV1Config,
) -> None:
    policy, _manifest = _policy(lexical_config)
    wrong = policy_manifest(
        lexical_config,
        claim_count=4,
        registry_snapshot_id="other-registry",
    )
    with pytest.raises(ValidationError, match="snapshot"):
        policy.search(
            epoch_id=1,
            manifest=wrong,
            chunk_version_id="chunk",
            chunk_text="alpha",
        )


def test_registry_lexemes_must_be_canonical() -> None:
    with pytest.raises(ValidationError, match="sorted and unique"):
        LexicalRegistrySnapshot("registry", (("claim", ("z", "a", "a")),))
