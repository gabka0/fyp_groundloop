"""Frozen lexical-v1 query selection with an injected PostgreSQL boundary."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    AdmissionChannel,
    CandidatePolicyManifest,
    ChannelHit,
    PairKey,
    stable_m4_digest,
)

_EXPECTED_CONFIG: dict[str, object] = {
    "channel_depth_rule": "approximate_cap_per_inserted_chunk",
    "empty_query_behavior": "no_hits",
    "idf_formula": "ln((claim_count + 1) / (document_frequency + 1))",
    "lexeme_selection_tie_rule": "idf_desc_then_lexeme_asc",
    "max_query_lexemes": 32,
    "normalization_version": "postgres-simple-v1",
    "postgres_regconfig": "simple",
    "query_operator": "or",
    "ranking_expression": "ts_rank_cd",
    "ranking_normalization_mask": 32,
    "result_tie_rule": "score_desc_then_claim_id_asc",
    "schema_version": "groundloop-m4-lexical-v1",
    "stop_word_policy": "none",
}


def _canonical_json(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class LexicalV1Config:
    config_hash: str
    max_query_lexemes: int = 32
    ranking_normalization_mask: int = 32
    postgres_regconfig: str = "simple"

    @classmethod
    def load(cls, path: Path) -> LexicalV1Config:
        if not path.is_file():
            raise FileNotFoundError(f"lexical-v1 config is absent: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload != _EXPECTED_CONFIG:
            raise ValidationError("lexical-v1 config differs from the frozen policy")
        canonical = _canonical_json(payload)
        return cls(
            config_hash=hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        )


@dataclass(frozen=True, slots=True)
class LexicalRegistrySnapshot:
    snapshot_id: str
    claim_lexemes: tuple[tuple[str, tuple[str, ...]], ...]

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip():
            raise ValidationError("lexical registry snapshot_id must be non-empty")
        claim_ids = tuple(claim_id for claim_id, _ in self.claim_lexemes)
        if claim_ids != tuple(sorted(set(claim_ids))):
            raise ValidationError("claim lexeme rows must be sorted and unique")
        for claim_id, lexemes in self.claim_lexemes:
            if not claim_id.strip():
                raise ValidationError("claim lexeme claim_id must be non-empty")
            if any(not lexeme.strip() for lexeme in lexemes):
                raise ValidationError("claim lexemes must be non-empty")
            if lexemes != tuple(sorted(set(lexemes))):
                raise ValidationError("claim lexemes must be sorted and unique")

    @property
    def claim_count(self) -> int:
        return len(self.claim_lexemes)

    @property
    def document_frequency(self) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for _claim_id, lexemes in self.claim_lexemes:
            counts.update(set(lexemes))
        return dict(counts)


@dataclass(frozen=True, slots=True)
class PreparedLexicalQuery:
    lexemes: tuple[str, ...]
    idf_by_lexeme: tuple[tuple[str, float], ...]
    query_artifact_hash: str


@dataclass(frozen=True, slots=True)
class LexicalRawHit:
    claim_id: str
    score: float

    def __post_init__(self) -> None:
        if not self.claim_id.strip():
            raise ValidationError("lexical raw hit claim_id must be non-empty")
        if not math.isfinite(self.score) or self.score < 0.0:
            raise ValidationError(
                "lexical raw hit score must be finite and nonnegative"
            )


class LexemeAnalyzer(Protocol):
    """Boundary implemented by PostgreSQL `to_tsvector('simple', text)`."""

    @property
    def artifact_id(self) -> str: ...

    def analyze(self, text: str) -> tuple[str, ...]: ...


class LexicalSearchBackend(Protocol):
    """Boundary for parameterized PostgreSQL OR-query + ts_rank_cd search."""

    @property
    def artifact_id(self) -> str: ...

    def search(
        self,
        *,
        query_lexemes: tuple[str, ...],
        limit: int,
        normalization_mask: int,
    ) -> tuple[LexicalRawHit, ...]: ...


class DeterministicFakeLexemeAnalyzer:
    """Download-free test double; not claimed equivalent to PostgreSQL parsing."""

    artifact_id = "fake-simple-lexeme-analyzer-v1"
    _token = re.compile(r"[^\W_]+(?:['-][^\W_]+)*", re.UNICODE)

    def analyze(self, text: str) -> tuple[str, ...]:
        return tuple(match.group(0).casefold() for match in self._token.finditer(text))


class DeterministicFakeLexicalBackend:
    """Overlap-score test double behind the real PostgreSQL scoring boundary."""

    artifact_id = "fake-ts-rank-cd-v1"

    def __init__(self, registry: LexicalRegistrySnapshot) -> None:
        self._claims = registry.claim_lexemes

    def search(
        self,
        *,
        query_lexemes: tuple[str, ...],
        limit: int,
        normalization_mask: int,
    ) -> tuple[LexicalRawHit, ...]:
        if normalization_mask != 32:
            raise ValidationError("lexical-v1 requires normalization mask 32")
        query = set(query_lexemes)
        hits = []
        for claim_id, lexemes in self._claims:
            overlap = len(query & set(lexemes))
            if overlap:
                # Deterministic fake score only. PostgreSQL owns real ts_rank_cd.
                score = overlap / (overlap + 1.0)
                hits.append(LexicalRawHit(claim_id, score))
        hits.sort(key=lambda hit: (-hit.score, hit.claim_id))
        return tuple(hits[:limit])


class LexicalV1Policy:
    def __init__(
        self,
        *,
        config: LexicalV1Config,
        registry: LexicalRegistrySnapshot,
        analyzer: LexemeAnalyzer,
        backend: LexicalSearchBackend,
    ) -> None:
        self.config = config
        self.registry = registry
        self.analyzer = analyzer
        self.backend = backend

    def prepare_query(self, chunk_text: str) -> PreparedLexicalQuery:
        distinct = set(self.analyzer.analyze(chunk_text))
        if any(not lexeme.strip() for lexeme in distinct):
            raise ValidationError("analyzer emitted an empty lexeme")
        frequencies = self.registry.document_frequency
        claim_count = self.registry.claim_count
        scored = [
            (
                lexeme,
                math.log((claim_count + 1) / (frequencies.get(lexeme, 0) + 1)),
            )
            for lexeme in distinct
        ]
        scored.sort(key=lambda item: (-item[1], item[0]))
        selected = tuple(scored[: self.config.max_query_lexemes])
        lexemes = tuple(lexeme for lexeme, _idf in selected)
        artifact_hash = stable_m4_digest(
            "m4-lexical-query-v1",
            self.config.config_hash,
            self.registry.snapshot_id,
            self.analyzer.artifact_id,
            *(
                value
                for lexeme, idf in selected
                for value in (lexeme, format(idf, ".17g"))
            ),
        )
        return PreparedLexicalQuery(lexemes, selected, artifact_hash)

    def search(
        self,
        *,
        epoch_id: int,
        manifest: CandidatePolicyManifest,
        chunk_version_id: str,
        chunk_text: str,
    ) -> tuple[ChannelHit, ...]:
        if epoch_id <= 0:
            raise ValidationError("epoch_id must be positive")
        if not chunk_version_id.strip():
            raise ValidationError("chunk_version_id must be non-empty")
        if manifest.lexical_config_hash != self.config.config_hash:
            raise ValidationError("manifest uses a different lexical-v1 config")
        if manifest.lexical_regconfig_identity != self.config.postgres_regconfig:
            raise ValidationError("manifest uses a different lexical regconfig")
        if manifest.claim_registry_snapshot_id != self.registry.snapshot_id:
            raise ValidationError("manifest uses a different claim registry snapshot")
        if manifest.claim_count != self.registry.claim_count:
            raise ValidationError("manifest claim count differs from the registry")
        limit = manifest.approximate_cap_per_inserted_chunk
        query = self.prepare_query(chunk_text)
        if not query.lexemes:
            return ()
        raw_hits = self.backend.search(
            query_lexemes=query.lexemes,
            limit=limit,
            normalization_mask=self.config.ranking_normalization_mask,
        )
        if not self.backend.artifact_id.strip():
            raise ValidationError("lexical backend artifact_id must be non-empty")
        claim_ids = tuple(hit.claim_id for hit in raw_hits)
        if len(set(claim_ids)) != len(claim_ids):
            raise ValidationError("lexical backend returned duplicate claim IDs")
        ordered = sorted(raw_hits, key=lambda hit: (-hit.score, hit.claim_id))[:limit]
        return tuple(
            ChannelHit(
                epoch_id=epoch_id,
                pair=PairKey(hit.claim_id, chunk_version_id),
                candidate_policy_id=manifest.policy_id,
                channel=AdmissionChannel.LEXICAL,
                rank=rank,
                score=hit.score,
                channel_artifact_hash=stable_m4_digest(
                    "m4-lexical-channel-hit-v1",
                    query.query_artifact_hash,
                    self.backend.artifact_id,
                    chunk_version_id,
                    hit.claim_id,
                    format(hit.score, ".17g"),
                    str(rank),
                ),
            )
            for rank, hit in enumerate(ordered, start=1)
        )


def load_frozen_lexical_v1(repo_root: Path) -> LexicalV1Config:
    return LexicalV1Config.load(repo_root / "configs/m4/impact/lexical_v1.json")
