"""Naturally-versioned Git-history pilot for the M4 empirical contract.

This module deliberately keeps three boundaries visible:

* Git commits and extracted text are deterministic source facts.
* Pinned BGE/MiniLM outputs are versioned model judgments, not truth labels.
* ``empirical_eval`` computes exact metrics over those frozen artifacts.

The command is opt-in because it needs local model artifacts and PostgreSQL.
No model or source repository is downloaded by this module.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import subprocess
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, cast

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from groundloop.ai.contracts import AtomicClaim, ChunkDraft, VerificationResult
from groundloop.ai.verification.adapter import PinnedMiniLMVerifier
from groundloop.domain import AnswerState, ClaimState, normalized_text_hash
from groundloop.errors import ValidationError
from groundloop.m4.admission.fusion import fuse_admission_channels
from groundloop.m4.admission.lexical import (
    LexicalRegistrySnapshot,
    LexicalV1Policy,
    load_frozen_lexical_v1,
)
from groundloop.m4.admission.manifest import build_candidate_policy_manifest
from groundloop.m4.admission.postgres_common import PostgresAdmissionServerIdentity
from groundloop.m4.admission.postgres_lexical import (
    PostgresLexicalSearchBackend,
    PostgresSimpleLexemeAnalyzer,
)
from groundloop.m4.admission.vector import ExactReverseVectorIndex
from groundloop.m4.artifacts import PostgresM4ArtifactRegistry
from groundloop.m4.contracts import (
    CandidatePolicyManifest,
    ChannelHit,
    PairJudgment,
    PairKey,
    SnapshotRefreshResult,
    VectorIndexKind,
    sha256_text,
    stable_m4_digest,
)
from groundloop.m4.empirical_eval import (
    AblationKind,
    EmpiricalStudyReport,
    EmpiricalStudySpec,
    FrozenOracleEvent,
    HistoryAssignment,
    ObjectStatus,
    PolicySpec,
    TreatmentEvent,
    VerifierMeasurement,
    WrittenEmpiricalBundle,
    evaluate_frozen_history,
    write_empirical_bundle,
)
from groundloop.m4.event_audit import (
    EventAuditDisposition,
    EventAuditSpec,
    PersistedEventAudit,
    run_and_persist_event_audit,
)
from groundloop.m4.models.config import (
    PinnedM3AdapterBundle,
    PinnedM3ReuseConfig,
    build_pinned_m3_adapters,
)
from groundloop.m4.models.contracts import (
    CHUNK_ROLE_TEMPLATE,
    CLAIM_ROLE_TEMPLATE,
    PairVerificationArtifact,
    PairVerificationInput,
    RoleEmbeddingProvenance,
)
from groundloop.m4.models.embedding import ClaimEmbeddingInput
from groundloop.m4.models.verification import M4CalibratedVerifierAdapter
from groundloop.m4.oracles.grounding import recompute_grounding_states
from groundloop.m4.oracles.refresh import (
    RefreshChunk,
    RefreshClaim,
    run_snapshot_refresh,
)
from groundloop.m4.oracles.testing import DeterministicJudgmentTable
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m4.smoke import _create_schema, _drop_schema, _psycopg_url

_SCHEMA_VERSION = "groundloop-m4-real-git-histories-v2"
_RESULT_SCHEMA = "groundloop-m4-real-history-study-result-v2"
_CHUNKER_ID = "git-inclusive-line-range-v1"
_HEX = frozenset("0123456789abcdef")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _json_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValidationError(f"{name} must be non-empty")


def _require_hash(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in _HEX for character in value):
        raise ValidationError(f"{name} must be a lowercase SHA-256 digest")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValidationError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValidationError(f"{name} must be an array")
    return cast(list[object], value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be non-empty text")
    return value


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _text(value, name)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer")
    return value


def _optional_integer(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _integer(value, name)


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{name} must be boolean")
    return value


@dataclass(frozen=True, slots=True)
class FrozenExtraction:
    extraction_id: str
    rule: str
    parent_start_line: int | None
    parent_end_line: int | None
    parent_excerpt_sha256: str | None
    child_start_line: int
    child_end_line: int
    child_excerpt_sha256: str

    def __post_init__(self) -> None:
        _require_text("extraction_id", self.extraction_id)
        if self.rule != "inclusive_utf8_line_range_trim_outer_whitespace_append_lf_v1":
            raise ValidationError("unsupported real-history extraction rule")
        if self.child_start_line <= 0 or self.child_end_line < self.child_start_line:
            raise ValidationError("invalid child extraction line range")
        _require_hash("child_excerpt_sha256", self.child_excerpt_sha256)
        parent_fields = (
            self.parent_start_line,
            self.parent_end_line,
            self.parent_excerpt_sha256,
        )
        if any(value is None for value in parent_fields) and not all(
            value is None for value in parent_fields
        ):
            raise ValidationError(
                "parent extraction fields must be all present or absent"
            )
        if self.parent_start_line is not None:
            assert self.parent_end_line is not None
            assert self.parent_excerpt_sha256 is not None
            if (
                self.parent_start_line <= 0
                or self.parent_end_line < self.parent_start_line
            ):
                raise ValidationError("invalid parent extraction line range")
            _require_hash("parent_excerpt_sha256", self.parent_excerpt_sha256)


@dataclass(frozen=True, slots=True)
class FrozenClaim:
    claim_id: str
    text: str
    required: bool
    claim_family_id: str
    prior_citation_extraction_ids: tuple[str, ...]
    construction_note: str

    def __post_init__(self) -> None:
        _require_text("claim_id", self.claim_id)
        _require_text("claim text", self.text)
        _require_text("claim_family_id", self.claim_family_id)
        _require_text("construction_note", self.construction_note)
        if self.prior_citation_extraction_ids != tuple(
            sorted(set(self.prior_citation_extraction_ids))
        ):
            raise ValidationError("prior citation IDs must be sorted and unique")


@dataclass(frozen=True, slots=True)
class FrozenGitHistory:
    history_id: str
    repository_url: str
    commit: str
    parent: str
    path: str
    diff_sha256: str
    parent_blob_oid: str | None
    parent_content_sha256: str | None
    child_blob_oid: str
    child_content_sha256: str
    event_kind: str
    extractions: tuple[FrozenExtraction, ...]
    answer_id: str
    claims: tuple[FrozenClaim, ...]

    def __post_init__(self) -> None:
        for name, value in (
            ("history_id", self.history_id),
            ("repository_url", self.repository_url),
            ("commit", self.commit),
            ("parent", self.parent),
            ("path", self.path),
            ("child_blob_oid", self.child_blob_oid),
            ("answer_id", self.answer_id),
        ):
            _require_text(name, value)
        if self.event_kind not in {"insert", "replace"}:
            raise ValidationError("real history must be insert or replace")
        _require_hash("child_content_sha256", self.child_content_sha256)
        _require_hash("diff_sha256", self.diff_sha256)
        if (self.parent_blob_oid is None) != (self.parent_content_sha256 is None):
            raise ValidationError("parent blob and content hashes must co-occur")
        if self.parent_content_sha256 is not None:
            _require_hash("parent_content_sha256", self.parent_content_sha256)
        if self.event_kind == "replace" and self.parent_blob_oid is None:
            raise ValidationError("replace history requires a parent path")
        if self.event_kind == "insert" and self.parent_blob_oid is not None:
            raise ValidationError("insert history must record an absent parent path")
        extraction_ids = tuple(item.extraction_id for item in self.extractions)
        claim_ids = tuple(item.claim_id for item in self.claims)
        if not extraction_ids or len(set(extraction_ids)) != len(extraction_ids):
            raise ValidationError("history extractions must be non-empty and unique")
        if not claim_ids or len(set(claim_ids)) != len(claim_ids):
            raise ValidationError("history claims must be non-empty and unique")
        for claim in self.claims:
            if not set(claim.prior_citation_extraction_ids) <= set(extraction_ids):
                raise ValidationError("claim citation names an unknown extraction")


@dataclass(frozen=True, slots=True)
class AdmissionStudyConfig:
    approximate_cap_per_inserted_chunk: int
    vector_depth: int
    lexical_depth: int
    frontier_depth: int
    snapshot_refresh_depth: int
    fusion_rule: str
    lineage_rule: str
    frontier_rule: str
    fresh_fallback_rule: str

    def __post_init__(self) -> None:
        for name, count_value in (
            (
                "approximate_cap_per_inserted_chunk",
                self.approximate_cap_per_inserted_chunk,
            ),
            ("vector_depth", self.vector_depth),
            ("lexical_depth", self.lexical_depth),
            ("frontier_depth", self.frontier_depth),
            ("snapshot_refresh_depth", self.snapshot_refresh_depth),
        ):
            if count_value <= 0:
                raise ValidationError(f"{name} must be positive")
        for name, text_value in (
            ("fusion_rule", self.fusion_rule),
            ("lineage_rule", self.lineage_rule),
            ("frontier_rule", self.frontier_rule),
            ("fresh_fallback_rule", self.fresh_fallback_rule),
        ):
            _require_text(name, text_value)


@dataclass(frozen=True, slots=True)
class RealGitStudyDefinition:
    study_id: str
    split_id: str
    annotation_protocol: Mapping[str, object]
    admission: AdmissionStudyConfig
    histories: tuple[FrozenGitHistory, ...]
    definition_hash: str


@dataclass(frozen=True, slots=True)
class VerifiedExcerpt:
    extraction: FrozenExtraction
    parent_text: str | None
    child_text: str
    parent_chunk_id: str | None
    child_chunk_id: str


@dataclass(frozen=True, slots=True)
class VerifiedGitHistory:
    spec: FrozenGitHistory
    runtime_root: Path
    source_remote_name: str
    change_status: str
    excerpts: tuple[VerifiedExcerpt, ...]


@dataclass(frozen=True, slots=True)
class RealHistoryStudyRunConfig:
    repo_root: Path
    artifact_root: Path
    database_url: str
    source_roots: Mapping[str, Path]
    definition_path: Path | None = None
    model_config_path: Path | None = None
    output_directory: Path = Path("/tmp/groundloop-m4-10-real-history")
    bootstrap_seed: int = 20260720
    bootstrap_replicates: int = 10_000

    @property
    def resolved_definition_path(self) -> Path:
        return self.definition_path or (
            self.repo_root / "configs/m4/real_history/pinned_git_histories_v1.json"
        )

    @property
    def resolved_model_config_path(self) -> Path:
        return self.model_config_path or (
            self.repo_root / "configs/m4/models/m3_reuse_v1.json"
        )


@dataclass(frozen=True, slots=True)
class RealHistoryStudyResult:
    report: EmpiricalStudyReport
    bundle: WrittenEmpiricalBundle
    result_manifest: Mapping[str, object]
    structural_hash: str
    timing_hash: str


def load_real_git_study_definition(path: Path) -> RealGitStudyDefinition:
    """Load and validate the committed, manually curated study definition."""
    try:
        raw = path.read_bytes()
        payload = _object(json.loads(raw), "study definition")
    except (OSError, json.JSONDecodeError) as error:
        raise ValidationError(f"cannot read real-history definition: {path}") from error
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise ValidationError("unsupported real-history study schema")
    annotation = _object(payload.get("annotation_protocol"), "annotation_protocol")
    admission_raw = _object(payload.get("admission"), "admission")
    admission = AdmissionStudyConfig(
        approximate_cap_per_inserted_chunk=_integer(
            admission_raw.get("approximate_cap_per_inserted_chunk"),
            "approximate_cap_per_inserted_chunk",
        ),
        vector_depth=_integer(admission_raw.get("vector_depth"), "vector_depth"),
        lexical_depth=_integer(admission_raw.get("lexical_depth"), "lexical_depth"),
        frontier_depth=_integer(admission_raw.get("frontier_depth"), "frontier_depth"),
        snapshot_refresh_depth=_integer(
            admission_raw.get("snapshot_refresh_depth"), "snapshot_refresh_depth"
        ),
        fusion_rule=_text(admission_raw.get("fusion_rule"), "fusion_rule"),
        lineage_rule=_text(admission_raw.get("lineage_rule"), "lineage_rule"),
        frontier_rule=_text(admission_raw.get("frontier_rule"), "frontier_rule"),
        fresh_fallback_rule=_text(
            admission_raw.get("fresh_fallback_rule"), "fresh_fallback_rule"
        ),
    )
    histories: list[FrozenGitHistory] = []
    for raw_history in _array(payload.get("histories"), "histories"):
        history = _object(raw_history, "history")
        source = _object(history.get("source_identity"), "source_identity")
        extractions: list[FrozenExtraction] = []
        for raw_extraction in _array(history.get("extractions"), "extractions"):
            item = _object(raw_extraction, "extraction")
            extractions.append(
                FrozenExtraction(
                    extraction_id=_text(item.get("extraction_id"), "extraction_id"),
                    rule=_text(item.get("rule"), "extraction rule"),
                    parent_start_line=_optional_integer(
                        item.get("parent_start_line"), "parent_start_line"
                    ),
                    parent_end_line=_optional_integer(
                        item.get("parent_end_line"), "parent_end_line"
                    ),
                    parent_excerpt_sha256=_optional_text(
                        item.get("parent_excerpt_sha256"), "parent_excerpt_sha256"
                    ),
                    child_start_line=_integer(
                        item.get("child_start_line"), "child_start_line"
                    ),
                    child_end_line=_integer(
                        item.get("child_end_line"), "child_end_line"
                    ),
                    child_excerpt_sha256=_text(
                        item.get("child_excerpt_sha256"), "child_excerpt_sha256"
                    ),
                )
            )
        claims: list[FrozenClaim] = []
        for raw_claim in _array(history.get("claims"), "claims"):
            item = _object(raw_claim, "claim")
            citations = tuple(
                sorted(
                    _text(value, "prior citation")
                    for value in _array(
                        item.get("prior_citation_extraction_ids"),
                        "prior_citation_extraction_ids",
                    )
                )
            )
            claims.append(
                FrozenClaim(
                    claim_id=_text(item.get("claim_id"), "claim_id"),
                    text=_text(item.get("text"), "claim text"),
                    required=_boolean(item.get("required"), "required"),
                    claim_family_id=_text(
                        item.get("claim_family_id"), "claim_family_id"
                    ),
                    prior_citation_extraction_ids=citations,
                    construction_note=_text(
                        item.get("construction_note"), "construction_note"
                    ),
                )
            )
        histories.append(
            FrozenGitHistory(
                history_id=_text(history.get("history_id"), "history_id"),
                repository_url=_text(source.get("repository_url"), "repository_url"),
                commit=_text(source.get("commit"), "commit"),
                parent=_text(source.get("parent"), "parent"),
                path=_text(source.get("path"), "path"),
                diff_sha256=_text(source.get("diff_sha256"), "diff_sha256"),
                parent_blob_oid=_optional_text(
                    source.get("parent_blob_oid"), "parent_blob_oid"
                ),
                parent_content_sha256=_optional_text(
                    source.get("parent_content_sha256"), "parent_content_sha256"
                ),
                child_blob_oid=_text(source.get("child_blob_oid"), "child_blob_oid"),
                child_content_sha256=_text(
                    source.get("child_content_sha256"), "child_content_sha256"
                ),
                event_kind=_text(history.get("event_kind"), "event_kind"),
                extractions=tuple(extractions),
                answer_id=_text(history.get("answer_id"), "answer_id"),
                claims=tuple(sorted(claims, key=lambda claim: claim.claim_id)),
            )
        )
    canonical = tuple(sorted(histories, key=lambda history: history.history_id))
    if len(canonical) < 3:
        raise ValidationError("real-history study requires at least three histories")
    if len({history.history_id for history in canonical}) != len(canonical):
        raise ValidationError("real-history IDs must be unique")
    repositories = {history.repository_url for history in canonical}
    if len(repositories) < 3:
        raise ValidationError("real-history study requires three source repositories")
    return RealGitStudyDefinition(
        study_id=_text(payload.get("study_id"), "study_id"),
        split_id=_text(payload.get("split_id"), "split_id"),
        annotation_protocol=annotation,
        admission=admission,
        histories=canonical,
        definition_hash=_sha256(raw),
    )


def _git(root: Path, *arguments: str, binary: bool = False) -> str | bytes:
    command = ("git", "-C", str(root), *arguments)
    try:
        output = subprocess.run(
            command,
            check=True,
            capture_output=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        detail = ""
        if isinstance(error, subprocess.CalledProcessError):
            detail = error.stderr.decode("utf-8", errors="replace").strip()
        raise ValidationError(
            f"Git source check failed for {root}: {' '.join(arguments)}: {detail}"
        ) from error
    return output if binary else output.decode("utf-8").strip()


def _extract_lines(content: bytes, start: int, end: int) -> str:
    try:
        lines = content.decode("utf-8").splitlines()
    except UnicodeDecodeError as error:
        raise ValidationError("pinned documentation blob is not UTF-8") from error
    if end > len(lines):
        raise ValidationError("pinned extraction line range exceeds the blob")
    return "\n".join(lines[start - 1 : end]).strip() + "\n"


def verify_git_histories(
    definition: RealGitStudyDefinition,
    source_roots: Mapping[str, Path],
) -> tuple[VerifiedGitHistory, ...]:
    """Verify commits, parents, remotes, blobs, hashes, diffs and line ranges."""
    if set(source_roots) != {history.history_id for history in definition.histories}:
        raise ValidationError("runtime source roots must cover every history exactly")
    verified: list[VerifiedGitHistory] = []
    for history in definition.histories:
        root = source_roots[history.history_id].resolve()
        if not (root / ".git").exists() and _git(root, "rev-parse", "--git-dir") == "":
            raise ValidationError(f"runtime source root is not a Git checkout: {root}")
        actual_parent = cast(str, _git(root, "rev-parse", f"{history.commit}^"))
        if actual_parent != history.parent:
            raise ValidationError(f"parent drift for {history.history_id}")
        remotes = cast(str, _git(root, "remote", "-v")).splitlines()
        matching_remote = next(
            (
                line.split()[0]
                for line in remotes
                if len(line.split()) >= 2 and line.split()[1] == history.repository_url
            ),
            None,
        )
        if matching_remote is None:
            raise ValidationError(
                f"no local remote matches source identity {history.repository_url}"
            )
        child_blob = cast(
            str, _git(root, "rev-parse", f"{history.commit}:{history.path}")
        )
        child_content = cast(
            bytes, _git(root, "show", f"{history.commit}:{history.path}", binary=True)
        )
        if child_blob != history.child_blob_oid:
            raise ValidationError(f"child blob OID drift for {history.history_id}")
        if _sha256(child_content) != history.child_content_sha256:
            raise ValidationError(f"child content hash drift for {history.history_id}")
        parent_content: bytes | None = None
        if history.parent_blob_oid is None:
            exists = (
                subprocess.run(
                    (
                        "git",
                        "-C",
                        str(root),
                        "cat-file",
                        "-e",
                        f"{history.parent}:{history.path}",
                    ),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                ).returncode
                == 0
            )
            if exists:
                raise ValidationError("insert history unexpectedly has a parent blob")
        else:
            parent_blob = cast(
                str, _git(root, "rev-parse", f"{history.parent}:{history.path}")
            )
            parent_content = cast(
                bytes,
                _git(root, "show", f"{history.parent}:{history.path}", binary=True),
            )
            if parent_blob != history.parent_blob_oid:
                raise ValidationError(f"parent blob OID drift for {history.history_id}")
            if _sha256(parent_content) != history.parent_content_sha256:
                raise ValidationError(
                    f"parent content hash drift for {history.history_id}"
                )
        change = cast(
            str,
            _git(
                root,
                "diff",
                "--name-status",
                history.parent,
                history.commit,
                "--",
                history.path,
            ),
        )
        expected_prefix = "A\t" if history.event_kind == "insert" else "M\t"
        if not change.startswith(expected_prefix):
            raise ValidationError(
                f"source change kind differs for {history.history_id}: {change!r}"
            )
        diff_bytes = cast(
            bytes,
            _git(
                root,
                "diff",
                "--no-ext-diff",
                "--unified=3",
                history.parent,
                history.commit,
                "--",
                history.path,
                binary=True,
            ),
        )
        if _sha256(diff_bytes) != history.diff_sha256:
            raise ValidationError(f"Git diff hash drift for {history.history_id}")
        excerpts: list[VerifiedExcerpt] = []
        for extraction in history.extractions:
            child_text = _extract_lines(
                child_content, extraction.child_start_line, extraction.child_end_line
            )
            if _sha256(child_text.encode("utf-8")) != extraction.child_excerpt_sha256:
                raise ValidationError(
                    "child excerpt drift for "
                    f"{history.history_id}/{extraction.extraction_id}"
                )
            parent_text: str | None = None
            parent_chunk_id: str | None = None
            if extraction.parent_start_line is not None:
                assert extraction.parent_end_line is not None
                assert extraction.parent_excerpt_sha256 is not None
                if parent_content is None:
                    raise ValidationError("parent extraction has no parent blob")
                parent_text = _extract_lines(
                    parent_content,
                    extraction.parent_start_line,
                    extraction.parent_end_line,
                )
                if _sha256(parent_text.encode("utf-8")) != (
                    extraction.parent_excerpt_sha256
                ):
                    raise ValidationError(
                        "parent excerpt drift for "
                        f"{history.history_id}/{extraction.extraction_id}"
                    )
                if parent_text == child_text:
                    raise ValidationError("selected replacement excerpt did not change")
                parent_chunk_id = (
                    f"git-parent:{history.history_id}:{extraction.extraction_id}:"
                    f"{history.parent[:12]}"
                )
            child_chunk_id = (
                f"git-child:{history.history_id}:{extraction.extraction_id}:"
                f"{history.commit[:12]}"
            )
            excerpts.append(
                VerifiedExcerpt(
                    extraction=extraction,
                    parent_text=parent_text,
                    child_text=child_text,
                    parent_chunk_id=parent_chunk_id,
                    child_chunk_id=child_chunk_id,
                )
            )
        verified.append(
            VerifiedGitHistory(
                spec=history,
                runtime_root=root,
                source_remote_name=matching_remote,
                change_status=change,
                excerpts=tuple(excerpts),
            )
        )
    return tuple(verified)


def _refresh_claims(
    history: VerifiedGitHistory,
    claim_vectors: Mapping[str, tuple[float, ...]],
) -> tuple[RefreshClaim, ...]:
    return tuple(
        RefreshClaim(
            claim.claim_id,
            history.spec.answer_id,
            claim.required,
            claim_vectors[claim.claim_id],
        )
        for claim in history.spec.claims
    )


def _child_drafts(history: VerifiedGitHistory) -> tuple[ChunkDraft, ...]:
    return tuple(
        sorted(
            (
                ChunkDraft(
                    chunk_version_id=excerpt.child_chunk_id,
                    document_version_id=f"git-child-version:{history.spec.history_id}",
                    chunk_index=index,
                    text=excerpt.child_text,
                    text_hash=normalized_text_hash(excerpt.child_text),
                    chunker_artifact_id=_CHUNKER_ID,
                )
                for index, excerpt in enumerate(history.excerpts)
            ),
            key=lambda draft: draft.chunk_version_id,
        )
    )


def _pair_inputs(
    history: VerifiedGitHistory,
    drafts: tuple[ChunkDraft, ...],
) -> tuple[PairVerificationInput, ...]:
    draft_by_id = {draft.chunk_version_id: draft for draft in drafts}
    prior_by_extraction = {
        excerpt.extraction.extraction_id: excerpt.parent_chunk_id
        for excerpt in history.excerpts
    }
    inputs: list[PairVerificationInput] = []
    for claim in history.spec.claims:
        citations = tuple(
            sorted(
                cast(str, prior_by_extraction[extraction_id])
                for extraction_id in claim.prior_citation_extraction_ids
            )
        )
        for chunk_id in sorted(draft_by_id):
            draft = draft_by_id[chunk_id]
            inputs.append(
                PairVerificationInput(
                    pair=PairKey(claim.claim_id, chunk_id),
                    claim_text=claim.text,
                    claim_required=claim.required,
                    claim_cited_chunk_version_ids=citations,
                    document_version_id=draft.document_version_id,
                    chunk_index=draft.chunk_index,
                    chunk_text=draft.text,
                    chunk_text_hash=draft.text_hash,
                    chunker_artifact_id=draft.chunker_artifact_id,
                )
            )
    return tuple(sorted(inputs, key=lambda item: item.pair))


def _states(
    history: VerifiedGitHistory,
    drafts: tuple[ChunkDraft, ...],
    judgments: tuple[PairJudgment, ...],
) -> tuple[tuple[ClaimState, ...], tuple[AnswerState, ...]]:
    return recompute_grounding_states(
        claim_to_answer={
            claim.claim_id: history.spec.answer_id for claim in history.spec.claims
        },
        required_claim_ids=frozenset(
            claim.claim_id for claim in history.spec.claims if claim.required
        ),
        chunk_text_hashes={draft.chunk_version_id: draft.text_hash for draft in drafts},
        judgments=judgments,
    )


def _policy_manifest(
    *,
    history: VerifiedGitHistory,
    definition: RealGitStudyDefinition,
    bundle: PinnedM3AdapterBundle,
    server: PostgresAdmissionServerIdentity,
    repo_root: Path,
) -> CandidatePolicyManifest:
    lexical = load_frozen_lexical_v1(repo_root)
    snapshot_id = "git-registry-" + stable_m4_digest(
        "m4-10-git-registry-v1",
        history.spec.history_id,
        *(claim.claim_id for claim in history.spec.claims),
    )
    return build_candidate_policy_manifest(
        policy_id=f"m4-10-policy:{history.spec.history_id}",
        embedding_model_artifact_id=bundle.embeddings.spec.model_artifact.artifact_id,
        claim_role_template=CLAIM_ROLE_TEMPLATE,
        chunk_role_template=CHUNK_ROLE_TEMPLATE,
        vector_method_version="exact-reverse-bge-v1",
        vector_index_kind=VectorIndexKind.EXACT,
        vector_index_build_config=(("method", "in-memory-exact"),),
        vector_search_config=(
            ("depth", str(definition.admission.vector_depth)),
            ("tie_rule", "distance_then_claim_id"),
        ),
        lexical_method_version="lexical-v1",
        lexical_config=lexical,
        lexical_postgres_version=server.postgres_version,
        lexical_regconfig_identity=server.regconfig_identity,
        claim_registry_snapshot_id=snapshot_id,
        claim_count=len(history.spec.claims),
        fusion_version="rank-interleave-v1",
        approximate_cap_per_inserted_chunk=(
            definition.admission.approximate_cap_per_inserted_chunk
        ),
        frontier_depth=definition.admission.frontier_depth,
        verifier_execution_spec_hash=bundle.verifier.spec.execution_spec_hash,
        decision_policy_version=(bundle.verifier.spec.decision_policy.policy_version),
    )


def _insert_decision_policy(
    connection: Connection[Any], config: PinnedM3ReuseConfig, epoch_id: int
) -> None:
    policy = config.decision_policy
    connection.execute(
        """
        INSERT INTO groundloop_decision_policy (
            policy_version, support_threshold, refute_threshold,
            tie_rule_version, valid_from_epoch
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (
            policy.policy_version,
            policy.support_threshold,
            policy.refute_threshold,
            policy.tie_rule_version,
            epoch_id,
        ),
    )


def _seed_event_database(
    connection: Connection[Any],
    *,
    history: VerifiedGitHistory,
    model_config: PinnedM3ReuseConfig,
    bundle: PinnedM3AdapterBundle,
    manifest: CandidatePolicyManifest,
    drafts: tuple[ChunkDraft, ...],
    selective_states: tuple[tuple[ClaimState, ...], tuple[AnswerState, ...]],
) -> tuple[int, int, str]:
    """Seed a disposable but relationally valid sealed M4 event envelope."""
    payload_hash = stable_m4_digest(
        "m4-10-git-event-v1",
        history.spec.history_id,
        history.spec.commit,
        history.spec.parent,
        history.spec.path,
        *(draft.text_hash for draft in drafts),
    )
    seed_row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (%s, %s, 1, 'committed', 'sealed', 'complete', 'strict', now())
        RETURNING epoch_id
        """,
        (
            f"m4-10-seed:{history.spec.history_id}",
            stable_m4_digest("seed", payload_hash),
        ),
    ).fetchone()
    event_id = f"m4-10-event:{history.spec.history_id}:{history.spec.commit[:12]}"
    event_row = connection.execute(
        """
        INSERT INTO groundloop_epoch (
            event_id, payload_hash, revision, structural_status,
            semantic_status, evaluation_state, publication_mode, sealed_at
        ) VALUES (%s, %s, 7, 'committed', 'sealed', 'complete', 'strict', now())
        RETURNING epoch_id
        """,
        (event_id, payload_hash),
    ).fetchone()
    if seed_row is None or event_row is None:
        raise ValidationError("failed to create disposable audit epochs")
    seed_epoch = int(seed_row[0])
    event_epoch = int(event_row[0])
    _insert_decision_policy(connection, model_config, seed_epoch)
    registry = PostgresM4ArtifactRegistry(connection)
    registry.register_model(bundle.embeddings.spec.model_artifact)
    registry.register_model(bundle.verifier.spec.model_artifact)
    registry.register_prompt(bundle.verifier.spec.prompt_artifact)

    document_id = f"git-document:{history.spec.history_id}"
    connection.execute(
        """INSERT INTO groundloop_document(document_id, source_uri, authority_class)
           VALUES (%s, %s, 'pinned-git-documentation')""",
        (document_id, history.spec.repository_url),
    )
    if history.spec.event_kind == "replace":
        old_version_id = f"git-parent-version:{history.spec.history_id}"
        assert history.spec.parent_content_sha256 is not None
        connection.execute(
            """
            INSERT INTO groundloop_document_version(
                document_version_id, document_id, content_hash,
                valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, %s, NULL)
            """,
            (
                old_version_id,
                document_id,
                history.spec.parent_content_sha256,
                seed_epoch,
            ),
        )
        for index, excerpt in enumerate(history.excerpts):
            assert (
                excerpt.parent_chunk_id is not None and excerpt.parent_text is not None
            )
            connection.execute(
                """
                INSERT INTO groundloop_chunk_version(
                    chunk_version_id, document_version_id, chunk_index, text,
                    text_hash, chunker_version, valid_from_epoch, valid_to_epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                """,
                (
                    excerpt.parent_chunk_id,
                    old_version_id,
                    index,
                    excerpt.parent_text,
                    normalized_text_hash(excerpt.parent_text),
                    _CHUNKER_ID,
                    seed_epoch,
                ),
            )
    new_version_id = f"git-child-version:{history.spec.history_id}"
    connection.execute(
        """
        INSERT INTO groundloop_document_version(
            document_version_id, document_id, content_hash,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, %s, %s, NULL)
        """,
        (
            new_version_id,
            document_id,
            history.spec.child_content_sha256,
            event_epoch,
        ),
    )
    for draft in drafts:
        connection.execute(
            """
            INSERT INTO groundloop_chunk_version(
                chunk_version_id, document_version_id, chunk_index, text,
                text_hash, chunker_version, valid_from_epoch, valid_to_epoch
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
            """,
            (
                draft.chunk_version_id,
                new_version_id,
                draft.chunk_index,
                draft.text,
                draft.text_hash,
                _CHUNKER_ID,
                event_epoch,
            ),
        )

    question_id = f"question:{history.spec.history_id}"
    with connection.transaction():
        connection.execute(
            "INSERT INTO groundloop_question VALUES (%s, %s, %s)",
            (
                question_id,
                f"What changed in {history.spec.path}?",
                seed_epoch,
            ),
        )
        connection.execute(
            """
            INSERT INTO groundloop_answer_version
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                history.spec.answer_id,
                question_id,
                "Frozen benchmark answer represented by atomic claims.",
                "manual-fixture",
                "m4-10",
                "manual-claim-construction-v1",
                seed_epoch,
            ),
        )
        for claim in history.spec.claims:
            connection.execute(
                """
                INSERT INTO groundloop_claim
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    claim.claim_id,
                    history.spec.answer_id,
                    claim.text,
                    "manual-fixture",
                    "m4-10",
                    "manual-claim-construction-v1",
                    claim.required,
                ),
            )

    runtime = PostgresM4RuntimeStore(connection)
    claim_ids = tuple(claim.claim_id for claim in history.spec.claims)
    runtime.register_claim_registry_snapshot(
        manifest.claim_registry_snapshot_id, claim_ids
    )
    runtime.register_candidate_policy(manifest)
    connection.execute(
        """
        INSERT INTO groundloop_m4_update (
            epoch_id, update_kind, candidate_policy_id,
            previous_published_epoch_id, registry_snapshot_id, manifest
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            event_epoch,
            history.spec.event_kind,
            manifest.policy_id,
            seed_epoch,
            manifest.claim_registry_snapshot_id,
            Jsonb(
                {
                    "schema": _SCHEMA_VERSION,
                    "history_id": history.spec.history_id,
                    "source_commit": history.spec.commit,
                }
            ),
        ),
    )
    if history.spec.event_kind == "replace":
        connection.execute(
            """
            INSERT INTO groundloop_m4_structural_deactivation
            VALUES (%s, %s)
            """,
            (event_epoch, f"git-parent-version:{history.spec.history_id}"),
        )

    claim_states, answer_states = selective_states
    for claim_state in claim_states:
        connection.execute(
            """
            INSERT INTO groundloop_published_claim_state (
                claim_id, valid_from_epoch, valid_to_epoch,
                support_count, refute_count, best_support_score,
                best_refute_score, supporting_observation_ids,
                refuting_observation_ids, status, certificate_digest
            ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                claim_state.claim_id,
                event_epoch,
                claim_state.support_count,
                claim_state.refute_count,
                claim_state.best_support_score,
                claim_state.best_refute_score,
                list(claim_state.supporting_observation_ids),
                list(claim_state.refuting_observation_ids),
                claim_state.status.value,
                stable_m4_digest(
                    "m4-10-certificate", claim_state.claim_id, str(event_epoch)
                ),
            ),
        )
    for answer_state in answer_states:
        connection.execute(
            """
            INSERT INTO groundloop_published_answer_state (
                answer_version_id, valid_from_epoch, valid_to_epoch,
                required_claim_count, supported_count, unsupported_count,
                refuted_count, conflicted_count, status
            ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s)
            """,
            (
                answer_state.answer_version_id,
                event_epoch,
                answer_state.required_claim_count,
                answer_state.supported_count,
                answer_state.unsupported_count,
                answer_state.refuted_count,
                answer_state.conflicted_count,
                answer_state.status.value,
            ),
        )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return seed_epoch, event_epoch, event_id


def _seed_claim_index(
    connection: Connection[Any],
    *,
    history: VerifiedGitHistory,
    manifest: CandidatePolicyManifest,
    claim_artifacts: tuple[Any, ...],
) -> LexicalRegistrySnapshot:
    registry = PostgresM4ArtifactRegistry(connection)
    registry.seed_claim_admission_index(
        manifest=manifest,
        artifacts=claim_artifacts,
        claim_texts={claim.claim_id: claim.text for claim in history.spec.claims},
    )
    analyzer = PostgresSimpleLexemeAnalyzer(
        connection, PostgresAdmissionServerIdentity.inspect(connection)
    )
    return LexicalRegistrySnapshot(
        snapshot_id=manifest.claim_registry_snapshot_id,
        claim_lexemes=tuple(
            (claim.claim_id, analyzer.analyze(claim.text))
            for claim in history.spec.claims
        ),
    )


def _status_projection(
    oracle: FrozenOracleEvent,
    states: tuple[tuple[ClaimState, ...], tuple[AnswerState, ...]],
) -> tuple[tuple[ObjectStatus, ...], tuple[ObjectStatus, ...]]:
    claims, answers = states
    claim_status = {state.claim_id: state.status.value for state in claims}
    answer_status = {state.answer_version_id: state.status.value for state in answers}
    return (
        tuple(
            ObjectStatus(item.object_id, claim_status[item.object_id])
            for item in oracle.claim_status_effects
        ),
        tuple(
            ObjectStatus(item.object_id, answer_status[item.object_id])
            for item in oracle.answer_status_effects
        ),
    )


def _lineage_pairs(history: VerifiedGitHistory) -> tuple[PairKey, ...]:
    child_by_extraction = {
        excerpt.extraction.extraction_id: excerpt.child_chunk_id
        for excerpt in history.excerpts
    }
    return tuple(
        sorted(
            PairKey(claim.claim_id, child_by_extraction[extraction_id])
            for claim in history.spec.claims
            for extraction_id in claim.prior_citation_extraction_ids
        )
    )


def _history_component_ids(history: VerifiedGitHistory) -> tuple[str, ...]:
    normalized_contents = {
        normalized_text_hash(claim.text) for claim in history.spec.claims
    }
    for excerpt in history.excerpts:
        normalized_contents.add(normalized_text_hash(excerpt.child_text))
        if excerpt.parent_text is not None:
            normalized_contents.add(normalized_text_hash(excerpt.parent_text))
    return tuple(
        sorted(
            {
                f"repo:{history.spec.repository_url}",
                f"path:{history.spec.repository_url}:{history.spec.path}",
                f"commit:{history.spec.repository_url}:{history.spec.commit}",
                (
                    "document-lineage:"
                    f"{history.spec.repository_url}:{history.spec.path}"
                ),
                *(
                    f"claim-family:{claim.claim_family_id}"
                    for claim in history.spec.claims
                ),
                *(
                    f"normalized-content:{content_hash}"
                    for content_hash in normalized_contents
                ),
            }
        )
    )


def _policies(definition: RealGitStudyDefinition) -> tuple[PolicySpec, ...]:
    details = {
        AblationKind.EXHAUSTIVE_REFRESH: "independent_snapshot_refresh_k",
        AblationKind.VECTOR_ONLY: "exact_reverse_bge_top_l_per_child_chunk",
        AblationKind.LEXICAL_ONLY: "postgresql_lexical_v1_top_l_per_child_chunk",
        AblationKind.UNION: definition.admission.fusion_rule,
        AblationKind.LINEAGE: definition.admission.lineage_rule,
        AblationKind.FRONTIER: definition.admission.frontier_rule,
        AblationKind.FRESH_FALLBACK: definition.admission.fresh_fallback_rule,
    }
    return tuple(
        sorted(
            (
                PolicySpec(
                    policy_id=f"m4-10-{kind.value}-v1",
                    kind=kind,
                    policy_hash=stable_m4_digest(
                        "m4-10-real-history-treatment-v1",
                        definition.definition_hash,
                        kind.value,
                        details[kind],
                    ),
                )
                for kind in AblationKind
            ),
            key=lambda policy: policy.policy_id,
        )
    )


def _fused_approximate_pairs(
    *,
    manifest: CandidatePolicyManifest,
    inserted_chunk_ids: tuple[str, ...],
    vector_hits: tuple[ChannelHit, ...],
    lexical_hits: tuple[ChannelHit, ...],
) -> tuple[PairKey, ...]:
    fused = fuse_admission_channels(
        inserted_chunk_ids=inserted_chunk_ids,
        manifest=manifest,
        vector_hits=vector_hits,
        lexical_hits=lexical_hits,
    )
    return tuple(sorted(item.pair for item in fused.admitted_pairs))


class _MeasuredVerifierBackend:
    """Count and time real MiniLM batch calls without changing model semantics."""

    def __init__(self, delegate: PinnedMiniLMVerifier) -> None:
        self._delegate = delegate
        self.model_artifact = delegate.model_artifact
        self.prompt_artifact = delegate.prompt_artifact
        self.calibration_version = delegate.calibration_version
        self.temperature = delegate.temperature
        self.max_length = delegate.max_length
        self.call_count = 0
        self.attempted_pair_count = 0
        self.completed_pair_count = 0
        self.batch_latencies_ms: list[float] = []

    def verify_batch(
        self, pairs: Sequence[tuple[AtomicClaim, ChunkDraft]]
    ) -> tuple[VerificationResult, ...]:
        self.call_count += 1
        self.attempted_pair_count += len(pairs)
        started = time.perf_counter_ns()
        results = self._delegate.verify_batch(pairs)
        self.batch_latencies_ms.append(
            (time.perf_counter_ns() - started) / 1_000_000.0
        )
        self.completed_pair_count += len(results)
        return results


@dataclass(frozen=True, slots=True)
class _ExecutedTreatment:
    artifacts: tuple[PairVerificationArtifact, ...]
    work: VerifierMeasurement
    verifier_latency_ms: float
    batch_latencies_ms: tuple[float, ...]


def _assert_same_operational_judgment(
    actual: PairVerificationArtifact,
    expected: PairVerificationArtifact,
) -> None:
    actual_result = actual.result
    expected_result = expected.result
    exact_actual = (
        actual.pair,
        actual.pair_input_hash,
        actual.execution_spec_hash,
        actual.decision_policy_version,
        actual.decision_policy_hash,
        actual.operational_label,
        actual_result.claim_id,
        actual_result.chunk_version_id,
        actual_result.candidate_id,
        actual_result.model_artifact_id,
        actual_result.prompt_artifact_id,
        actual_result.calibration_version,
        actual_result.temperature,
        actual_result.input_hash,
    )
    exact_expected = (
        expected.pair,
        expected.pair_input_hash,
        expected.execution_spec_hash,
        expected.decision_policy_version,
        expected.decision_policy_hash,
        expected.operational_label,
        expected_result.claim_id,
        expected_result.chunk_version_id,
        expected_result.candidate_id,
        expected_result.model_artifact_id,
        expected_result.prompt_artifact_id,
        expected_result.calibration_version,
        expected_result.temperature,
        expected_result.input_hash,
    )
    if exact_actual != exact_expected:
        raise ValidationError(
            "treatment verifier operational identity or label differs from the "
            "frozen exhaustive table"
        )
    actual_values = (
        actual_result.scores.support,
        actual_result.scores.refute,
        actual_result.scores.neutral,
        *(actual_result.raw_logits or ()),
    )
    expected_values = (
        expected_result.scores.support,
        expected_result.scores.refute,
        expected_result.scores.neutral,
        *(expected_result.raw_logits or ()),
    )
    if len(actual_values) != len(expected_values) or any(
        not math.isclose(actual_value, expected_value, rel_tol=1e-6, abs_tol=1e-6)
        for actual_value, expected_value in zip(
            actual_values, expected_values, strict=True
        )
    ):
        raise ValidationError(
            "treatment verifier scores differ materially from the frozen "
            "exhaustive table"
        )


def _build_treatment_verifier_backend(
    config: PinnedM3ReuseConfig,
    bundle: PinnedM3AdapterBundle,
) -> PinnedMiniLMVerifier:
    checkpoint = bundle.availability.verifier_checkpoint
    if checkpoint is None:
        raise ValidationError("treatment verifier checkpoint is unavailable")
    return PinnedMiniLMVerifier(
        model_path=str(checkpoint),
        model_revision=config.verifier_revision,
        temperature=config.calibration_temperature,
        max_length=config.verifier_max_length,
        batch_size=config.verifier_batch_size,
        local_files_only=True,
        artifact_sha256=config.verifier_checkpoint_tree_sha256,
        logical_model_id=config.verifier_logical_model_id,
        calibration_version=config.calibration_version,
    )


def _execute_treatment(
    *,
    backend: PinnedMiniLMVerifier,
    bundle: PinnedM3AdapterBundle,
    inputs: tuple[PairVerificationInput, ...],
    expected_artifacts: tuple[PairVerificationArtifact, ...],
    token_counts: Mapping[PairKey, int],
    batch_size: int,
) -> _ExecutedTreatment:
    measured_backend = _MeasuredVerifierBackend(backend)
    verifier = M4CalibratedVerifierAdapter(
        measured_backend,
        bundle.verifier.spec,
        batch_size=batch_size,
    )
    started = time.perf_counter_ns()
    artifacts = verifier.verify_pairs(inputs)
    verifier_latency_ms = (time.perf_counter_ns() - started) / 1_000_000.0
    if len(artifacts) != len(expected_artifacts):
        raise ValidationError("treatment verifier returned the wrong artifact count")
    for artifact, expected in zip(artifacts, expected_artifacts, strict=True):
        _assert_same_operational_judgment(artifact, expected)
    if (
        measured_backend.attempted_pair_count != len(inputs)
        or measured_backend.completed_pair_count != len(inputs)
        or measured_backend.call_count != len(measured_backend.batch_latencies_ms)
    ):
        raise ValidationError("treatment verifier measurement is incomplete")
    return _ExecutedTreatment(
        artifacts=artifacts,
        work=VerifierMeasurement(
            attempted_pair_count=measured_backend.attempted_pair_count,
            completed_pair_count=measured_backend.completed_pair_count,
            failed_pair_count=0,
            timeout_pair_count=0,
            call_count=measured_backend.call_count,
            input_tokens=sum(token_counts[item.pair] for item in inputs),
            output_tokens=None,
            retrieval_latency_ms=None,
            verifier_latency_ms=None,
            end_to_end_latency_ms=None,
        ),
        verifier_latency_ms=verifier_latency_ms,
        batch_latencies_ms=tuple(measured_backend.batch_latencies_ms),
    )


class _ExplodingJudge:
    def judge_pairs(self, pairs: tuple[PairKey, ...]) -> NoReturn:
        raise AssertionError(
            f"PersistedEventAudit replay invoked model judge: {pairs!r}"
        )


def _persist_oracle_event(
    connection: Connection[Any],
    *,
    definition: RealGitStudyDefinition,
    history: VerifiedGitHistory,
    model_config: PinnedM3ReuseConfig,
    bundle: PinnedM3AdapterBundle,
    manifest: CandidatePolicyManifest,
    drafts: tuple[ChunkDraft, ...],
    refresh_claims: tuple[RefreshClaim, ...],
    judgments: tuple[PairJudgment, ...],
    union_pairs: tuple[PairKey, ...],
    claim_artifacts: tuple[Any, ...],
) -> tuple[
    PersistedEventAudit,
    SnapshotRefreshResult,
    FrozenOracleEvent,
    Mapping[str, object],
]:
    judgment_by_pair = {judgment.pair: judgment for judgment in judgments}
    union_judgments = tuple(judgment_by_pair[pair] for pair in union_pairs)
    working_states = _states(history, drafts, ())
    selective_states = _states(history, drafts, union_judgments)
    _seed_event_database(
        connection,
        history=history,
        model_config=model_config,
        bundle=bundle,
        manifest=manifest,
        drafts=drafts,
        selective_states=selective_states,
    )
    _seed_claim_index(
        connection,
        history=history,
        manifest=manifest,
        claim_artifacts=claim_artifacts,
    )
    event_id = f"m4-10-event:{history.spec.history_id}:{history.spec.commit[:12]}"
    chunks = tuple(
        RefreshChunk(draft.chunk_version_id, draft.text_hash, chunk.vector.vector)
        for draft, chunk in zip(
            drafts,
            sorted(
                bundle.embeddings.embed_chunks(drafts),
                key=lambda artifact: artifact.vector.chunk_version_id,
            ),
            strict=True,
        )
    )
    snapshot_hash = stable_m4_digest(
        "m4-10-active-snapshot-v1",
        history.spec.history_id,
        history.spec.commit,
        *(chunk.chunk_version_id for chunk in chunks),
    )
    judge = DeterministicJudgmentTable(judgments)
    refresh = run_snapshot_refresh(
        corpus_snapshot_hash=snapshot_hash,
        refresh_policy_id="m4-10-independent-snapshot-refresh-k-v1",
        depth_k=definition.admission.snapshot_refresh_depth,
        claims=refresh_claims,
        active_chunks=chunks,
        judge=judge,
    )
    audit_spec = EventAuditSpec(
        event_id=event_id,
        treatment_manifest_id=stable_m4_digest(
            "m4-10-union-treatment",
            history.spec.history_id,
            *(f"{pair.claim_id}:{pair.chunk_version_id}" for pair in union_pairs),
        ),
        split_id=definition.split_id,
        audit_judge_identity=(bundle.verifier.spec.execution_spec_hash),
        refresh_judge_identity=(bundle.verifier.spec.execution_spec_hash),
        corpus_snapshot_hash=snapshot_hash,
        refresh_policy_id=refresh.refresh_policy_id,
        refresh_depth_k=refresh.depth_k,
        claims=refresh_claims,
        active_chunks_after=chunks,
        inserted_active_chunk_ids=tuple(draft.chunk_version_id for draft in drafts),
        surviving_judgments=(),
        working_claim_states=working_states[0],
        working_answer_states=working_states[1],
        selective_admitted_pairs=union_pairs,
        selective_judgments=union_judgments,
        selective_claim_states=selective_states[0],
        selective_answer_states=selective_states[1],
    )
    persisted = run_and_persist_event_audit(
        connection,
        spec=audit_spec,
        audit_judge=judge,
        refresh_judge=judge,
    )
    replay = run_and_persist_event_audit(
        connection,
        spec=audit_spec,
        audit_judge=_ExplodingJudge(),
        refresh_judge=_ExplodingJudge(),
    )
    if persisted.disposition is not EventAuditDisposition.CREATED:
        raise ValidationError("fresh disposable event audit did not create a row")
    if replay.disposition is not EventAuditDisposition.REPLAYED:
        raise ValidationError("persisted event audit did not exact-replay")
    row = connection.execute(
        """
        SELECT generic.manifest, typed.evaluation_run_id, typed.epoch_id,
               typed.event_id, typed.input_hash, typed.result_hash,
               typed.expected_pair_count, typed.positive_pair_count,
               typed.missed_positive_pair_count,
               typed.selective_verifier_pair_count
        FROM groundloop_impact_evaluation_run AS generic
        JOIN groundloop_m4_event_audit_run AS typed USING (evaluation_run_id)
        WHERE typed.evaluation_run_id = %s
        """,
        (persisted.evaluation_run_id,),
    ).fetchone()
    if row is None or not isinstance(row[0], dict):
        raise ValidationError("persisted event-audit raw row is absent")
    raw_manifest = cast(dict[str, object], row[0])
    oracle = FrozenOracleEvent.from_persisted_manifest(
        audit=persisted,
        refresh=refresh,
        persisted_manifest=raw_manifest,
        history_id=history.spec.history_id,
        event_index=0,
        event_type=history.spec.event_kind,
        event_manifest_hash=stable_m4_digest(
            "m4-10-git-event-manifest-v1",
            history.spec.history_id,
            history.spec.commit,
            history.spec.parent,
            history.spec.child_content_sha256,
        ),
    )
    raw_row: Mapping[str, object] = {
        "history_id": history.spec.history_id,
        "evaluation_run_id": str(row[1]),
        "epoch_id": int(row[2]),
        "event_id": str(row[3]),
        "input_hash": str(row[4]).strip(),
        "result_hash": str(row[5]).strip(),
        "expected_pair_count": int(row[6]),
        "positive_pair_count": int(row[7]),
        "missed_positive_pair_count": int(row[8]),
        "selective_verifier_pair_count": int(row[9]),
        "created_then_zero_model_replayed": True,
        "manifest": raw_manifest,
    }
    return persisted, refresh, oracle, raw_row


def _token_counts(
    *,
    checkpoint: Path,
    inputs: tuple[PairVerificationInput, ...],
    max_length: int,
) -> dict[PairKey, int]:
    try:
        transformers = importlib.import_module("transformers")
    except ImportError as error:
        raise ValidationError(
            "real-history tokenizer telemetry needs transformers"
        ) from error
    tokenizer: Any = transformers.AutoTokenizer.from_pretrained(
        checkpoint, local_files_only=True
    )
    result: dict[PairKey, int] = {}
    for item in inputs:
        encoded = tokenizer(
            item.chunk_text,
            item.claim_text,
            add_special_tokens=True,
            truncation=False,
        )
        count = len(encoded["input_ids"])
        if count < 0:
            raise ValidationError("tokenizer produced a negative token count")
        result[item.pair] = count
    if any(count > max_length for count in result.values()):
        # This is telemetry, not a rejection. The verifier records truncation.
        pass
    return result


def _artifact_payload(
    artifact: PairVerificationArtifact,
    pair_input: PairVerificationInput,
    token_count: int,
) -> dict[str, object]:
    result = artifact.result
    raw_input: dict[str, object] = {
        "claim_id": pair_input.pair.claim_id,
        "chunk_version_id": pair_input.pair.chunk_version_id,
        "claim_text": pair_input.claim_text,
        "chunk_text": pair_input.chunk_text,
        "pair_input_hash": pair_input.input_hash,
        "verifier_input_hash": result.input_hash,
        "token_count_untruncated": token_count,
    }
    raw_output: dict[str, object] = {
        "artifact_id": artifact.artifact_id,
        "raw_output_hash": result.raw_output_hash,
        "raw_logits": list(result.raw_logits or ()),
        "scores": {
            "support": result.scores.support,
            "refute": result.scores.refute,
            "neutral": result.scores.neutral,
        },
        "operational_label": artifact.operational_label.value,
        "decision_policy_version": artifact.decision_policy_version,
        "decision_policy_hash": artifact.decision_policy_hash,
        "execution_spec_hash": artifact.execution_spec_hash,
    }
    return {
        "pair": {
            "claim_id": artifact.pair.claim_id,
            "chunk_version_id": artifact.pair.chunk_version_id,
        },
        "raw_input": raw_input,
        "raw_input_sha256": _json_hash(raw_input),
        "raw_output": raw_output,
        "raw_output_sha256": _json_hash(raw_output),
    }


def _embedding_payload(
    history_id: str, provenance: RoleEmbeddingProvenance
) -> dict[str, object]:
    return {
        "history_id": history_id,
        "subject_id": provenance.subject_id,
        "role": provenance.role.value,
        "artifact_id": provenance.artifact_id,
        "model_artifact_id": provenance.model_artifact_id,
        "model_id": provenance.model_id,
        "model_revision": provenance.model_revision,
        "tokenizer_revision": provenance.tokenizer_revision,
        "role_template_hash": provenance.role_template_hash,
        "input_hash": provenance.input_hash,
        "vector_hash": provenance.vector_hash,
        "adapter_spec_hash": provenance.adapter_spec_hash,
        "token_count": provenance.token_count,
        "max_tokens": provenance.max_tokens,
        "truncated": provenance.truncated,
    }


def _write_json(path: Path, value: object) -> str:
    payload = (_canonical_json(value) + "\n").encode("utf-8")
    path.write_bytes(payload)
    return _sha256(payload)


def _write_jsonl(path: Path, rows: tuple[Mapping[str, object], ...]) -> str:
    payload = "".join(_canonical_json(row) + "\n" for row in rows).encode("utf-8")
    path.write_bytes(payload)
    return _sha256(payload)


def run_real_history_study(
    config: RealHistoryStudyRunConfig,
) -> RealHistoryStudyResult:
    """Run all three real histories and write one reproducible output bundle."""
    definition = load_real_git_study_definition(config.resolved_definition_path)
    histories = verify_git_histories(definition, config.source_roots)
    model_config = PinnedM3ReuseConfig.load(config.resolved_model_config_path)
    bundle = build_pinned_m3_adapters(model_config, artifact_root=config.artifact_root)
    treatment_backend = _build_treatment_verifier_backend(model_config, bundle)
    output = config.output_directory
    output.mkdir(parents=True, exist_ok=True)
    (output / "failures_timeouts.jsonl").unlink(missing_ok=True)

    policies = _policies(definition)
    policy_by_kind = {policy.kind: policy for policy in policies}
    oracle_events: list[FrozenOracleEvent] = []
    treatments: list[TreatmentEvent] = []
    source_rows: list[Mapping[str, object]] = []
    runtime_source_rows: list[Mapping[str, object]] = []
    artifact_rows: list[Mapping[str, object]] = []
    embedding_rows: list[Mapping[str, object]] = []
    admission_query_rows: list[Mapping[str, object]] = []
    admission_rows: list[Mapping[str, object]] = []
    event_rows: list[Mapping[str, object]] = []
    timing_rows: list[Mapping[str, object]] = []
    actual_oracle_pair_executions = 0
    actual_oracle_batch_executions = 0
    actual_treatment_pair_executions = 0
    actual_treatment_batch_executions = 0

    for history in histories:
        claim_inputs = tuple(
            ClaimEmbeddingInput(claim.claim_id, claim.text)
            for claim in history.spec.claims
        )
        started = time.perf_counter_ns()
        claim_artifacts = bundle.embeddings.embed_claims(claim_inputs)
        claim_embedding_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        drafts = _child_drafts(history)
        started = time.perf_counter_ns()
        chunk_artifacts = bundle.embeddings.embed_chunks(drafts)
        chunk_embedding_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        claim_vectors = {
            artifact.vector.claim_id: artifact.vector.vector
            for artifact in claim_artifacts
        }
        embedding_rows.extend(
            _embedding_payload(history.spec.history_id, artifact.provenance)
            for artifact in claim_artifacts
        )
        embedding_rows.extend(
            _embedding_payload(history.spec.history_id, artifact.provenance)
            for artifact in chunk_artifacts
        )
        refresh_claims = _refresh_claims(history, claim_vectors)
        pair_inputs = _pair_inputs(history, drafts)
        token_counts = _token_counts(
            checkpoint=(
                config.artifact_root / model_config.verifier_checkpoint_relative_path
            ),
            inputs=pair_inputs,
            max_length=model_config.verifier_max_length,
        )
        started = time.perf_counter_ns()
        artifacts = bundle.verifier.verify_pairs(pair_inputs)
        oracle_verifier_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        actual_oracle_pair_executions += len(pair_inputs)
        actual_oracle_batch_executions += math.ceil(
            len(pair_inputs) / model_config.verifier_batch_size
        )
        judgments = tuple(
            artifact.to_pair_judgment(split_id=definition.split_id)
            for artifact in artifacts
        )
        artifact_by_pair = {artifact.pair: artifact for artifact in artifacts}
        input_by_pair = {item.pair: item for item in pair_inputs}
        artifact_rows.extend(
            _artifact_payload(
                artifact,
                input_by_pair[artifact.pair],
                token_counts[artifact.pair],
            )
            for artifact in artifacts
        )

        exact_index = ExactReverseVectorIndex(
            tuple(artifact.vector for artifact in claim_artifacts)
        )
        event_id = f"m4-10-event:{history.spec.history_id}:{history.spec.commit[:12]}"

        schema_name = f"groundloop_m4_10_real_{uuid.uuid4().hex}"
        admin = psycopg.connect(_psycopg_url(config.database_url), autocommit=True)
        try:
            _create_schema(admin, schema_name)
            server = PostgresAdmissionServerIdentity.inspect(admin)
            manifest = _policy_manifest(
                history=history,
                definition=definition,
                bundle=bundle,
                server=server,
                repo_root=config.repo_root,
            )
            started = time.perf_counter_ns()
            vector_searches = tuple(
                exact_index.search(
                    epoch_id=1,
                    candidate_policy_id=manifest.policy_id,
                    chunk=chunk.vector,
                    limit=definition.admission.vector_depth,
                )
                for chunk in chunk_artifacts
            )
            vector_hits = tuple(
                hit for search in vector_searches for hit in search.hits
            )
            vector_latency_ms = (time.perf_counter_ns() - started) / 1_000_000.0
            vector_pairs = tuple(sorted({hit.pair for hit in vector_hits}))
            admission_query_rows.extend(
                {
                    "history_id": history.spec.history_id,
                    "event_id": event_id,
                    "channel": "vector",
                    "chunk_version_id": chunk.vector.chunk_version_id,
                    "query_artifact_hash": search.query_artifact_hash,
                    "chunk_input_hash": chunk.vector.input_hash,
                    "requested_limit": definition.admission.vector_depth,
                    "returned_count": len(search.hits),
                    "indexed_claim_count": exact_index.claim_count,
                    "policy_id": manifest.policy_id,
                    "policy_hash": manifest.policy_hash,
                    "claim_registry_snapshot_id": (
                        manifest.claim_registry_snapshot_id
                    ),
                    "index_kind": "exact",
                    "score_semantics": "dot_l2_normalized_bge_role_vectors",
                }
                for chunk, search in zip(
                    chunk_artifacts, vector_searches, strict=True
                )
            )
            # Seeding the audit envelope needs the selected union state. Lexical
            # retrieval needs the same persisted claim registry, so seed base
            # objects first using a temporary empty selection and repair by
            # recreating the disposable schema after scoring.
            empty_states = _states(history, drafts, ())
            _seed_event_database(
                admin,
                history=history,
                model_config=model_config,
                bundle=bundle,
                manifest=manifest,
                drafts=drafts,
                selective_states=empty_states,
            )
            lexical_registry = _seed_claim_index(
                admin,
                history=history,
                manifest=manifest,
                claim_artifacts=claim_artifacts,
            )
            lexical_config = load_frozen_lexical_v1(config.repo_root)
            analyzer = PostgresSimpleLexemeAnalyzer(admin, server)
            backend = PostgresLexicalSearchBackend(
                connection=admin,
                server=server,
                manifest=manifest,
            )
            backend.validate_registry(lexical_registry)
            lexical_policy = LexicalV1Policy(
                config=lexical_config,
                registry=lexical_registry,
                analyzer=analyzer,
                backend=backend,
            )
            lexical_queries = {
                draft.chunk_version_id: lexical_policy.prepare_query(draft.text)
                for draft in drafts
            }
            started = time.perf_counter_ns()
            lexical_searches = {
                draft.chunk_version_id: lexical_policy.search(
                    epoch_id=1,
                    manifest=manifest,
                    chunk_version_id=draft.chunk_version_id,
                    chunk_text=draft.text,
                )[: definition.admission.lexical_depth]
                for draft in drafts
            }
            lexical_hits = tuple(
                hit
                for draft in drafts
                for hit in lexical_searches[draft.chunk_version_id]
            )
            lexical_latency_ms = (time.perf_counter_ns() - started) / 1_000_000.0
            lexical_pairs = tuple(sorted({hit.pair for hit in lexical_hits}))
            admission_query_rows.extend(
                {
                    "history_id": history.spec.history_id,
                    "event_id": event_id,
                    "channel": "lexical",
                    "chunk_version_id": draft.chunk_version_id,
                    "query_artifact_hash": lexical_queries[
                        draft.chunk_version_id
                    ].query_artifact_hash,
                    "query_lexemes": list(
                        lexical_queries[draft.chunk_version_id].lexemes
                    ),
                    "idf_by_lexeme": [
                        {"lexeme": lexeme, "idf": idf}
                        for lexeme, idf in lexical_queries[
                            draft.chunk_version_id
                        ].idf_by_lexeme
                    ],
                    "requested_limit": definition.admission.lexical_depth,
                    "policy_execution_limit": (
                        manifest.approximate_cap_per_inserted_chunk
                    ),
                    "returned_count": len(lexical_searches[draft.chunk_version_id]),
                    "indexed_claim_count": lexical_registry.claim_count,
                    "policy_id": manifest.policy_id,
                    "policy_hash": manifest.policy_hash,
                    "lexical_config_hash": manifest.lexical_config_hash,
                    "claim_registry_snapshot_id": (manifest.claim_registry_snapshot_id),
                    "analyzer_artifact_id": analyzer.artifact_id,
                    "backend_artifact_id": backend.artifact_id,
                    "postgres_server_artifact_hash": server.artifact_hash,
                    "postgres_version": server.postgres_version,
                    "pgvector_version": server.pgvector_version,
                    "regconfig_identity": server.regconfig_identity,
                    "ranking_semantics": "ts_rank_cd_normalization_32",
                }
                for draft in drafts
            )
            admission_rows.extend(
                {
                    "history_id": history.spec.history_id,
                    "event_id": event_id,
                    "channel": hit.channel.value,
                    "claim_id": hit.pair.claim_id,
                    "chunk_version_id": hit.pair.chunk_version_id,
                    "rank": hit.rank,
                    "score": hit.score,
                    "policy_id": hit.candidate_policy_id,
                    "policy_hash": manifest.policy_hash,
                    "claim_registry_snapshot_id": (
                        manifest.claim_registry_snapshot_id
                    ),
                    "channel_artifact_hash": hit.channel_artifact_hash,
                    "query_artifact_hash": lexical_queries[
                        hit.pair.chunk_version_id
                    ].query_artifact_hash,
                    "postgres_server_artifact_hash": server.artifact_hash,
                    "postgres_version": server.postgres_version,
                    "pgvector_version": server.pgvector_version,
                    "regconfig_identity": server.regconfig_identity,
                    "query_lexemes": list(
                        lexical_queries[hit.pair.chunk_version_id].lexemes
                    ),
                }
                for hit in lexical_hits
            )
        finally:
            if not admin.closed:
                _drop_schema(admin, schema_name)
                admin.close()

        vector_query_by_chunk = {
            search.hits[0].pair.chunk_version_id: search.query_artifact_hash
            for search in vector_searches
            if search.hits
        }
        admission_rows.extend(
            {
                "history_id": history.spec.history_id,
                "event_id": event_id,
                "channel": hit.channel.value,
                "claim_id": hit.pair.claim_id,
                "chunk_version_id": hit.pair.chunk_version_id,
                "rank": hit.rank,
                "score": hit.score,
                "policy_id": hit.candidate_policy_id,
                "policy_hash": manifest.policy_hash,
                "claim_registry_snapshot_id": manifest.claim_registry_snapshot_id,
                "channel_artifact_hash": hit.channel_artifact_hash,
                "query_artifact_hash": vector_query_by_chunk[hit.pair.chunk_version_id],
                "score_semantics": "dot_l2_normalized_bge_role_vectors",
            }
            for hit in vector_hits
        )

        union_pairs = _fused_approximate_pairs(
            manifest=manifest,
            inserted_chunk_ids=tuple(
                sorted(draft.chunk_version_id for draft in drafts)
            ),
            vector_hits=vector_hits,
            lexical_hits=lexical_hits,
        )
        lineage_pairs = tuple(sorted(set(union_pairs) | set(_lineage_pairs(history))))
        # M3-imported claims have no invented reserve. In this fixture every
        # withdrawn explicit citation is immediately covered by mandatory
        # replacement lineage, so neither frontier nor fresh fallback adds a
        # pair. The equality is a measured negative result, not fabricated work.
        frontier_pairs = lineage_pairs
        fresh_pairs = frontier_pairs

        schema_name = f"groundloop_m4_10_audit_{uuid.uuid4().hex}"
        audit_connection = psycopg.connect(
            _psycopg_url(config.database_url), autocommit=True
        )
        try:
            _create_schema(audit_connection, schema_name)
            server = PostgresAdmissionServerIdentity.inspect(audit_connection)
            audit_manifest = _policy_manifest(
                history=history,
                definition=definition,
                bundle=bundle,
                server=server,
                repo_root=config.repo_root,
            )
            if audit_manifest != manifest:
                raise ValidationError(
                    "candidate policy changed between admission and persisted audit"
                )
            persisted, refresh, oracle, raw_event = _persist_oracle_event(
                audit_connection,
                definition=definition,
                history=history,
                model_config=model_config,
                bundle=bundle,
                manifest=audit_manifest,
                drafts=drafts,
                refresh_claims=refresh_claims,
                judgments=judgments,
                union_pairs=union_pairs,
                claim_artifacts=claim_artifacts,
            )
            event_rows.append(raw_event)
            if persisted.expected_pair_count != len(pair_inputs):
                raise ValidationError("persisted audit omitted a full-pair judgment")
        finally:
            if not audit_connection.closed:
                _drop_schema(audit_connection, schema_name)
                audit_connection.close()
        oracle_events.append(oracle)

        pair_sets = {
            AblationKind.EXHAUSTIVE_REFRESH: refresh.retrieved_pairs,
            AblationKind.VECTOR_ONLY: vector_pairs,
            AblationKind.LEXICAL_ONLY: lexical_pairs,
            AblationKind.UNION: union_pairs,
            AblationKind.LINEAGE: lineage_pairs,
            AblationKind.FRONTIER: frontier_pairs,
            AblationKind.FRESH_FALLBACK: fresh_pairs,
        }
        treatment_timing_details: list[Mapping[str, object]] = []
        for kind in AblationKind:
            pairs = tuple(sorted(pair_sets[kind]))
            execution = _execute_treatment(
                backend=treatment_backend,
                bundle=bundle,
                inputs=tuple(input_by_pair[pair] for pair in pairs),
                expected_artifacts=tuple(artifact_by_pair[pair] for pair in pairs),
                token_counts=token_counts,
                batch_size=model_config.verifier_batch_size,
            )
            selected_judgments = tuple(
                artifact.to_pair_judgment(split_id=definition.split_id)
                for artifact in execution.artifacts
            )
            projected = _status_projection(
                oracle, _states(history, drafts, selected_judgments)
            )
            policy = policy_by_kind[kind]
            actual_treatment_pair_executions += execution.work.completed_pair_count
            actual_treatment_batch_executions += execution.work.call_count
            treatment_timing_details.append(
                {
                    "policy_id": policy.policy_id,
                    "event_id": oracle.event_id,
                    "attempted_pair_count": execution.work.attempted_pair_count,
                    "completed_pair_count": execution.work.completed_pair_count,
                    "batch_call_count": execution.work.call_count,
                    "verifier_latency_ms": execution.verifier_latency_ms,
                    "batch_latencies_ms": list(execution.batch_latencies_ms),
                    "artifact_ids": [
                        artifact.artifact_id for artifact in execution.artifacts
                    ],
                    "raw_output_hashes": [
                        artifact.result.raw_output_hash
                        for artifact in execution.artifacts
                    ],
                    "timeout_deadline_enforced": False,
                    "timeout_outcome_count": 0,
                }
            )
            treatments.append(
                TreatmentEvent(
                    policy_id=policy.policy_id,
                    event_id=oracle.event_id,
                    treatment_manifest_hash=stable_m4_digest(
                        "m4-10-treatment-output-v1",
                        policy.policy_hash,
                        oracle.event_manifest_hash,
                        *(f"{pair.claim_id}:{pair.chunk_version_id}" for pair in pairs),
                    ),
                    admitted_pairs=pairs,
                    claim_post_statuses=projected[0],
                    answer_post_statuses=projected[1],
                    work=execution.work,
                )
            )
        source_rows.append(
            {
                "history_id": history.spec.history_id,
                "source_identity": {
                    "repository_url": history.spec.repository_url,
                    "commit": history.spec.commit,
                    "parent": history.spec.parent,
                    "path": history.spec.path,
                    "diff_sha256": history.spec.diff_sha256,
                    "parent_blob_oid": history.spec.parent_blob_oid,
                    "parent_content_sha256": history.spec.parent_content_sha256,
                    "child_blob_oid": history.spec.child_blob_oid,
                    "child_content_sha256": history.spec.child_content_sha256,
                },
                "git_change_status": history.change_status,
                "extractions": [
                    {
                        "extraction_id": excerpt.extraction.extraction_id,
                        "parent_excerpt_sha256": (
                            excerpt.extraction.parent_excerpt_sha256
                        ),
                        "child_excerpt_sha256": (
                            excerpt.extraction.child_excerpt_sha256
                        ),
                        "parent_chunk_id": excerpt.parent_chunk_id,
                        "child_chunk_id": excerpt.child_chunk_id,
                    }
                    for excerpt in history.excerpts
                ],
                "claim_construction": [
                    {
                        "claim_id": claim.claim_id,
                        "text_sha256": sha256_text(claim.text),
                        "required": claim.required,
                        "claim_family_id": claim.claim_family_id,
                        "prior_citation_extraction_ids": list(
                            claim.prior_citation_extraction_ids
                        ),
                        "construction_note": claim.construction_note,
                    }
                    for claim in history.spec.claims
                ],
            }
        )
        runtime_source_rows.append(
            {
                "history_id": history.spec.history_id,
                "runtime_location": str(history.runtime_root),
                "runtime_remote_name": history.source_remote_name,
                "matched_pinned_repository_url": True,
            }
        )
        timing_rows.append(
            {
                "history_id": history.spec.history_id,
                "claim_embedding_latency_ms": claim_embedding_ms,
                "chunk_embedding_latency_ms": chunk_embedding_ms,
                "vector_ranking_latency_ms": vector_latency_ms,
                "postgres_lexical_latency_ms": lexical_latency_ms,
                "oracle_verifier_latency_ms": oracle_verifier_ms,
                "treatment_verifier_executions": treatment_timing_details,
                "classifier_output_tokens": None,
                "classifier_output_tokens_null_reason": (
                    "three-way classifier emits logits and does not generate tokens"
                ),
                "treatment_work_latency_location": (
                    "run-varying per-treatment and per-batch latencies are stored in "
                    "this timing sidecar, outside deterministic empirical manifests"
                ),
                "timeout_policy": (
                    "no deadline was enforced; zero denotes observed timeout outcomes, "
                    "not a deadline-qualified timeout rate"
                ),
                "first_history_includes_lazy_model_load": (
                    history.spec.history_id == histories[0].spec.history_id
                ),
            }
        )

    assignments = tuple(
        HistoryAssignment(
            history_id=history.spec.history_id,
            split_id=definition.split_id,
            component_ids=_history_component_ids(history),
        )
        for history in histories
    )
    spec = EmpiricalStudySpec(
        schema_version="m4-empirical-study-v1",
        study_id=definition.study_id,
        dataset_version=(f"git-blobs:{definition.definition_hash}"),
        selected_split_id=definition.split_id,
        measurement_source=(
            "pinned_git_blobs_real_bge_real_calibrated_minilm_postgresql_lexical_v1"
        ),
        verifier_identity=bundle.verifier.spec.execution_spec_hash,
        decision_policy_id=model_config.decision_policy.policy_version,
        histories=assignments,
        policies=policies,
        oracle_events=tuple(sorted(oracle_events, key=lambda event: event.history_id)),
        treatments=tuple(treatments),
        bootstrap_seed=config.bootstrap_seed,
        bootstrap_replicates=config.bootstrap_replicates,
    )
    report = evaluate_frozen_history(spec)
    empirical_directory = output / "empirical"
    empirical_bundle = write_empirical_bundle(report, empirical_directory)

    source_rows_tuple = tuple(source_rows)
    artifact_rows_tuple = tuple(
        sorted(
            artifact_rows,
            key=lambda row: (
                cast(dict[str, object], row["pair"])["claim_id"],
                cast(dict[str, object], row["pair"])["chunk_version_id"],
            ),
        )
    )
    embedding_rows_tuple = tuple(
        sorted(
            embedding_rows,
            key=lambda row: (
                str(row["history_id"]),
                str(row["role"]),
                str(row["subject_id"]),
            ),
        )
    )
    admission_query_rows_tuple = tuple(
        sorted(
            admission_query_rows,
            key=lambda row: (
                str(row["history_id"]),
                str(row["channel"]),
                str(row["chunk_version_id"]),
            ),
        )
    )
    admission_rows_tuple = tuple(
        sorted(
            admission_rows,
            key=lambda row: (
                str(row["history_id"]),
                str(row["channel"]),
                str(row["chunk_version_id"]),
                int(cast(int, row["rank"])),
                str(row["claim_id"]),
            ),
        )
    )
    event_rows_tuple = tuple(sorted(event_rows, key=lambda row: str(row["history_id"])))
    timing_rows_tuple = tuple(
        sorted(timing_rows, key=lambda row: str(row["history_id"]))
    )
    runtime_source_rows_tuple = tuple(
        sorted(runtime_source_rows, key=lambda row: str(row["history_id"]))
    )
    file_hashes = {
        "source_manifest.json": _write_json(
            output / "source_manifest.json", source_rows_tuple
        ),
        "oracle_artifacts.jsonl": _write_jsonl(
            output / "oracle_artifacts.jsonl", artifact_rows_tuple
        ),
        "embedding_artifacts.jsonl": _write_jsonl(
            output / "embedding_artifacts.jsonl", embedding_rows_tuple
        ),
        "admission_queries.jsonl": _write_jsonl(
            output / "admission_queries.jsonl", admission_query_rows_tuple
        ),
        "admission_channel_hits.jsonl": _write_jsonl(
            output / "admission_channel_hits.jsonl", admission_rows_tuple
        ),
        "persisted_event_rows.jsonl": _write_jsonl(
            output / "persisted_event_rows.jsonl", event_rows_tuple
        ),
        "timings.json": _write_json(output / "timings.json", timing_rows_tuple),
        "runtime_sources.json": _write_json(
            output / "runtime_sources.json", runtime_source_rows_tuple
        ),
    }
    structural_payload: dict[str, object] = {
        "schema_version": _RESULT_SCHEMA,
        "study_definition_sha256": definition.definition_hash,
        "empirical_study_manifest_hash": empirical_bundle.study_manifest_hash,
        "empirical_report_hash": empirical_bundle.report_hash,
        "empirical_bundle_manifest_hash": empirical_bundle.bundle_manifest_hash,
        "source_manifest_sha256": file_hashes["source_manifest.json"],
        "oracle_artifacts_sha256": file_hashes["oracle_artifacts.jsonl"],
        "embedding_artifacts_sha256": file_hashes["embedding_artifacts.jsonl"],
        "admission_queries_sha256": file_hashes["admission_queries.jsonl"],
        "admission_channel_hits_sha256": file_hashes["admission_channel_hits.jsonl"],
        "persisted_event_rows_sha256": file_hashes["persisted_event_rows.jsonl"],
        "history_count": len(histories),
        "source_repository_count": len({h.spec.repository_url for h in histories}),
        "event_count": len(oracle_events),
        "treatment_count": len(treatments),
        "actual_oracle_model_pair_executions": actual_oracle_pair_executions,
        "actual_oracle_model_batch_executions": actual_oracle_batch_executions,
        "actual_treatment_model_pair_executions": actual_treatment_pair_executions,
        "actual_treatment_model_batch_executions": actual_treatment_batch_executions,
        "treatment_verifier_measurement_kind": "observed_real_execution",
        "timeout_deadline_enforced": False,
        "embedding_artifact_count": len(embedding_rows_tuple),
        "admission_query_count": len(admission_query_rows_tuple),
        "admission_zero_hit_query_count": sum(
            int(row["returned_count"] == 0) for row in admission_query_rows_tuple
        ),
        "admission_channel_hit_count": len(admission_rows_tuple),
        "persisted_event_audit_count": len(event_rows),
        "persisted_event_audit_exact_replay_count": len(event_rows),
        "model_identity": {
            "embedding_model_id": model_config.embedding_model_id,
            "embedding_revision": model_config.embedding_revision,
            "embedding_tree_sha256": model_config.embedding_snapshot_tree_sha256,
            "verifier_logical_model_id": model_config.verifier_logical_model_id,
            "verifier_revision": model_config.verifier_revision,
            "verifier_tree_sha256": model_config.verifier_checkpoint_tree_sha256,
            "prompt_artifact_id": model_config.verifier_prompt_artifact_id,
            "prompt_template_sha256": model_config.verifier_prompt_template_sha256,
            "calibration_version": model_config.calibration_version,
            "calibration_file_sha256": model_config.calibration_file_sha256,
            "decision_policy_version": model_config.decision_policy.policy_version,
        },
        "interpretation": {
            "ground_truth_claimed": False,
            "model_accuracy_claimed": False,
            "statistically_reliable_confidence_interval_claimed": False,
            "independence_claimed": False,
            "cross_split_leakage_claimed": False,
            "split_note": (
                "Claim-family and exact normalized-content components are declared, "
                "but this pilot has one test split, so cross-split validation is "
                "vacuous until development/train histories are added."
            ),
            "cluster_bootstrap_note": (
                "Three repository clusters are too few for reliable uncertainty claims."
            ),
            "closure_verdict": (
                "Executable real-history pilot complete; M4.9 scientific closure "
                "remains open "
                "pending a larger independently annotated multi-history benchmark."
            ),
        },
    }
    structural_hash = _json_hash(structural_payload)
    timing_payload: dict[str, object] = {
        "timings_sha256": file_hashes["timings.json"],
        "runtime_sources_sha256": file_hashes["runtime_sources.json"],
        "timings": timing_rows_tuple,
        "runtime_sources": runtime_source_rows_tuple,
    }
    timing_hash = _json_hash(timing_payload)
    result_manifest: dict[str, object] = {
        **structural_payload,
        "structural_hash": structural_hash,
        "timing_hash": timing_hash,
        "timing_payload": timing_payload,
        "output_directory": str(output),
    }
    _write_json(output / "result_manifest.json", result_manifest)
    return RealHistoryStudyResult(
        report=report,
        bundle=empirical_bundle,
        result_manifest=result_manifest,
        structural_hash=structural_hash,
        timing_hash=timing_hash,
    )
