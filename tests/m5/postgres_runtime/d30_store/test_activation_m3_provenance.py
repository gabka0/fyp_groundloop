"""D30 falsifier 5: exact activation-base M3 publication provenance."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from groundloop.ai.contracts import (
    AtomicClaim,
    CitedAnswer,
    ClaimExtractionResult,
    PipelineRunManifest,
    PipelineRunStatus,
    QueryKind,
    RetrievalCandidate,
    ScoreTriple,
    VerificationResult,
    stable_digest,
)
from groundloop.ai.manifest import manifest_from_dict, manifest_to_dict
from groundloop.ai.pipeline import claim_version_id
from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    normalized_text_hash,
)
from groundloop.errors import EventConflictError
from groundloop.m5.runtime import postgres_withdrawal
from tests.m5.postgres_runtime.d30_store.test_dynamic_claim_provenance import (
    _f8_tier10_locator,
    _F8Tier10Cursor,
    _valid_dynamic_authority,
)

_ACTIVATION_ROW = (
    "activation-a",
    "a" * 64,
    41,
    object(),
    "m5_active",
    1,
    41,
    41,
    0,
    "committed",
    "sealed",
    "complete",
    "strict",
    object(),
)


class _OneRowResult:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _ActivationOnlyCursor:
    def execute(self, statement: object, _parameters: object = None) -> Any:
        sql = " ".join(str(statement).split())
        if "FROM groundloop_m5_activation AS activation" in sql:
            return _OneRowResult(_ACTIVATION_ROW)
        if "SELECT claim_id FROM groundloop_claim" in sql:
            return _OneRowResult(("claim-a",))
        raise AssertionError("bootstrap classification consulted unrelated SQL")


def _jsonb(value: object) -> object:
    """Round-trip a value through the shape returned by a JSONB driver."""

    return json.loads(json.dumps(value))


class _MissingDynamicCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object, _parameters: object = None) -> _OneRowResult:
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if "FROM groundloop_m5_activation AS activation" in sql:
            return _OneRowResult(_ACTIVATION_ROW)
        if "SELECT claim_id FROM groundloop_claim" in sql:
            return _OneRowResult(("claim-a",))
        return _OneRowResult(None)


class _M3LockCursor:
    def __init__(self, rows: dict[tuple[str, tuple[object, ...]], object]) -> None:
        self._rows = rows
        self.acquisitions: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, statement: object, parameters: object = None) -> _OneRowResult:
        sql = " ".join(str(statement).split())
        relations = (
            ("image", "FROM groundloop_m4_update AS update_row"),
            ("epoch", "FROM groundloop_epoch"),
            ("execution", "FROM groundloop_verification_execution"),
            ("run", "FROM groundloop_pipeline_run"),
            ("candidate", "FROM groundloop_retrieval_candidate"),
            ("model", "FROM groundloop_model_artifact"),
            ("prompt", "FROM groundloop_prompt_artifact"),
            ("embedding", "FROM groundloop_chunk_embedding"),
            ("artifact_use", "FROM groundloop_pipeline_artifact_use"),
        )
        relation = next(
            (name for name, marker in relations if marker in sql),
            None,
        )
        if relation is None:
            raise AssertionError(f"unexpected tier-10 query: {sql}")
        key = (relation, tuple(parameters or ()))
        self.acquisitions.append(key)
        if key not in self._rows:
            raise AssertionError(f"unexpected M3 coordinate: {key}")
        return _OneRowResult(self._rows[key])  # type: ignore[arg-type]


class _MixedBranchLockCursor(_F8Tier10Cursor):
    def __init__(
        self,
        rows: dict[tuple[str, tuple[object, ...]], object],
        dynamic_claim: Any,
        owner: Any,
    ) -> None:
        super().__init__(dynamic_claim, owner)
        self._m3_rows = rows
        self.acquisitions: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, statement: object, parameters: object = None) -> Any:
        sql = " ".join(str(statement).split())
        if "FROM groundloop_m4_verification_execution" in sql:
            column = next(
                name
                for name in ("observation_id", "job_id", "admitted_pair_id")
                if f"WHERE {name} = %s" in sql
            )
            coordinate = tuple(parameters or ())
            expected = {
                "observation_id": self.claim.currency.observation_id,
                "job_id": self.claim.child_job_id,
                "admitted_pair_id": self.claim.admitted_pair_id,
            }
            if coordinate != (expected[column],):
                raise AssertionError(
                    f"unexpected dynamic execution coordinate: {coordinate}"
                )
            self.calls.append((sql, coordinate))
            self.acquisitions.append(("dynamic_execution", (column, *coordinate)))
            return _OneRowResult(self.claim.execution_rows[0])
        relations = (
            ("image", "FROM groundloop_m4_update AS update_row"),
            ("epoch", "FROM groundloop_epoch"),
            ("execution", "FROM groundloop_verification_execution"),
            ("run", "FROM groundloop_pipeline_run"),
            ("candidate", "FROM groundloop_retrieval_candidate"),
            ("model", "FROM groundloop_model_artifact"),
            ("prompt", "FROM groundloop_prompt_artifact"),
            ("embedding", "FROM groundloop_chunk_embedding"),
            ("artifact_use", "FROM groundloop_pipeline_artifact_use"),
        )
        relation = next(
            (name for name, marker in relations if marker in sql),
            None,
        )
        if relation is None:
            return super().execute(statement, parameters or ())
        coordinate = tuple(parameters or ())
        key = (relation, coordinate)
        self.calls.append((sql, coordinate))
        self.acquisitions.append(key)
        if key not in self._m3_rows:
            raise AssertionError(f"unexpected M3 coordinate: {key}")
        return _OneRowResult(self._m3_rows[key])  # type: ignore[arg-type]


def _m3_sources(function_source: Callable[[str], str]) -> tuple[str, str]:
    return (
        "\n".join(
            (
                function_source("_gather_d30_claim_authority"),
                function_source("_d30_observation_row"),
                function_source("_d30_m3_claim_locator"),
                function_source("_lock_d29_observation_authority"),
            )
        ),
        function_source("_validate_d30_m3_bootstrap"),
    )


def _cell(row: tuple[object, ...], index: int, value: object) -> tuple[object, ...]:
    values = list(row)
    values[index] = value
    return tuple(values)


def _m3_image_row(
    *,
    epoch_id: int,
    claim_id: str,
    chunk_id: str,
    chunk_text: str,
) -> tuple[object, ...]:
    registry_id = f"registry-{epoch_id}"
    document_version_id = "document-version-a"
    return (
        epoch_id,
        registry_id,
        registry_id,
        1,
        "a" * 64,
        registry_id,
        claim_id,
        0,
        claim_id,
        "answer-a",
        "Claim A",
        "extractor-model",
        "extractor-revision",
        "extractor-prompt-v1",
        True,
        chunk_id,
        document_version_id,
        0,
        chunk_text,
        normalized_text_hash(chunk_text),
        "chunker-v1",
        1,
        None,
        document_version_id,
        "document-a",
        "b" * 64,
        1,
        None,
    )


def _valid_m3_claim() -> Any:
    atomic = AtomicClaim("local-a", "Claim A", True, ("chunk-a",))
    claim_id = claim_version_id("answer-a", atomic)
    chunk_id = "chunk-a"
    chunk_text = "Chunk A"
    chunk_text_hash = normalized_text_hash(chunk_text)
    chunk_input_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
    run_id = "run-a"
    candidate_id = "candidate-a"
    verifier_model_id = "verifier-model"
    embedding_model_id = "embedding-model"
    prompt_id = "verification-prompt"
    calibration = "calibration-v1"
    input_hash = "1" * 64
    raw_output_hash = "2" * 64
    run_input_hash = "3" * 64
    template = "Judge the evidence."
    template_hash = hashlib.sha256(template.encode()).hexdigest()
    epoch_id = 40
    scores = ScoreTriple(0.8, 0.1, 0.1)
    raw_logits = (2.0, 0.0, -1.0)
    candidate = RetrievalCandidate(
        candidate_id,
        QueryKind.CLAIM,
        claim_id,
        chunk_id,
        0.9,
        1,
        embedding_model_id,
        "cosine-v1",
    )
    verification = VerificationResult(
        atomic.local_claim_id,
        chunk_id,
        candidate_id,
        verifier_model_id,
        prompt_id,
        calibration,
        1.0,
        scores,
        input_hash,
        raw_output_hash,
        raw_logits,
    )
    answer = CitedAnswer("Answer", (chunk_id,), "4" * 64, "5" * 64)
    extraction = ClaimExtractionResult((atomic,), "6" * 64, "7" * 64)
    observation_id = "observation-" + stable_digest(
        "m3-observation-v1",
        claim_id,
        chunk_id,
        "direct_verification",
        verifier_model_id,
        prompt_id,
        calibration,
        input_hash,
    )
    named_ids = (
        f"embedding:{chunk_id}:{embedding_model_id}",
        embedding_model_id,
        verifier_model_id,
        prompt_id,
        candidate_id,
        observation_id,
    )
    manifest = PipelineRunManifest(
        schema_version="m3-v1",
        run_id=run_id,
        status=PipelineRunStatus.PUBLISHED,
        config_hash="8" * 64,
        input_hash=run_input_hash,
        corpus_hash="9" * 64,
        question_id="question-a",
        decision_policy_version="policy-a",
        answer_version_id="answer-a",
        semantic_epoch_id=epoch_id,
        confirmed_as_of_epoch=epoch_id,
        model_artifact_ids=(embedding_model_id, verifier_model_id),
        prompt_artifact_ids=(prompt_id,),
        chunk_version_ids=(chunk_id,),
        chunk_text_hashes=((chunk_id, chunk_text_hash),),
        retrieval_candidates=(candidate,),
        answer=answer,
        extraction=extraction,
        claims=(atomic,),
        verifications=(verification,),
        claim_states=(
            ClaimState(
                claim_id,
                1,
                0,
                0.8,
                None,
                (observation_id,),
                (),
                ClaimStatus.SUPPORTED,
            ),
        ),
        answer_states=(
            AnswerState(
                "answer-a",
                1,
                1,
                0,
                0,
                0,
                AnswerStatus.VALID,
            ),
        ),
        timings=(),
        reused_artifact_ids=(),
        new_artifact_ids=named_ids,
    )
    currency = postgres_withdrawal._D30CurrencyRow(
        "claim",
        claim_id,
        chunk_id,
        "direct_verification",
        observation_id,
        0,
    )
    model_row = (
        verifier_model_id,
        "verification",
        "provider",
        "model-name",
        "immutable-revision",
        "tokenizer-revision",
        "license",
        "b" * 64,
        None,
    )
    embedding_model_row = (
        embedding_model_id,
        "embedding",
        "provider",
        "embedding-name",
        "embedding-revision",
        "tokenizer-revision",
        "license",
        "c" * 64,
        None,
    )
    prompt_row = (
        prompt_id,
        "verification",
        "v1",
        template,
        template_hash,
        "d" * 64,
    )
    return postgres_withdrawal._D30M3ClaimLocator(
        currency=currency,
        observation_row=(
            observation_id,
            "claim",
            claim_id,
            chunk_id,
            "direct_verification",
            scores.support,
            scores.refute,
            scores.neutral,
            model_row[3],
            model_row[4],
            f"v1:{template_hash}",
            input_hash,
            epoch_id,
            raw_output_hash,
            True,
        ),
        epoch_row=(
            epoch_id,
            f"m3-run:{run_id}",
            run_input_hash,
            0,
            "committed",
            "sealed",
            "complete",
            "provisional",
            object(),
        ),
        execution_row=(
            observation_id,
            run_id,
            candidate_id,
            verifier_model_id,
            prompt_id,
            calibration,
            1.0,
            raw_logits,
            raw_output_hash,
            None,
        ),
        run_row=(
            run_id,
            "m3-v1",
            "published",
            "8" * 64,
            run_input_hash,
            "9" * 64,
            "question-a",
            "answer-a",
            epoch_id,
            _jsonb(manifest_to_dict(manifest)),
            None,
            object(),
            object(),
        ),
        candidate_row=(
            candidate_id,
            run_id,
            "claim",
            claim_id,
            claim_id,
            chunk_id,
            embedding_model_id,
            "cosine-v1",
            0.9,
            1,
        ),
        model_row=model_row,
        prompt_row=prompt_row,
        embedding_model_row=embedding_model_row,
        chunk_embedding_row=(
            chunk_id,
            embedding_model_id,
            "[1,0]",
            chunk_input_hash,
        ),
        artifact_use_rows=tuple(
            (run_id, kind, artifact_id, False)
            for kind, artifact_id in sorted(
                (
                    ("model", verifier_model_id),
                    ("prompt", prompt_id),
                    ("model", embedding_model_id),
                    ("embedding", f"embedding:{chunk_id}:{embedding_model_id}"),
                    ("retrieval", candidate_id),
                    ("verification", observation_id),
                )
            )
        ),
        image_rows=tuple(
            _m3_image_row(
                epoch_id=image_epoch_id,
                claim_id=claim_id,
                chunk_id=chunk_id,
                chunk_text=chunk_text,
            )
            for image_epoch_id in (41, 50)
        ),
    )


def _m3_lock_rows(*claims: Any) -> dict[tuple[str, tuple[object, ...]], object]:
    rows: dict[tuple[str, tuple[object, ...]], object] = {}
    for claim in claims:
        rows[("epoch", (int(claim.epoch_row[0]),))] = claim.epoch_row
        rows[("execution", (claim.currency.observation_id,))] = claim.execution_row
        rows[("run", (str(claim.run_row[0]),))] = claim.run_row
        rows[("candidate", (str(claim.candidate_row[0]),))] = claim.candidate_row
        for model in (claim.model_row, claim.embedding_model_row):
            rows[("model", (str(model[0]),))] = model
        rows[("prompt", (str(claim.prompt_row[0]),))] = claim.prompt_row
        rows[
            (
                "embedding",
                (
                    claim.currency.chunk_version_id,
                    str(claim.embedding_model_row[0]),
                ),
            )
        ] = claim.chunk_embedding_row
        for image_row in claim.image_rows:
            rows[
                (
                    "image",
                    (
                        str(image_row[6]),
                        str(image_row[15]),
                        int(str(image_row[0])),
                    ),
                )
            ] = image_row
        for artifact_use in claim.artifact_use_rows:
            coordinate = tuple(str(value) for value in artifact_use[:3])
            rows[("artifact_use", coordinate)] = artifact_use
    return rows


class _M3ClaimHolderCursor(_M3LockCursor):
    """Drive one bootstrap holder through its production locator/lock queries."""

    def __init__(
        self,
        claim: Any,
        *,
        published_valid_from_epoch: int,
    ) -> None:
        super().__init__(_m3_lock_rows(claim))
        self.claim = claim
        self.published_valid_from_epoch = published_valid_from_epoch
        self.statements: list[str] = []

    def execute(self, statement: object, parameters: object = None) -> _OneRowResult:
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        claim = self.claim
        key = claim.currency.full_key
        if "SELECT activation.activation_id" in sql:
            activation = _cell(_cell(_ACTIVATION_ROW, 6, 50), 7, 50)
            return _OneRowResult(activation)
        if "SELECT activation.base_m4_epoch_id, m4_head.epoch_id" in sql:
            return _OneRowResult((41, 50, 50))
        if "FROM groundloop_semantic_observation" in sql:
            assert tuple(parameters or ()) == (claim.currency.observation_id,)
            return _OneRowResult(claim.observation_row)
        if "FROM groundloop_observation_currency" in sql:
            assert tuple(parameters or ()) == key
            return _OneRowResult(
                (*key, claim.currency.observation_id, claim.currency.installed_revision)
            )
        if "FROM groundloop_published_observation_currency" in sql:
            assert tuple(parameters or ()) == key
            return _OneRowResult(
                (
                    *key,
                    claim.currency.observation_id,
                    self.published_valid_from_epoch,
                    None,
                )
            )
        return super().execute(statement, parameters)


def _exercise_m3_claim_holder_path(
    cursor: _M3ClaimHolderCursor,
) -> tuple[postgres_withdrawal.ObservationDependency, ...]:
    claim = cursor.claim
    authority = postgres_withdrawal._gather_d30_claim_authority(
        cursor,  # type: ignore[arg-type]
        (claim.currency,),
    )
    assert authority.currency_rows == (claim.currency,)
    assert len(authority.bootstrap) == 1
    assert authority.dynamic == ()
    assert authority.owners == ()
    locator = replace(
        _m3_only_locator(*authority.bootstrap),
        d30_claims=authority,
    )
    postgres_withdrawal._lock_d30_m3_image_membership(
        cursor,  # type: ignore[arg-type]
        locator,
    )
    attempts, candidates = postgres_withdrawal._lock_d29_tier_10_authority(
        cursor,  # type: ignore[arg-type]
        locator,
        predecessor_epoch_id=50,
        candidate_policy_id="policy-a",
    )
    assert attempts == {}
    assert candidates == ()
    dynamic = postgres_withdrawal._lock_d30_dynamic_owner_topology(
        cursor,  # type: ignore[arg-type]
        locator,
        attempts,
        {},
        predecessor_epoch_id=50,
        candidate_policy_id="policy-a",
        verifier_execution_spec_hash="f" * 64,
    )
    assert dynamic == {}
    requirement_edges, direct_dependencies = (
        postgres_withdrawal._lock_d29_observation_authority(
            cursor,  # type: ignore[arg-type]
            locator,
            predecessor_epoch_id=50,
            candidate_policy_id="policy-a",
            verifier_authority={},
            bootstrap_authority={},
            d30_dynamic_authority=dynamic,
        )
    )
    assert requirement_edges == ()
    return direct_dependencies


def test_bootstrap_branch_is_positive_m3_closure_not_negative_m4_inference(
    function_source: Callable[[str], str],
) -> None:
    gather, validate = _m3_sources(function_source)
    assert "installed_revision" in gather
    assert "base_m4_epoch_id" in gather
    for relation in (
        "groundloop_verification_execution",
        "groundloop_pipeline_run",
        "groundloop_retrieval_candidate",
        "groundloop_model_artifact",
        "groundloop_prompt_artifact",
        "groundloop_pipeline_artifact_use",
        "groundloop_epoch",
    ):
        assert relation in gather + validate
    for forbidden in (
        "groundloop_working_observation_delta",
        "groundloop_m4_verification_execution",
        "groundloop_m5_runtime_epoch",
        "groundloop_semantic_job",
        "groundloop_admitted_pair",
    ):
        assert forbidden not in validate
    assert "NOT EXISTS" not in validate.upper()


def test_bootstrap_validates_published_run_epoch_and_exact_epoch_identity(
    function_source: Callable[[str], str],
) -> None:
    gather, validate = _m3_sources(function_source)
    source = gather + validate
    for literal in (
        "published",
        "semantic_epoch_id",
        "m3-run:",
        "input_hash",
        "revision",
        "committed",
        "sealed",
        "complete",
        "provisional",
        "sealed_at",
    ):
        assert literal in source
    assert "produced_epoch" in source
    assert "valid_from_epoch" in source
    assert "produced_epoch == valid_from_epoch" not in validate


def test_bootstrap_cross_binds_candidate_artifacts_manifest_and_named_uses(
    function_source: Callable[[str], str],
) -> None:
    source = "\n".join(_m3_sources(function_source))
    for field in (
        "run_id",
        "candidate_id",
        "query_kind",
        "query_id",
        "claim_id",
        "chunk_version_id",
        "embedding_model_artifact_id",
        "model_artifact_id",
        "prompt_artifact_id",
        "manifest",
        "artifact_kind",
        "artifact_id",
        "reused",
    ):
        assert field in "\n".join(_m3_sources(function_source))
    for kind in ("model", "prompt", "embedding", "retrieval", "verification"):
        assert kind in source
    assert "query_kind" in source and "claim" in source


def test_bootstrap_identity_calibration_scores_and_null_reuse_are_exact(
    function_source: Callable[[str], str],
) -> None:
    source = function_source("_validate_d30_m3_bootstrap")
    closure = "\n".join(_m3_sources(function_source))
    for field in (
        "observation_id",
        "task_type",
        "model_id",
        "model_version",
        "prompt_version",
        "input_hash",
        "support_score",
        "refute_score",
        "neutral_score",
        "calibration_version",
        "temperature",
        "raw_logits",
        "raw_output_hash",
        "reused_from_observation_id",
    ):
        assert field in closure
    assert "execution[9] is not None" in source
    assert "m3-observation-v1" in source
    assert "direct_verification" in source


def test_dynamic_row_cannot_borrow_zero_revision_bootstrap_authority(
    function_source: Callable[[str], str],
) -> None:
    gather = function_source("_gather_d30_claim_authority")
    finalizer = function_source("_lock_d30_dynamic_owner_topology")
    source = gather + finalizer
    assert "installed_revision" in source
    assert "_validate_d30_m3_bootstrap" in source
    assert "_validate_d30_dynamic_claim" in source
    assert "if currency.installed_revision == 0" in gather
    dynamic = finalizer.index("locator.d30_claims.dynamic")
    bootstrap = finalizer.index("locator.d30_claims.bootstrap")
    assert dynamic < bootstrap


def test_zero_revision_dispatches_only_to_positive_m3_locator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    currency = postgres_withdrawal._D30CurrencyRow(
        "claim", "claim-a", "chunk-a", "direct_verification", "observation-a", 0
    )
    observation = (
        "observation-a",
        "claim",
        "claim-a",
        "chunk-a",
        "direct_verification",
        0.9,
        0.05,
        0.05,
        "model",
        "revision",
        "prompt",
        "a" * 64,
        41,
        "b" * 64,
        True,
    )
    sentinel = object()
    monkeypatch.setattr(
        postgres_withdrawal,
        "_d30_observation_row",
        lambda cursor, observation_id: observation,
    )

    def m3_locator(
        cursor: object, *, currency: object, observation_row: object
    ) -> object:
        assert isinstance(cursor, _ActivationOnlyCursor)
        assert currency is not None and observation_row == observation
        return sentinel

    monkeypatch.setattr(postgres_withdrawal, "_d30_m3_claim_locator", m3_locator)
    monkeypatch.setattr(
        postgres_withdrawal,
        "_d30_owner_locator",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("bootstrap classification consulted a dynamic owner")
        ),
    )
    authority = postgres_withdrawal._gather_d30_claim_authority(
        _ActivationOnlyCursor(),
        (currency,),  # type: ignore[arg-type]
    )
    assert authority.currency_rows == (currency,)
    assert authority.bootstrap == (sentinel,)
    assert authority.dynamic == ()
    assert authority.owners == ()


def test_positive_revision_missing_delta_fails_without_m3_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    currency = postgres_withdrawal._D30CurrencyRow(
        "claim", "claim-a", "chunk-a", "arbitrary-task", "observation-a", 9
    )
    observation = (
        "observation-a",
        "claim",
        "claim-a",
        "chunk-a",
        "arbitrary-task",
        0.9,
        0.05,
        0.05,
        "model",
        "revision",
        "prompt",
        "a" * 64,
        41,
        "b" * 64,
        True,
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_d30_observation_row",
        lambda cursor, observation_id: observation,
    )
    monkeypatch.setattr(
        postgres_withdrawal,
        "_d30_m3_claim_locator",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("dynamic claim borrowed bootstrap authority")
        ),
    )
    cursor = _MissingDynamicCursor()
    with pytest.raises(EventConflictError, match="working delta"):
        postgres_withdrawal._gather_d30_claim_authority(  # type: ignore[arg-type]
            cursor, (currency,)
        )
    assert any(
        "groundloop_working_observation_delta" in sql for sql in cursor.statements
    )
    assert all(
        "groundloop_m4_verification_execution" not in sql for sql in cursor.statements
    )


def test_activation_head_authority_accepts_only_exact_legacy_guard() -> None:
    assert (
        postgres_withdrawal._validate_d30_activation_row(
            _ACTIVATION_ROW,
            predecessor_epoch_id=41,
        )
        == 41
    )
    mutations = (
        (0, ""),
        (1, "short"),
        (2, 42),
        (4, "m4_only"),
        (5, 0),
        (6, 40),
        (7, 40),
        (9, "pending"),
        (10, "open"),
        (11, "partial"),
        (12, "provisional"),
        (13, None),
    )
    for index, bad in mutations:
        row = list(_ACTIVATION_ROW)
        row[index] = bad
        with pytest.raises(EventConflictError):
            postgres_withdrawal._validate_d30_activation_row(
                tuple(row),
                predecessor_epoch_id=41,
            )


def test_valid_activation_base_m3_closure_passes() -> None:
    postgres_withdrawal._validate_d30_m3_bootstrap(
        _valid_m3_claim(),
        activation_base_epoch_id=41,
        predecessor_epoch_id=50,
    )


def test_activation_base_holder_accepts_later_open_publication_epoch() -> None:
    claim = _valid_m3_claim()
    cursor = _M3ClaimHolderCursor(claim, published_valid_from_epoch=41)

    dependencies = _exercise_m3_claim_holder_path(cursor)

    assert int(claim.observation_row[12]) == 40
    assert cursor.published_valid_from_epoch == 41
    assert claim.currency.installed_revision == 0
    assert dependencies == (
        postgres_withdrawal.ObservationDependency(
            claim.currency.observation_id,
            postgres_withdrawal.PairKey(
                claim.currency.subject_id,
                claim.currency.chunk_version_id,
            ),
        ),
    )
    assert any(
        "FROM groundloop_observation_currency" in statement
        and "FOR UPDATE" in statement
        for statement in cursor.statements
    )
    assert any(
        "FROM groundloop_published_observation_currency" in statement
        and "valid_to_epoch IS NULL" in statement
        and "FOR UPDATE" in statement
        for statement in cursor.statements
    )


def test_activation_base_holder_rejects_publication_after_activation_base() -> None:
    claim = _valid_m3_claim()
    cursor = _M3ClaimHolderCursor(claim, published_valid_from_epoch=42)

    with pytest.raises(EventConflictError, match="activation-base currency changed"):
        _exercise_m3_claim_holder_path(cursor)

    assert int(claim.observation_row[12]) == 40
    assert cursor.published_valid_from_epoch == 42
    assert claim.currency.installed_revision == 0


@pytest.mark.parametrize(
    ("index", "bad"),
    (
        (0, 42),
        (2, "other-registry"),
        (3, 0),
        (5, "other-registry"),
        (6, "other-claim"),
        (7, 1),
        (8, "other-claim"),
        (9, "other-answer"),
        (10, "Other claim"),
        (14, False),
        (15, "other-chunk"),
        (16, "other-document-version"),
        (19, "f" * 64),
        (21, 42),
        (22, 41),
        (23, "other-document-version"),
        (25, "short"),
        (26, 42),
        (27, 41),
    ),
)
def test_activation_image_identity_and_interval_corruptions_fail_closed(
    index: int,
    bad: object,
) -> None:
    claim = _valid_m3_claim()
    image_rows = (_cell(claim.image_rows[0], index, bad), claim.image_rows[1])
    with pytest.raises(EventConflictError, match="image"):
        postgres_withdrawal._validate_d30_m3_bootstrap(
            replace(claim, image_rows=image_rows),
            activation_base_epoch_id=41,
            predecessor_epoch_id=50,
        )


@pytest.mark.parametrize("shape", ("missing", "reversed", "duplicate"))
def test_activation_image_requires_exact_deduplicated_base_predecessor_order(
    shape: str,
) -> None:
    claim = _valid_m3_claim()
    if shape == "missing":
        image_rows = claim.image_rows[:1]
    elif shape == "reversed":
        image_rows = tuple(reversed(claim.image_rows))
    else:
        image_rows = (*claim.image_rows, claim.image_rows[-1])
    with pytest.raises(EventConflictError, match="image"):
        postgres_withdrawal._validate_d30_m3_bootstrap(
            replace(claim, image_rows=image_rows),
            activation_base_epoch_id=41,
            predecessor_epoch_id=50,
        )


def test_manifest_required_flag_is_cross_bound_to_installed_claim_image() -> None:
    claim = _valid_m3_claim()
    manifest = manifest_from_dict(claim.run_row[9])
    changed_atomic = replace(manifest.claims[0], required=False)
    required_control = AtomicClaim("local-b", "Claim B", True, ("chunk-a",))
    assert manifest.extraction is not None
    manifest = replace(
        manifest,
        claims=(changed_atomic, required_control),
        extraction=replace(
            manifest.extraction,
            claims=(changed_atomic, required_control),
        ),
    )
    with pytest.raises(EventConflictError, match="image"):
        postgres_withdrawal._validate_d30_m3_bootstrap(
            replace(
                claim,
                run_row=_cell(
                    claim.run_row,
                    9,
                    _jsonb(manifest_to_dict(manifest)),
                ),
            ),
            activation_base_epoch_id=41,
            predecessor_epoch_id=50,
        )


def test_manifest_chunk_hash_and_embedding_input_hash_are_exact() -> None:
    claim = _valid_m3_claim()
    manifest = manifest_from_dict(claim.run_row[9])
    wrong_manifest = replace(
        manifest,
        chunk_text_hashes=((claim.currency.chunk_version_id, "f" * 64),),
    )
    for changed in (
        replace(
            claim,
            run_row=_cell(
                claim.run_row,
                9,
                _jsonb(manifest_to_dict(wrong_manifest)),
            ),
        ),
        replace(
            claim,
            chunk_embedding_row=_cell(
                claim.chunk_embedding_row,
                3,
                "f" * 64,
            ),
        ),
    ):
        with pytest.raises(EventConflictError, match="image"):
            postgres_withdrawal._validate_d30_m3_bootstrap(
                changed,
                activation_base_epoch_id=41,
                predecessor_epoch_id=50,
            )


def test_base_and_predecessor_image_chunk_bytes_must_be_identical() -> None:
    claim = _valid_m3_claim()
    changed_text = "Chunk B"
    changed_predecessor = _cell(
        _cell(claim.image_rows[1], 18, changed_text),
        19,
        normalized_text_hash(changed_text),
    )
    with pytest.raises(EventConflictError, match="image"):
        postgres_withdrawal._validate_d30_m3_bootstrap(
            replace(
                claim,
                image_rows=(claim.image_rows[0], changed_predecessor),
            ),
            activation_base_epoch_id=41,
            predecessor_epoch_id=50,
        )


@pytest.mark.parametrize(
    ("field", "index", "bad"),
    (
        ("epoch_row", 1, "m3-run:other"),
        ("epoch_row", 3, 1),
        ("epoch_row", 4, "pending"),
        ("execution_row", 0, "other-observation"),
        ("execution_row", 5, "other-calibration"),
        ("execution_row", 8, "f" * 64),
        ("execution_row", 9, "older-observation"),
        ("run_row", 2, "staged"),
        ("run_row", 8, 39),
        ("candidate_row", 2, "question"),
        ("candidate_row", 3, "other-claim"),
        ("model_row", 1, "embedding"),
        ("prompt_row", 1, "generation"),
        ("embedding_model_row", 1, "verification"),
        ("observation_row", 5, 0.7),
        ("observation_row", 13, "f" * 64),
    ),
)
def test_named_m3_closure_corruptions_fail_closed(
    field: str,
    index: int,
    bad: object,
) -> None:
    claim = _valid_m3_claim()
    claim = replace(claim, **{field: _cell(getattr(claim, field), index, bad)})
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_m3_bootstrap(
            claim,
            activation_base_epoch_id=41,
            predecessor_epoch_id=50,
        )


def test_missing_or_changed_named_m3_artifact_use_fails_closed() -> None:
    claim = _valid_m3_claim()
    for artifact_uses in (
        claim.artifact_use_rows[:-1],
        (
            _cell(claim.artifact_use_rows[0], 3, True),
            *claim.artifact_use_rows[1:],
        ),
    ):
        with pytest.raises(EventConflictError):
            postgres_withdrawal._validate_d30_m3_bootstrap(
                replace(claim, artifact_use_rows=artifact_uses),
                activation_base_epoch_id=41,
                predecessor_epoch_id=50,
            )


@pytest.mark.parametrize("confusion", ("verification-global", "candidate-local"))
def test_m3_local_and_global_claim_id_confusion_fails_closed(
    confusion: str,
) -> None:
    claim = _valid_m3_claim()
    manifest = manifest_from_dict(claim.run_row[9])
    local_claim_id = manifest.claims[0].local_claim_id
    if confusion == "verification-global":
        manifest = replace(
            manifest,
            verifications=(
                replace(
                    manifest.verifications[0],
                    claim_id=claim.currency.subject_id,
                ),
            ),
        )
    else:
        manifest = replace(
            manifest,
            retrieval_candidates=(
                replace(
                    manifest.retrieval_candidates[0],
                    query_id=local_claim_id,
                ),
            ),
        )
    mutated = replace(
        claim,
        run_row=_cell(
            claim.run_row,
            9,
            _jsonb(manifest_to_dict(manifest)),
        ),
    )
    with pytest.raises(EventConflictError):
        postgres_withdrawal._validate_d30_m3_bootstrap(
            mutated,
            activation_base_epoch_id=41,
            predecessor_epoch_id=50,
        )


def test_m3_manifest_rejects_extra_keys_and_primitive_type_aliases() -> None:
    claim = _valid_m3_claim()
    raw = claim.run_row[9]
    assert type(raw) is dict

    extra = deepcopy(raw)
    extra["unexpected"] = "non-authority"

    required_as_int = deepcopy(raw)
    required_as_int["claims"][0]["required"] = 1
    required_as_int["extraction"]["claims"][0]["required"] = 1

    rank_as_bool = deepcopy(raw)
    rank_as_bool["retrieval_candidates"][0]["rank"] = True

    temperature_as_int = deepcopy(raw)
    temperature_as_int["verifications"][0]["temperature"] = 1

    for malformed in (extra, required_as_int, rank_as_bool, temperature_as_int):
        with pytest.raises(EventConflictError, match="manifest is invalid"):
            postgres_withdrawal._validate_d30_m3_bootstrap(
                replace(claim, run_row=_cell(claim.run_row, 9, malformed)),
                activation_base_epoch_id=41,
                predecessor_epoch_id=50,
            )


def test_m3_manifest_accepts_the_exact_jsonb_array_shape() -> None:
    claim = _valid_m3_claim()
    jsonb_value = _jsonb(claim.run_row[9])

    postgres_withdrawal._validate_d30_m3_bootstrap(
        replace(claim, run_row=_cell(claim.run_row, 9, jsonb_value)),
        activation_base_epoch_id=41,
        predecessor_epoch_id=50,
    )


def _shared_run_m3_claim(first: Any) -> Any:
    observation_id = "observation-z"
    candidate_id = "candidate-z"
    currency = replace(
        first.currency,
        subject_id="claim-z",
        observation_id=observation_id,
    )
    artifact_uses = tuple(
        _cell(row, 2, candidate_id)
        if str(row[1]) == "retrieval"
        else _cell(row, 2, observation_id)
        if str(row[1]) == "verification"
        else row
        for row in first.artifact_use_rows
    )
    return replace(
        first,
        currency=currency,
        observation_row=_cell(
            _cell(first.observation_row, 0, observation_id), 2, "claim-z"
        ),
        execution_row=_cell(
            _cell(first.execution_row, 0, observation_id), 2, candidate_id
        ),
        candidate_row=_cell(
            _cell(
                _cell(first.candidate_row, 0, candidate_id),
                3,
                "claim-z",
            ),
            4,
            "claim-z",
        ),
        artifact_use_rows=artifact_uses,
        image_rows=tuple(
            _cell(_cell(row, 6, "claim-z"), 8, "claim-z") for row in first.image_rows
        ),
    )


def _m3_only_locator(*claims: Any) -> Any:
    empty_coordinates = postgres_withdrawal._DocumentDeclarationCoordinates(
        direct_scope_root_job_ids=(),
        requirement_scope_root_job_ids=(),
        direct_job_ids=(),
        requirement_job_ids=(),
    )
    return postgres_withdrawal._D29LocatorAuthority(
        predecessor_snapshots=None,
        admitted_locator_keys=(),
        qualifying_admitted_pair_digests=(),
        candidate_root_job_ids=(),
        candidate_root_job_coordinates=(),
        candidate_dependency_coordinates=(),
        direct_job_ids=(),
        direct_job_coordinates=(),
        direct_dependency_coordinates=(),
        direct_admitted_pair_ids=(),
        direct_scope_root_job_ids=(),
        direct_verifier_observation_ids=(),
        verifier_job_ids=(),
        verifier_root_job_ids=(),
        verifier_observation_ids=(),
        verifier_dependency_coordinates=(),
        verifier_artifact_ids=(),
        verifier_pair_input_hashes=(),
        bootstrap_observation_coordinates=(),
        direct_bootstrap_observation_coordinates=(),
        requirement_currency_keys=(),
        direct_currency_keys=(),
        direct_frontier_keys=(),
        touched_requirement_ids=(),
        prospective_coordinates=empty_coordinates,
        d30_claims=postgres_withdrawal._D30ClaimLocatorAuthority(
            currency_rows=tuple(claim.currency for claim in claims),
            dynamic=(),
            bootstrap=tuple(claims),
            owners=(),
            activation_row=None,
        ),
    )


def test_shared_m3_points_lock_once_in_frozen_relation_and_key_order() -> None:
    first = _valid_m3_claim()
    second = _shared_run_m3_claim(first)
    claims = (second, first)
    rows = _m3_lock_rows(*claims)

    cursor = _M3LockCursor(rows)
    direct_attempts, direct_candidates = (
        postgres_withdrawal._lock_d29_tier_10_authority(
            cursor,  # type: ignore[arg-type]
            _m3_only_locator(*claims),
            predecessor_epoch_id=50,
            candidate_policy_id="policy-a",
        )
    )
    assert direct_attempts == {}
    assert direct_candidates == ()

    relation_order = (
        "epoch",
        "execution",
        "run",
        "candidate",
        "model",
        "prompt",
        "embedding",
        "artifact_use",
    )
    expected = [
        key
        for relation in relation_order
        for key in sorted(
            (candidate for candidate in rows if candidate[0] == relation),
            key=lambda item: tuple(str(value) for value in item[1]),
        )
    ]
    assert cursor.acquisitions == expected
    assert len(cursor.acquisitions) == len(set(cursor.acquisitions))
    for shared in (
        ("epoch", (40,)),
        ("run", ("run-a",)),
        ("model", ("embedding-model",)),
        ("model", ("verifier-model",)),
        ("prompt", ("verification-prompt",)),
        ("embedding", ("chunk-a", "embedding-model")),
    ):
        assert cursor.acquisitions.count(shared) == 1


def test_dynamic_and_m3_shared_artifacts_lock_once_in_global_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _valid_m3_claim()
    dynamic, owner = _valid_dynamic_authority(execution=True)
    execution = dynamic.execution_rows[0]
    assert execution is not None
    shared_execution = _cell(
        _cell(execution, 3, bootstrap.model_row[0]),
        4,
        bootstrap.prompt_row[0],
    )
    shared_observation = _cell(
        _cell(
            _cell(dynamic.observation_row, 8, bootstrap.model_row[3]),
            9,
            bootstrap.model_row[4],
        ),
        10,
        f"{bootstrap.prompt_row[2]}:{bootstrap.prompt_row[4]}",
    )
    dynamic = replace(
        dynamic,
        observation_row=shared_observation,
        execution_rows=(shared_execution, shared_execution, shared_execution),
        model_row=bootstrap.model_row,
        prompt_row=bootstrap.prompt_row,
    )
    postgres_withdrawal._validate_d30_dynamic_claim(
        dynamic,
        owner=owner,
        candidate_policy_id="policy-a",
    )
    locator = _f8_tier10_locator(dynamic, owner)
    locator.d30_claims.bootstrap = (bootstrap,)
    assert locator.d30_claims.owners == (owner,)
    assert locator.d30_claims.dynamic == (dynamic,)
    assert locator.d30_claims.bootstrap == (bootstrap,)
    rows = _m3_lock_rows(bootstrap)
    cursor = _MixedBranchLockCursor(rows, dynamic, owner)
    monkeypatch.setattr(
        postgres_withdrawal,
        "_lock_d29_direct_attempts",
        lambda *_args, **_kwargs: {},
    )

    direct_attempts, direct_candidates = (
        postgres_withdrawal._lock_d29_tier_10_authority(
            cursor,  # type: ignore[arg-type]
            locator,
            predecessor_epoch_id=50,
            candidate_policy_id="policy-a",
        )
    )

    assert direct_attempts == {}
    assert direct_candidates == ()
    relation_order = (
        "epoch",
        "execution",
        "run",
        "candidate",
        "model",
        "prompt",
        "embedding",
        "artifact_use",
    )
    expected = [
        (
            "dynamic_execution",
            ("observation_id", dynamic.currency.observation_id),
        ),
        ("dynamic_execution", ("job_id", dynamic.child_job_id)),
        (
            "dynamic_execution",
            ("admitted_pair_id", dynamic.admitted_pair_id),
        ),
        *(
            key
            for relation in relation_order
            for key in sorted(
                (candidate for candidate in rows if candidate[0] == relation),
                key=lambda item: tuple(str(value) for value in item[1]),
            )
        ),
    ]
    assert cursor.acquisitions == expected
    assert dynamic.model_row == bootstrap.model_row
    assert dynamic.prompt_row == bootstrap.prompt_row
    assert cursor.acquisitions.count(("model", (str(bootstrap.model_row[0]),))) == 1
    assert cursor.acquisitions.count(("prompt", (str(bootstrap.prompt_row[0]),))) == 1


def test_m3_image_points_rerun_once_in_epoch_claim_chunk_order() -> None:
    first = _valid_m3_claim()
    second = _shared_run_m3_claim(first)
    locator = _m3_only_locator(second, first, first)
    rows = {
        (
            "image",
            (str(row[6]), str(row[15]), int(str(row[0]))),
        ): row
        for claim in (first, second)
        for row in claim.image_rows
    }
    cursor = _M3LockCursor(rows)
    postgres_withdrawal._lock_d30_m3_image_membership(
        cursor,  # type: ignore[arg-type]
        locator,
    )
    expected = sorted(
        rows,
        key=lambda item: (
            int(item[1][2]),
            str(item[1][0]).encode(),
            str(item[1][1]).encode(),
        ),
    )
    assert cursor.acquisitions == expected
    assert len(cursor.acquisitions) == len(set(cursor.acquisitions))
