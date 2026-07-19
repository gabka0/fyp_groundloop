"""Wave-1 deterministic tests for independent M4 empirical baselines."""

from __future__ import annotations

import ast
import hashlib
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import pytest

from groundloop.domain import (
    AnswerState,
    AnswerStatus,
    ClaimState,
    ClaimStatus,
    VerificationLabel,
)
from groundloop.errors import ValidationError
from groundloop.m4.contracts import (
    JudgmentSourceKind,
    PairJudgment,
    PairKey,
    stable_m4_digest,
)
from groundloop.m4.oracles import (
    DeterministicJudgmentTable,
    RefreshChunk,
    RefreshClaim,
    compute_affected_sets,
    compute_exhaustive_additive_delta,
    exact_brute_force_pairs,
    pair_positive_claim_ids,
    recompute_grounding_states,
    run_full_pair_audit,
    run_snapshot_refresh,
)

ROOT = Path(__file__).resolve().parents[3]
HASH_A = "a" * 64
HASH_B = "b" * 64


def _judgment(
    claim_id: str,
    chunk_id: str,
    label: VerificationLabel,
    *,
    split_id: str = "development",
) -> PairJudgment:
    scores = {
        VerificationLabel.SUPPORT: (0.9, 0.05, 0.05),
        VerificationLabel.REFUTE: (0.05, 0.9, 0.05),
        VerificationLabel.NEUTRAL: (0.05, 0.05, 0.9),
    }[label]
    return PairJudgment(
        pair=PairKey(claim_id, chunk_id),
        source_kind=JudgmentSourceKind.MODEL,
        source_artifact_id="deterministic-verifier-v1",
        decision_policy_or_guideline_id="decision-policy-v1",
        derived_label=label,
        input_hash=stable_m4_digest("fixture-input", claim_id, chunk_id),
        split_id=split_id,
        support_score=scores[0],
        refute_score=scores[1],
        neutral_score=scores[2],
    )


def _table(*judgments: PairJudgment) -> DeterministicJudgmentTable:
    return DeterministicJudgmentTable(
        tuple(sorted(judgments, key=lambda item: item.pair))
    )


def _unsupported_claim(claim_id: str) -> ClaimState:
    return ClaimState(
        claim_id=claim_id,
        support_count=0,
        refute_count=0,
        best_support_score=None,
        best_refute_score=None,
        supporting_observation_ids=(),
        refuting_observation_ids=(),
        status=ClaimStatus.UNSUPPORTED,
    )


def _unsupported_answer(answer_id: str) -> AnswerState:
    return AnswerState(
        answer_version_id=answer_id,
        required_claim_count=1,
        supported_count=0,
        unsupported_count=1,
        refuted_count=0,
        conflicted_count=0,
        status=AnswerStatus.UNSUPPORTED,
    )


def test_full_pair_audit_enumerates_the_exact_cartesian_product() -> None:
    judgments = tuple(
        _judgment(claim_id, chunk_id, VerificationLabel.NEUTRAL)
        for claim_id in ("c1", "c2")
        for chunk_id in ("p1", "p2")
    )
    first = run_full_pair_audit(
        event_id="event",
        registered_claim_ids=("c2", "c1"),
        inserted_active_chunk_ids=("p2", "p1"),
        judge=_table(*judgments),
    )
    second = run_full_pair_audit(
        event_id="event",
        registered_claim_ids=("c1", "c2"),
        inserted_active_chunk_ids=("p1", "p2"),
        judge=_table(*judgments),
    )

    assert first == second
    assert first.expected_pair_count == 4
    assert tuple(judgment.pair for judgment in first.judgments) == (
        PairKey("c1", "p1"),
        PairKey("c1", "p2"),
        PairKey("c2", "p1"),
        PairKey("c2", "p2"),
    )
    assert first.positive_pairs == ()
    assert first.manifest_id.startswith("full-pair-audit-")


def test_full_pair_cardinality_for_small_empty_and_nonempty_domains() -> None:
    for claim_count in range(5):
        for chunk_count in range(5):
            claims = tuple(f"c{index}" for index in range(claim_count))
            chunks = tuple(f"p{index}" for index in range(chunk_count))
            judgments = tuple(
                _judgment(claim_id, chunk_id, VerificationLabel.NEUTRAL)
                for claim_id in claims
                for chunk_id in chunks
            )
            result = run_full_pair_audit(
                event_id=f"event-{claim_count}-{chunk_count}",
                registered_claim_ids=tuple(reversed(claims)),
                inserted_active_chunk_ids=tuple(reversed(chunks)),
                judge=_table(*judgments),
            )

            assert result.expected_pair_count == claim_count * chunk_count
            assert len(result.judgments) == result.expected_pair_count


def test_full_pair_audit_rejects_missing_and_reordered_results() -> None:
    pair = PairKey("claim", "chunk")
    with pytest.raises(ValidationError, match="misses"):
        run_full_pair_audit(
            event_id="event",
            registered_claim_ids=(pair.claim_id,),
            inserted_active_chunk_ids=(pair.chunk_version_id,),
            judge=_table(),
        )

    class WrongPairJudge:
        def judge_pairs(
            self, pairs: tuple[PairKey, ...]
        ) -> tuple[PairJudgment, ...]:
            assert pairs == (pair,)
            return (_judgment("other", "chunk", VerificationLabel.NEUTRAL),)

    with pytest.raises(ValidationError, match="identity or order"):
        run_full_pair_audit(
            event_id="event",
            registered_claim_ids=(pair.claim_id,),
            inserted_active_chunk_ids=(pair.chunk_version_id,),
            judge=WrongPairJudge(),
        )


def test_audit_manifest_binds_exact_scores_and_split() -> None:
    original = _judgment("claim", "chunk", VerificationLabel.SUPPORT)
    changed_scores = replace(
        original,
        support_score=0.8,
        refute_score=0.1,
        neutral_score=0.1,
    )
    changed_split = replace(original, split_id="test")

    def manifest(judgment: PairJudgment) -> str:
        return run_full_pair_audit(
            event_id="event",
            registered_claim_ids=("claim",),
            inserted_active_chunk_ids=("chunk",),
            judge=_table(judgment),
        ).manifest_id

    manifests = {
        manifest(original),
        manifest(changed_scores),
        manifest(changed_split),
    }
    assert len(manifests) == 3


def test_deliberate_selective_miss_is_visible_without_admission_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A circular oracle would verify only the neutral decoy and fail this."""
    exploding_admission = ModuleType("groundloop.m4.admission")

    def explode(name: str) -> object:
        raise AssertionError(f"oracle touched selective admission attribute {name}")

    exploding_admission.__getattr__ = explode
    monkeypatch.setitem(sys.modules, "groundloop.m4.admission", exploding_admission)

    decoy = _judgment("c_decoy", "p_new", VerificationLabel.NEUTRAL)
    missed = _judgment("c_missed", "p_new", VerificationLabel.SUPPORT)
    audit = run_full_pair_audit(
        event_id="insert-event",
        registered_claim_ids=("c_decoy", "c_missed"),
        inserted_active_chunk_ids=("p_new",),
        judge=_table(decoy, missed),
    )

    selective_claims, selective_answers = recompute_grounding_states(
        claim_to_answer={"c_decoy": "a_decoy", "c_missed": "a_missed"},
        required_claim_ids=frozenset({"c_decoy", "c_missed"}),
        chunk_text_hashes={"p_new": HASH_A},
        judgments=(decoy,),
    )
    exhaustive_claims, exhaustive_answers = recompute_grounding_states(
        claim_to_answer={"c_decoy": "a_decoy", "c_missed": "a_missed"},
        required_claim_ids=frozenset({"c_decoy", "c_missed"}),
        chunk_text_hashes={"p_new": HASH_A},
        judgments=audit.judgments,
    )
    affected = compute_affected_sets(
        baseline_id="selective-to-exhaustive-admission",
        before_claim_states=selective_claims,
        after_claim_states=exhaustive_claims,
        before_answer_states=selective_answers,
        after_answer_states=exhaustive_answers,
    )

    assert audit.expected_pair_count == 2
    assert audit.positive_pairs == (PairKey("c_missed", "p_new"),)
    assert pair_positive_claim_ids(audit) == ("c_missed",)
    assert affected.materialized_state_claim_ids == ("c_missed",)
    assert affected.decision_summary_claim_ids == ("c_missed",)
    assert affected.status_claim_ids == ("c_missed",)
    assert affected.answer_status_ids == ("a_missed",)
    assert exhaustive_claims[1].status is ClaimStatus.SUPPORTED
    assert exhaustive_answers[1].status is AnswerStatus.VALID


def test_affected_sets_separate_provenance_summary_status_and_answer() -> None:
    before_claim = ClaimState(
        claim_id="claim",
        support_count=1,
        refute_count=0,
        best_support_score=0.9,
        best_refute_score=None,
        supporting_observation_ids=("old-observation",),
        refuting_observation_ids=(),
        status=ClaimStatus.SUPPORTED,
    )
    after_claim = ClaimState(
        claim_id="claim",
        support_count=1,
        refute_count=0,
        best_support_score=0.9,
        best_refute_score=None,
        supporting_observation_ids=("new-observation", "old-observation"),
        refuting_observation_ids=(),
        status=ClaimStatus.SUPPORTED,
    )
    answer = AnswerState(
        answer_version_id="answer",
        required_claim_count=1,
        supported_count=1,
        unsupported_count=0,
        refuted_count=0,
        conflicted_count=0,
        status=AnswerStatus.VALID,
    )
    affected = compute_affected_sets(
        baseline_id="provenance-only",
        before_claim_states=(before_claim,),
        after_claim_states=(after_claim,),
        before_answer_states=(answer,),
        after_answer_states=(answer,),
    )

    assert affected.materialized_state_claim_ids == ("claim",)
    assert affected.decision_summary_claim_ids == ()
    assert affected.status_claim_ids == ()
    assert affected.answer_status_ids == ()


def test_exhaustive_additive_delta_retains_working_witnesses() -> None:
    old_support = _judgment("claim", "p-old", VerificationLabel.SUPPORT)
    new_refute = _judgment("claim", "p-new", VerificationLabel.REFUTE)
    working_claims, working_answers = recompute_grounding_states(
        claim_to_answer={"claim": "answer"},
        required_claim_ids=frozenset({"claim"}),
        chunk_text_hashes={"p-old": HASH_A},
        judgments=(old_support,),
    )
    audit = run_full_pair_audit(
        event_id="insert-event",
        registered_claim_ids=("claim",),
        inserted_active_chunk_ids=("p-new",),
        judge=_table(new_refute),
    )
    result = compute_exhaustive_additive_delta(
        baseline_id="Bw-to-Bx",
        audit=audit,
        claim_to_answer={"claim": "answer"},
        required_claim_ids=frozenset({"claim"}),
        active_chunk_text_hashes={"p-old": HASH_A, "p-new": HASH_B},
        surviving_judgments=(old_support,),
        working_claim_states=working_claims,
        working_answer_states=working_answers,
    )

    assert result.claim_states[0].support_count == 1
    assert result.claim_states[0].refute_count == 1
    assert result.claim_states[0].status is ClaimStatus.CONFLICTED
    assert result.answer_states[0].status is AnswerStatus.CONFLICTED
    assert result.affected_sets.status_claim_ids == ("claim",)
    assert result.affected_sets.answer_status_ids == ("answer",)


def test_affected_sets_reject_mismatched_snapshot_domains() -> None:
    with pytest.raises(ValidationError, match="claim domains"):
        compute_affected_sets(
            baseline_id="bad-domain",
            before_claim_states=(_unsupported_claim("c1"),),
            after_claim_states=(_unsupported_claim("c2"),),
            before_answer_states=(_unsupported_answer("a"),),
            after_answer_states=(_unsupported_answer("a"),),
        )


def test_exact_brute_force_refresh_uses_distance_then_chunk_id() -> None:
    claims = (
        RefreshClaim("c2", "a2", True, (0.0, 1.0)),
        RefreshClaim("c1", "a1", True, (1.0, 0.0)),
    )
    chunks = (
        RefreshChunk("p-b", HASH_B, (1.0, 0.0)),
        RefreshChunk("p-c", hashlib.sha256(b"c").hexdigest(), (0.0, 1.0)),
        RefreshChunk("p-a", HASH_A, (1.0, 0.0)),
    )

    assert exact_brute_force_pairs(
        claims=claims, active_chunks=chunks, depth_k=1
    ) == (PairKey("c1", "p-a"), PairKey("c2", "p-c"))
    assert exact_brute_force_pairs(
        claims=claims, active_chunks=chunks, depth_k=2
    ) == (
        PairKey("c1", "p-a"),
        PairKey("c1", "p-b"),
        PairKey("c2", "p-a"),
        PairKey("c2", "p-c"),
    )


def test_snapshot_refresh_recomputes_direct_witness_states_from_scratch() -> None:
    claims = (
        RefreshClaim("c1", "answer", True, (1.0, 0.0)),
        RefreshClaim("c2", "answer", True, (0.0, 1.0)),
    )
    chunks = (
        RefreshChunk("p-support", HASH_A, (1.0, 0.0)),
        RefreshChunk("p-refute", HASH_B, (0.0, 1.0)),
    )
    result = run_snapshot_refresh(
        corpus_snapshot_hash=hashlib.sha256(b"snapshot").hexdigest(),
        refresh_policy_id="rho-exact-cosine-v1",
        depth_k=1,
        claims=claims,
        active_chunks=chunks,
        judge=_table(
            _judgment("c1", "p-support", VerificationLabel.SUPPORT),
            _judgment("c2", "p-refute", VerificationLabel.REFUTE),
        ),
    )

    assert result.retrieved_pairs == (
        PairKey("c1", "p-support"),
        PairKey("c2", "p-refute"),
    )
    assert tuple(state.status for state in result.claim_states) == (
        ClaimStatus.SUPPORTED,
        ClaimStatus.REFUTED,
    )
    assert result.answer_states[0].status is AnswerStatus.CONTRADICTED
    assert result.manifest_id.startswith("snapshot-refresh-")


def test_snapshot_refresh_candidate_churn_is_not_an_admission_miss() -> None:
    claim = RefreshClaim("claim", "answer", True, (1.0, 0.0))
    old_chunk = RefreshChunk("p-old", HASH_A, (0.8, 0.6))
    new_chunk = RefreshChunk("p-new", HASH_B, (1.0, 0.0))
    old_support = _judgment("claim", "p-old", VerificationLabel.SUPPORT)
    new_neutral = _judgment("claim", "p-new", VerificationLabel.NEUTRAL)
    before = run_snapshot_refresh(
        corpus_snapshot_hash=hashlib.sha256(b"before").hexdigest(),
        refresh_policy_id="rho",
        depth_k=1,
        claims=(claim,),
        active_chunks=(old_chunk,),
        judge=_table(old_support),
    )
    after = run_snapshot_refresh(
        corpus_snapshot_hash=hashlib.sha256(b"after").hexdigest(),
        refresh_policy_id="rho",
        depth_k=1,
        claims=(claim,),
        active_chunks=(old_chunk, new_chunk),
        judge=_table(new_neutral),
    )
    exhaustive_claims, exhaustive_answers = recompute_grounding_states(
        claim_to_answer={"claim": "answer"},
        required_claim_ids=frozenset({"claim"}),
        chunk_text_hashes={"p-old": HASH_A, "p-new": HASH_B},
        judgments=(old_support, new_neutral),
    )

    assert before.claim_states[0].status is ClaimStatus.SUPPORTED
    assert after.claim_states[0].status is ClaimStatus.UNSUPPORTED
    assert exhaustive_claims[0].status is ClaimStatus.SUPPORTED
    assert exhaustive_answers[0].status is AnswerStatus.VALID


def test_distinct_content_counts_do_not_inflate_on_duplicate_chunks() -> None:
    claim_states, _ = recompute_grounding_states(
        claim_to_answer={"claim": "answer"},
        required_claim_ids=frozenset({"claim"}),
        chunk_text_hashes={"p1": HASH_A, "p2": HASH_A},
        judgments=(
            _judgment("claim", "p1", VerificationLabel.SUPPORT),
            _judgment("claim", "p2", VerificationLabel.SUPPORT),
        ),
    )
    assert claim_states[0].support_count == 1
    assert len(claim_states[0].supporting_observation_ids) == 2


def test_oracle_import_graph_excludes_selective_and_runtime_paths() -> None:
    forbidden_modules = (
        "groundloop.incremental",
        "groundloop.m4.admission",
        "groundloop.m4.pipeline",
        "groundloop.m4.runtime",
    )
    forbidden_contract_names = {"AdmittedPair", "ChannelHit"}
    source_root = ROOT / "src/groundloop/m4/oracles"
    violations: list[str] = []
    for path in sorted(source_root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_modules):
                        violations.append(f"{path.name}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith(forbidden_modules):
                    violations.append(f"{path.name}: from {module}")
                if module == "groundloop.m4.contracts":
                    names = {alias.name for alias in node.names}
                    forbidden = names & forbidden_contract_names
                    if forbidden:
                        violations.append(
                            f"{path.name}: forbidden contract {sorted(forbidden)}"
                        )
    assert violations == []
