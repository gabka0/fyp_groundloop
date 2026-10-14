"""Pure J33 predicate/identity tests, not production terminal-envelope evidence."""

from __future__ import annotations

import ast
import copy
import inspect
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from groundloop.errors import EventConflictError, InvalidEventError, ValidationError
from groundloop.incremental import IncrementalMaintenanceEngine
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind, stable_m4_digest
from groundloop.m4.evaluation_overlay import (
    EvaluationTransition,
    EvaluationTransitionKind,
    _declaration_hash,
    _sha256_json,
    _transition_payload,
)
from groundloop.m4.pipeline import StructuralPayload
from groundloop.m5.runtime import postgres_document_seal as seal
from groundloop.m5.runtime import postgres_readiness as readiness
from tests.helpers import make_repo, register_answer


def _point(revision: int = 3) -> tuple[dict[str, Any], dict[str, Any], Any]:
    update = CorpusUpdateIdentity("更新☃", "a" * 64, UpdateKind.DELETE, 30, "policy")
    base = dict(
        epoch_id=31,
        event_id=update.event_id,
        payload_hash=update.payload_hash,
        revision=revision,
        structural_status="committed",
        semantic_status="complete",
        evaluation_state="complete",
        publication_mode="provisional",
        sealed_at=None,
    )
    runtime = dict(
        epoch_id=31,
        structural_event_id=update.event_id,
        revision=revision,
        runtime_state="semantic_complete",
        terminal_at=None,
        expected_previous_published_epoch_id=30,
        candidate_policy_id="policy",
        requirement_root_set_hash="b" * 64,
        open_work_count=0,
        open_scope_count=0,
        blocking_failure_count=0,
    )
    return base, runtime, update


@pytest.mark.parametrize("revision", (3, 4, 179))
def test_complete_point_does_not_equate_m5_revision_to_evaluation_clock(
    revision: int,
) -> None:
    base, runtime, update = _point(revision)
    seal._check_complete_point(
        base, runtime, epoch_id=31, expected_revision=revision, update=update
    )


@pytest.mark.parametrize(
    "surface,field,value",
    (
        ("base", "epoch_id", 32),
        ("base", "event_id", "other"),
        ("base", "payload_hash", "c" * 64),
        ("base", "revision", 4),
        ("base", "structural_status", "failed"),
        ("base", "semantic_status", "pending"),
        ("base", "evaluation_state", "pending"),
        ("base", "publication_mode", "strict"),
        ("base", "sealed_at", object()),
        ("runtime", "epoch_id", 32),
        ("runtime", "structural_event_id", "other"),
        ("runtime", "revision", 4),
        ("runtime", "runtime_state", "semantic_pending"),
        ("runtime", "runtime_state", "sealed"),
        ("runtime", "runtime_state", "failed"),
        ("runtime", "terminal_at", object()),
        ("runtime", "expected_previous_published_epoch_id", 29),
        ("runtime", "candidate_policy_id", "other"),
        ("runtime", "open_work_count", 1),
        ("runtime", "open_scope_count", 1),
        ("runtime", "blocking_failure_count", 1),
    ),
)
def test_each_complete_point_mutation_rejects(
    surface: str, field: str, value: Any
) -> None:
    base, runtime, update = _point()
    (base if surface == "base" else runtime)[field] = value
    with pytest.raises((EventConflictError, InvalidEventError, ValidationError)):
        seal._check_complete_point(
            base, runtime, epoch_id=31, expected_revision=3, update=update
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("epoch_id", True),
        ("epoch_id", 0),
        ("epoch_id", "31"),
        ("expected_revision", True),
        ("expected_revision", 2),
        ("expected_revision", "3"),
        ("update", None),
    ),
)
def test_invalid_input_rejects_before_cursor_or_ports_use(
    field: str, value: Any
) -> None:
    args = dict(
        ports=None, cursor=None, epoch_id=31, expected_revision=3, update=_point()[2]
    )
    args[field] = value
    with pytest.raises(ValidationError):
        seal._stage_job_free_document_seal_local(**args)


class _CounterCursor:
    def __init__(self, counter: Any, transitions: Any) -> None:
        self.counter, self.transitions = counter, transitions
        self.calls: list[tuple[Any, Any]] = []

    def execute(self, query: Any, params: Any = None) -> Any:
        self.calls.append((query, params))
        return self

    def fetchone(self) -> Any:
        return self.counter

    def fetchall(self) -> Any:
        return self.transitions


def _terminal() -> tuple[Any, Any, EvaluationTransition]:
    transition = EvaluationTransition(
        stable_m4_digest("m4-evaluation-seal-v1", "31"),
        1,
        kind=EvaluationTransitionKind.SEAL,
    )
    return (
        (_declaration_hash(31, 1, 30, 0), "sealed", "complete", 31, 0, 2),
        [
            (
                transition.transition_id,
                _sha256_json(_transition_payload(transition, ())),
                "seal",
                1,
                2,
                0,
            )
        ],
        transition,
    )


def test_counter2_proof_uses_existing_m4_recipes_not_m5_revision() -> None:
    counter, transitions, transition = _terminal()
    cursor = _CounterCursor(counter, transitions)
    seal._check_staged_evaluation(cursor, 31, 30, transition)  # type: ignore[arg-type]
    assert len(cursor.calls) == 2
    assert all(
        str(q).lstrip().startswith("SELECT ") and p == (31,) for q, p in cursor.calls
    )


@pytest.mark.parametrize(
    "index,value",
    (
        (0, "c" * 64),
        (1, "active"),
        (2, "pending"),
        (3, 30),
        (4, 1),
        (5, 4),
    ),
)
def test_each_staged_counter_coordinate_rejects(index: int, value: Any) -> None:
    counter, transitions, transition = _terminal()
    changed = list(counter)
    changed[index] = value
    with pytest.raises(EventConflictError):
        seal._check_staged_evaluation(
            _CounterCursor(tuple(changed), transitions), 31, 30, transition
        )  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "index,value",
    (
        (0, "other"),
        (1, "c" * 64),
        (2, "delta"),
        (3, 3),
        (4, 4),
        (5, 1),
    ),
)
def test_each_staged_ledger_coordinate_rejects(index: int, value: Any) -> None:
    counter, transitions, transition = _terminal()
    changed = list(transitions[0])
    changed[index] = value
    with pytest.raises(EventConflictError):
        seal._check_staged_evaluation(
            _CounterCursor(counter, [tuple(changed)]), 31, 30, transition
        )  # type: ignore[arg-type]


@pytest.mark.parametrize("rows", ([], [(), ()]))
def test_missing_or_extra_evaluation_transition_rejects(rows: Any) -> None:
    counter, _, transition = _terminal()
    with pytest.raises(EventConflictError):
        seal._check_staged_evaluation(_CounterCursor(counter, rows), 31, 30, transition)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "visibility,committed",
    (
        (("39", "40", "41"), True),
        (("40", "40", "40"), False),
        (("41", "40", "40"), False),
    ),
)
def test_complete_proof_requires_prior_transaction_and_exact_artifact(
    visibility: Any,
    committed: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base, runtime, _ = _point()
    artifact = readiness._derive_artifact(
        base, runtime, target="semantic_complete", revision=2
    )
    contribution = dict(
        epoch_id=31,
        contribution_kind="semantic_readiness",
        source_id="semantic_complete",
        applied_revision=3,
        source_identity_hash=artifact.source_identity_hash,
        contribution_key_digest=artifact.anchor.contribution_key_digest,
        work_digest=artifact.work.work_digest,
    ) | dict(
        zip(artifact.work.counter_names(), artifact.work.counter_values(), strict=True)
    )
    monkeypatch.setattr(readiness, "_read_contribution", lambda *a: contribution)
    cursor = _CounterCursor(visibility, [])
    if not committed:
        with pytest.raises(InvalidEventError, match="separately committed"):
            seal._require_committed_readiness(cursor, "captured", base, runtime)  # type: ignore[arg-type]
    else:
        seal._require_committed_readiness(cursor, "captured", base, runtime)  # type: ignore[arg-type]
    original = copy.deepcopy(contribution)
    contribution["applied_revision"] = 4
    with pytest.raises(EventConflictError):
        seal._require_committed_readiness(cursor, "captured", base, runtime)  # type: ignore[arg-type]
    contribution.clear()
    contribution.update(original)
    monkeypatch.setattr(readiness, "_read_contribution", lambda *a: None)
    with pytest.raises(InvalidEventError, match="lacks genuine"):
        seal._require_committed_readiness(cursor, "captured", base, runtime)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "row,status,expected,status_calls",
    (
        (("39", "40", "41"), None, True, 0),
        (("40", "40", "40"), None, False, 0),  # quiet own top equals xmax
        (("41", "40", "40"), None, False, 0),  # quiet own sub exceeds xmax
        (("41", "40", "50"), "in progress", False, 1),
        (("41", "40", "50"), "committed", True, 1),
        (("41", "40", "50"), "aborted", False, 1),
        (("41", "40", "50"), None, False, 1),
        ((str((1 << 32) - 1), str((1 << 32) + 5), str((1 << 32) + 6)), None, True, 0),
        (("5", str((1 << 32) + 5), str((1 << 32) + 5)), None, False, 0),
        (("6", str((1 << 32) + 5), str((1 << 32) + 8)), "in progress", False, 1),
        (("6", str((1 << 32) + 5), str((1 << 32) + 8)), "committed", True, 1),
        (("2", "40", "40"), None, True, 0),
        (("1", "40", "40"), None, True, 0),
        (None, None, False, 0),
        (("40", None, "41"), None, False, 0),
        (("0", "40", "41"), None, False, 0),
        ((str(1 << 32), "40", "41"), None, False, 0),
        (("40", "2", "41"), None, False, 0),
        (("40", "40", "0"), None, False, 0),
    ),
)
def test_prior_commit_scalar_vectors_cover_subtransactions_and_wrap(
    row: Any,
    status: Any,
    expected: bool,
    status_calls: int,
) -> None:
    cursor = _CounterCursor((status,), [])
    assert seal._prior_committed_xmin(cursor, row) is expected  # type: ignore[arg-type]
    assert len(cursor.calls) == status_calls
    if status_calls:
        full = (
            int(row[1])
            + ((int(row[0]) - (int(row[1]) % (1 << 32)) + (1 << 31)) % (1 << 32))
            - (1 << 31)
        )
        assert cursor.calls[0][1] == (str(full),)


def test_adapter_has_no_outer_or_legacy_wrapper_or_cache_adoption() -> None:
    source = inspect.getsource(seal)
    tree = ast.parse(source)
    attributes = {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert attributes.isdisjoint(
        {
            "transaction",
            "commit",
            "rollback",
            "_stage_direct_seal_local",
            "_assert_incremental_evaluation",
            "_adopt_direct_seal_cache",
            "_adopt_direct_working_cache",
            "_hydrate_direct_working_cache",
            "compute_reference_states",
            "_assert_grounding_equality",
        }
    )
    assert not any(
        isinstance(node, (ast.Global, ast.Nonlocal)) for node in ast.walk(tree)
    )
    assert not hasattr(seal, "__all__")
    order = [
        source.index("ports." + name)
        for name in (
            "_promote_structural_overlay",
            "_promote_observation_currency",
            "_publish_grounding_states",
            "evaluation_store.apply_transition_local",
        )
    ]
    assert order == sorted(order)


@pytest.mark.parametrize(
    "surface,index,value",
    (
        ("claim", 0, "absent"),
        ("claim", 1, 99),
        ("claim", 2, 99),
        ("claim", 3, 0.9),
        ("claim", 4, 0.9),
        ("claim", 5, ["extra"]),
        ("claim", 6, ["extra"]),
        ("claim", 7, "supported"),
        ("answer", 0, "absent"),
        ("answer", 1, 99),
        ("answer", 2, 99),
        ("answer", 3, 99),
        ("answer", 4, 99),
        ("answer", 5, 99),
        ("answer", 6, "valid"),
        ("claim_cache_id", 0, "another-claim"),
        ("answer_cache_id", 0, "another-answer"),
    ),
)
def test_controlled_consumed_projection_checks_each_cache_value(
    surface: str,
    index: int,
    value: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pure predicate vectors, not reachable nonempty job-free SQL histories."""
    repository = make_repo()
    register_answer(repository)
    engine = IncrementalMaintenanceEngine.from_repository(repository)
    claim, answer = engine.claim_state("c1"), engine.answer_state("a1")
    claim_row = [
        "c1",
        claim.support_count,
        claim.refute_count,
        claim.best_support_score,
        claim.best_refute_score,
        list(claim.supporting_observation_ids),
        list(claim.refuting_observation_ids),
        claim.status.value,
    ]
    answer_row = [
        "a1",
        answer.required_claim_count,
        answer.supported_count,
        answer.unsupported_count,
        answer.refuted_count,
        answer.conflicted_count,
        answer.status.value,
    ]
    ports = SimpleNamespace(
        _active_epoch_id=31, _working_repository=repository, _working_engine=engine
    )
    monkeypatch.setattr(
        seal.postgres_matching,
        "_document_direct_policy",
        lambda *args: repository.current_policy(),
    )

    class ProjectionCursor:
        def execute(self, query: str, params: Any = None) -> Any:
            assert query.startswith("SELECT ") and params == (31,)
            self.query = query
            return self

        def fetchall(self) -> Any:
            return (
                [tuple(claim_row)]
                if "working_claim_state" in self.query
                else [tuple(answer_row)]
            )

    cursor = ProjectionCursor()
    args = dict(epoch_id=31, payload=StructuralPayload(), decision_policy_version="k1")
    seal._check_consumed_cache(cursor, ports, **args)  # type: ignore[arg-type]
    if surface == "claim_cache_id":
        engine._claim_states["c1"] = replace(claim, claim_id=value)
    elif surface == "answer_cache_id":
        engine._answer_states["a1"] = replace(answer, answer_version_id=value)
    else:
        (claim_row if surface == "claim" else answer_row)[index] = value
    with pytest.raises(EventConflictError):
        seal._check_consumed_cache(cursor, ports, **args)  # type: ignore[arg-type]
