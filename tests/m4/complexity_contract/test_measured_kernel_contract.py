"""Executable guards for the narrow M4.7 complexity claims.

These tests intentionally protect only code-structural facts that can be
proved statically.  They do not infer asymptotic complexity from timings.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from pathlib import Path
from typing import Any

import groundloop.incremental as incremental_module
from groundloop.incremental import IncrementalMaintenanceEngine, MaintenanceStats
from groundloop.m4.evaluation_overlay import PostgresEvaluationOverlayStore
from groundloop.m4.persistence import PostgresM4RuntimeStore

ROOT = Path(__file__).resolve().parents[3]
PROOF = ROOT / "docs/workstreams/m4_7_complexity_proof/README.md"


def _called_attribute_names(callable_object: Any) -> set[str]:
    tree = ast.parse(textwrap.dedent(inspect.getsource(callable_object)))
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }


def test_point_runtime_mutations_do_not_call_full_projection_readers() -> None:
    forbidden = {"read_epoch", "read_book", "_transition_book"}
    methods = (
        PostgresM4RuntimeStore.start_attempt_point,
        PostgresM4RuntimeStore.mark_retryable_failure_point,
        PostgresM4RuntimeStore.complete_point,
        PostgresM4RuntimeStore.fail_epoch_point,
        PostgresM4RuntimeStore.seal_epoch_point,
    )

    for method in methods:
        assert _called_attribute_names(method).isdisjoint(forbidden), method.__name__


def test_evaluation_transition_has_no_epoch_wide_override_rebuild() -> None:
    transition_source = inspect.getsource(
        PostgresEvaluationOverlayStore.apply_transition
    )
    point_source = inspect.getsource(
        PostgresEvaluationOverlayStore._apply_override_delta
    )

    assert "_apply_override_delta" in transition_source
    assert "object_type = %s AND object_id = %s" in point_source
    assert (
        "DELETE FROM groundloop_m4_evaluation_override_counter\n"
        "                WHERE epoch_id = %s\n"
        "            "
    ) not in transition_source


def test_grounding_patch_exposes_bounded_score_index_work() -> None:
    fields = MaintenanceStats.__dataclass_fields__

    assert "score_index_shift_work" in fields
    assert "score_index_shift_upper_bound" in fields
    assert hasattr(IncrementalMaintenanceEngine, "claim_state")
    assert hasattr(IncrementalMaintenanceEngine, "answer_state")
    assert hasattr(IncrementalMaintenanceEngine, "certificate")


def test_score_index_is_avl_backed_without_list_shifts() -> None:
    index_source = inspect.getsource(incremental_module._ScoreRangeIndex)
    avl_source = inspect.getsource(incremental_module._AVLSet)

    assert "support_entries: _AVLSet" in index_source
    assert "refute_entries: _AVLSet" in index_source
    assert "_rebalance" in avl_source
    assert ".insert(" not in index_source
    assert ".pop(" not in index_source


def test_publication_head_precedes_bootstrap_history_fallback() -> None:
    source = inspect.getsource(PostgresM4RuntimeStore.open_epoch)
    head_branch = source.index("if publication_head is not None:")
    fallback_query = source.index("SELECT max(epoch_id)", head_branch)
    active_query = source.index("e.structural_status = 'committed'")
    migration = (ROOT / "migrations/004_m4_working_publication.sql").read_text(
        encoding="utf-8"
    )

    assert head_branch < fallback_query
    assert active_query < head_branch
    assert "CREATE UNIQUE INDEX groundloop_one_open_structural_epoch" in migration
    assert "structural_status = 'committed'" in migration
    assert "semantic_status IN ('pending', 'complete')" in migration


def test_proof_names_every_current_nonunit_hidden_term() -> None:
    proof = PROOF.read_text(encoding="utf-8")

    for term in ("G", "T_score", "sort(A)", "B"):
        assert f"`{term}`" in proof
    assert "faster than existing works" in proof
    assert "No. **Confidence: high.**" in proof
