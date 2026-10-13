"""O33 pure guards/recipe checks, not live composition acceptance."""

from __future__ import annotations

import ast
import hashlib
import inspect
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg.pq import TransactionStatus

from groundloop.errors import EventConflictError, ValidationError
from groundloop.events import DeleteDocumentVersionEvent
from groundloop.m4.application import (
    ApplicationExecutionPolicy,
    DynamicEventPlan,
    OpenEventReceipt,
)
from groundloop.m4.contracts import CorpusUpdateIdentity, UpdateKind
from groundloop.m4.evaluation_overlay import PostgresEvaluationOverlayStore
from groundloop.m4.persistence import PostgresM4RuntimeStore
from groundloop.m4.pipeline import M4ExecutionMode, PostgresM4ApplicationPorts
from groundloop.m5.events import legacy_event_payload_digest
from groundloop.m5.runtime import digests, persistence
from groundloop.m5.runtime.contracts import (
    ActiveChunkSnapshot,
    M5RuntimeOperationalConfig,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
)


def test_every_preexisting_persistence_body_is_unchanged() -> None:
    """Pin all original AST bodies/constants/classes, not merely signatures."""
    source = Path(inspect.getfile(persistence)).read_text()
    tree = ast.parse(source)
    tree.body = [
        node
        for node in tree.body
        if not isinstance(node, (ast.Import, ast.ImportFrom))
        and not (isinstance(node, ast.FunctionDef) and node.name.startswith("_o33_"))
        and not (isinstance(node, ast.ClassDef) and node.name == "_O33ByteRegion")
    ]
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "PostgresM5RuntimeStore":
            node.body = [
                child
                for child in node.body
                if not (
                    isinstance(child, ast.FunctionDef)
                    and child.name
                    in {
                        "_activate_document_open_prerequisite",
                        "_open_document_open_prerequisite",
                    }
                )
            ]
    assert hashlib.sha256(
        ast.dump(tree, include_attributes=False).encode()
    ).hexdigest() == (
        "243428dc53a7152359b6c14dc13b3b9356146110502e9da5f0056df2aa333e4f"
    )


@pytest.mark.parametrize(
    "status", [s for s in TransactionStatus if s is not TransactionStatus.IDLE]
)
def test_idle_guard_rejects_every_nonidle_status(status: TransactionStatus) -> None:
    connection = SimpleNamespace(info=SimpleNamespace(transaction_status=status))
    with pytest.raises(ValidationError, match="idle owned outer transaction"):
        persistence._o33_require_idle(connection)


def test_idle_guard_allows_idle() -> None:
    connection = SimpleNamespace(
        info=SimpleNamespace(transaction_status=TransactionStatus.IDLE)
    )
    persistence._o33_require_idle(connection)


@pytest.mark.parametrize(
    "kind,point",
    [
        (
            "requirement",
            {
                "requirement_version_id": "requirement-α",
                "witness_hashes": ("a" * 64,),
                "supporting_observation_ids": ("observation-β",),
                "witness_count": 1,
                "satisfied": True,
                "decision_policy_version": "policy-γ",
            },
        ),
        (
            "group",
            {
                "group_version_id": "group-α",
                "requirement_count": 2,
                "satisfied_count": 1,
                "matching_size": 1,
                "complete": False,
                "decision_policy_version": "policy-γ",
                "certificate_digest": None,
            },
        ),
        (
            "claim",
            {
                "claim_id": "claim-α",
                "support_count": 1,
                "refute_count": 0,
                "best_support_score": 0.75,
                "best_refute_score": None,
                "supporting_observation_ids": ("observation-β",),
                "refuting_observation_ids": (),
                "complete_group_count": 1,
                "complete_group_ids": ("group-δ",),
                "status": "supported",
                "decision_policy_version": "policy-γ",
                "certificate_digest": "b" * 64,
            },
        ),
        (
            "answer",
            {
                "answer_version_id": "answer-α",
                "required_claim_count": 1,
                "supported_count": 1,
                "unsupported_count": 0,
                "refuted_count": 0,
                "conflicted_count": 0,
                "status": "grounded",
            },
        ),
    ],
)
def test_diagnostic_state_preimages_match_unchanged_frozen_recipes(
    kind: str, point: dict[str, object]
) -> None:
    recipe = getattr(digests, f"{kind}_state_artifact_digest")
    encoded = persistence._o33_state_preimage(kind, point)
    assert hashlib.sha256(encoded).hexdigest() == recipe(**point)


def test_private_open_has_no_caller_action_or_work_authority() -> None:
    signature = inspect.signature(
        persistence.PostgresM5RuntimeStore._open_document_open_prerequisite
    )
    assert not {
        "direct_stage",
        "structural_action",
        "work",
        "branch",
        "callback",
    }.intersection(signature.parameters)


@pytest.mark.parametrize(
    "outcome", ["sealed", "failed", "half-sealed", "half-failed", "loader-error"]
)
def test_controlled_terminal_routing_before_hydration_only(
    outcome: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Boundary doubles ONLY: no genuine terminal envelope/seal is claimed."""
    queries: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, _params=None):
            assert query.startswith("SELECT ")
            queries.append(query)
            return self

        def fetchone(self):
            if "runtime_mode" in queries[-1]:
                return ("m5_active",)
            if "activation" in queries[-1]:
                return ("activation",)
            return (1,)

    class Connection:
        info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)

        def transaction(self):
            return nullcontext()

        def cursor(self):
            return Cursor()

    connection = Connection()
    ports = object.__new__(PostgresM4ApplicationPorts)
    ports.connection = connection
    ports.execution_mode = M4ExecutionMode.MEASURED
    ports.runtime_store = object.__new__(PostgresM4RuntimeStore)
    ports.runtime_store._connection = connection
    ports.runtime_store._audit_transitions = False
    ports.evaluation_store = object.__new__(PostgresEvaluationOverlayStore)
    ports.evaluation_store._connection = connection
    store = persistence.PostgresM5RuntimeStore(connection)
    event = DeleteDocumentVersionEvent("controlled-terminal", "old-doc")
    payload = legacy_event_payload_digest(event)
    plan = M5TypedEventPlan(
        event.event_id,
        event,
        payload,
        DynamicEventPlan(
            CorpusUpdateIdentity(
                event.event_id, payload, UpdateKind.DELETE, 1, "a" * 64
            ),
            (),
            ("chunk",),
            (),
            "registry",
        ),
        "a" * 64,
        "a" * 64,
        RequirementRegistrySnapshot.build(()),
        ActiveChunkSnapshot.build(()),
        1,
    )
    sealed = outcome in {"sealed", "half-sealed", "loader-error"}
    existing = OpenEventReceipt(
        epoch_id=2,
        replayed=True,
        already_sealed=sealed,
        publication_id="controlled-publication" if sealed else None,
        already_failed=not sealed,
        failure_reason=None if sealed else "controlled-failure",
    )
    monkeypatch.setattr(
        persistence, "_o33_schema_authority", lambda _cursor: "controlled"
    )
    monkeypatch.setattr(store, "_read_existing_open", lambda *_a, **_kw: existing)
    loaded: list[str] = []

    def terminal(*_a, **_kw):
        loaded.append("canonical-loader")
        if outcome == "loader-error":
            raise ValidationError("controlled canonical envelope rejection")
        return None if outcome.startswith("half-") else object()

    monkeypatch.setattr(persistence, "_load_terminal_result", terminal)

    def forbidden(*_a, **_kw):
        pytest.fail("controlled terminal path crossed into hydration/producer/cache")

    monkeypatch.setattr(
        persistence._o33_withdrawal, "_load_retained_document_open", forbidden
    )
    monkeypatch.setattr(persistence, "_load_candidate_manifest", forbidden)
    monkeypatch.setattr(persistence, "_o33_complete_structural_work", forbidden)
    monkeypatch.setattr(
        persistence._o33_matching, "apply_matching_transition", forbidden
    )
    kwargs = dict(
        ports=ports,
        execution_policy=ApplicationExecutionPolicy("a" * 64, "b" * 64, "c" * 64),
        supplied_direct_open=None,
        supplied_requirement_withdrawal=None,
        requirement_roots=(),
        requirement_root_set_hash="0" * 64,
        recovery_operational_config=M5RuntimeOperationalConfig.build(60_000),
        recovery_root_fallback_required={},
    )
    if outcome.startswith("half-"):
        with pytest.raises(EventConflictError, match="partial terminal declaration"):
            store._open_document_open_prerequisite(plan, **kwargs)
    elif outcome == "loader-error":
        with pytest.raises(ValidationError, match="canonical envelope rejection"):
            store._open_document_open_prerequisite(plan, **kwargs)
    else:
        assert store._open_document_open_prerequisite(plan, **kwargs) is existing
    assert loaded == ["canonical-loader"] and len(queries) == 4
