"""Pure routing evidence for the M5-D29 hydration terminal cutoff.

This lane deliberately uses only the existing transaction-aware fake ports.
It proves application ordering and provenance checks; planner-internal
declaration validation and pre-open epoch custody remain later-lane evidence.
"""

from __future__ import annotations

import ast
import inspect
from copy import copy
from dataclasses import dataclass, field, fields, replace
from typing import TypeVar, cast

import pytest
from m5.runtime.fake_ports import (
    FakeDirect,
    FakeHarness,
    FakeMeasurements,
    FakeRuntime,
    FakeStructural,
    FakeTypedWorld,
    make_harness,
    make_repository,
    make_typed_plan,
    sha,
)

from groundloop.errors import ValidationError
from groundloop.events import (
    ChunkInput,
    DeleteDocumentVersionEvent,
    InsertDocumentEvent,
    ReplaceDocumentVersionEvent,
    apply_event,
)
from groundloop.m4.application import OpenEventReceipt, StructuralWithdrawal
from groundloop.m4.contracts import DiscoveryScope as M4DiscoveryScope
from groundloop.m4.contracts import LogicalJobSpec as M4LogicalJobSpec
from groundloop.m4.pipeline import StructuralPayload
from groundloop.m5.domain import (
    ConstructionKind,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
)
from groundloop.m5.events import (
    RegisterGroupEvent,
    ReplaceGroupEvent,
    RetireGroupEvent,
)
from groundloop.m5.runtime import application as application_module
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.application import (
    M5CandidatePolicyPort,
    M5DirectExecutionReceipt,
    M5DirectOpenPlan,
    M5DirectSubgraphPort,
    M5DiscoveryExecution,
    M5RequirementRootDeclaration,
    M5RuntimeReadPort,
    M5TerminalInvocationTelemetry,
    M5TypedApplication,
    M5TypedStructuralPort,
    M5VerifierExecution,
    _M5D29HydrationTerminalCutoff,
)
from groundloop.m5.runtime.contracts import (
    M5EventRunResult,
    M5JobLease,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementFallbackKey,
    M5RequirementWithdrawalPlan,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeWork,
    M5TypedEventPlan,
)


class _SyntheticStop(RuntimeError):
    """Stop a fake invocation after its open transaction has committed."""


class _CutoffSubclass(_M5D29HydrationTerminalCutoff):
    """Deliberately invalid D29 signal subtype."""


_T = TypeVar("_T")


def _unsafe_set(value: _T, **changes: object) -> _T:
    changed = copy(value)
    for name, replacement in changes.items():
        object.__setattr__(changed, name, replacement)
    return changed


def _signal(mode: str = "exact") -> _M5D29HydrationTerminalCutoff:
    if mode == "exact":
        return _M5D29HydrationTerminalCutoff()
    if mode == "subclass":
        return _CutoffSubclass()
    signal = _M5D29HydrationTerminalCutoff()
    if mode == "nonempty":
        signal.args = ("forbidden-payload",)
        return signal
    if mode == "field":
        signal.__dict__["payload"] = "forbidden-payload"
        return signal
    raise AssertionError(f"unknown signal mode: {mode}")


def _group(
    name: str,
    *,
    group_version_id: str,
    family_id: str,
    supersedes_group_version_id: str | None = None,
    supersedes_requirement_version_id: str | None = None,
) -> EvidenceGroupVersion:
    requirement_id = f"requirement:{name}"
    return EvidenceGroupVersion(
        group_version_id=group_version_id,
        group_family_id=family_id,
        owner_claim_id="claim-required",
        requirements=(
            EvidenceRequirementVersion(
                requirement_version_id=requirement_id,
                group_version_id=group_version_id,
                ordinal=0,
                requirement_text=f"evidence required for {name}",
                supersedes_requirement_version_id=(supersedes_requirement_version_id),
            ),
        ),
        construction_kind=ConstructionKind.CONTROLLED,
        construction_source_id="d29-lane-a-test-v1",
        supersedes_group_version_id=supersedes_group_version_id,
    )


def _seed_group(repository: object, group: EvidenceGroupVersion) -> None:
    from groundloop.m5.repository import M5Repository

    assert isinstance(repository, M5Repository)
    point = repository.begin_event_epoch()
    repository.register_group(group, point.epoch_id)


def _seed_document(repository: object, name: str) -> tuple[str, str]:
    from groundloop.m5.repository import M5Repository

    assert isinstance(repository, M5Repository)
    version_id = f"version:{name}:old"
    chunk_id = f"chunk:{name}:old"
    apply_event(
        repository.base,
        InsertDocumentEvent(
            event_id=f"seed-document:{name}",
            document_id=f"document:{name}",
            document_version_id=version_id,
            content_hash=sha(f"content:{name}:old"),
            chunks=(ChunkInput(chunk_id, 0, f"old evidence for {name}"),),
        ),
    )
    _ = repository.current_point
    return version_id, chunk_id


def _document_case(
    kind: str,
    name: str,
    *,
    with_requirement: bool = False,
) -> tuple[FakeHarness, M5TypedEventPlan, str | None]:
    repository = make_repository()
    requirement_id: str | None = None
    if with_requirement:
        group = _group(
            f"{name}:active",
            group_version_id=f"group:{name}:active",
            family_id=f"family:{name}:active",
        )
        _seed_group(repository, group)
        requirement_id = group.requirements[0].requirement_version_id

    old_version_id: str | None = None
    if kind in {"delete", "replace"}:
        old_version_id, _ = _seed_document(repository, name)

    event: (
        InsertDocumentEvent | DeleteDocumentVersionEvent | ReplaceDocumentVersionEvent
    )
    if kind == "insert":
        event = InsertDocumentEvent(
            event_id=f"event:{name}:insert",
            document_id=f"document:{name}",
            document_version_id=f"version:{name}:new",
            content_hash=sha(f"content:{name}:new"),
            chunks=(ChunkInput(f"chunk:{name}:new", 0, f"new evidence for {name}"),),
        )
    elif kind == "delete":
        assert old_version_id is not None
        event = DeleteDocumentVersionEvent(f"event:{name}:delete", old_version_id)
    elif kind == "replace":
        assert old_version_id is not None
        event = ReplaceDocumentVersionEvent(
            f"event:{name}:replace",
            f"document:{name}",
            old_version_id,
            f"version:{name}:new",
            sha(f"content:{name}:new"),
            (ChunkInput(f"chunk:{name}:new", 0, f"new evidence for {name}"),),
        )
    else:
        raise AssertionError(f"unknown document kind: {kind}")

    harness = make_harness(repository)
    return harness, make_typed_plan(harness.world, event), requirement_id


def _group_case(kind: str, name: str) -> tuple[FakeHarness, M5TypedEventPlan]:
    repository = make_repository()
    family_id = f"family:{name}"
    predecessor = _group(
        f"{name}:old",
        group_version_id=f"group:{name}:old",
        family_id=family_id,
    )
    if kind != "register":
        _seed_group(repository, predecessor)

    event: RegisterGroupEvent | ReplaceGroupEvent | RetireGroupEvent
    if kind == "register":
        event = RegisterGroupEvent(
            f"event:{name}:register",
            _group(
                f"{name}:new",
                group_version_id=f"group:{name}:new",
                family_id=family_id,
            ),
        )
    elif kind == "replace":
        event = ReplaceGroupEvent(
            f"event:{name}:replace",
            predecessor.group_version_id,
            _group(
                f"{name}:new",
                group_version_id=f"group:{name}:new",
                family_id=family_id,
                supersedes_group_version_id=predecessor.group_version_id,
                supersedes_requirement_version_id=(
                    predecessor.requirements[0].requirement_version_id
                ),
            ),
        )
    elif kind == "retire":
        event = RetireGroupEvent(f"event:{name}:retire", predecessor.group_version_id)
    else:
        raise AssertionError(f"unknown group kind: {kind}")

    harness = make_harness(repository)
    return harness, make_typed_plan(harness.world, event)


@dataclass(slots=True)
class _ObservedStructural(FakeStructural):
    plans: list[M5RequirementWithdrawalPlan] = field(default_factory=list)

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        plan = FakeStructural.plan_exact_requirement_withdrawal(self, event)
        self.plans.append(plan)
        return plan

    def open_typed_event_atomically(
        self,
        event: M5TypedEventPlan,
        direct_payload: StructuralPayload | None,
        direct_withdrawal: StructuralWithdrawal | None,
        requirement_withdrawal: M5RequirementWithdrawalPlan,
        direct_roots: tuple[M4LogicalJobSpec, ...],
        direct_scopes: tuple[M4DiscoveryScope, ...],
        requirement_roots: tuple[M5RequirementRootDeclaration, ...],
        requirement_root_set_hash: str,
    ) -> OpenEventReceipt:
        self.world.operation_log.append("call:typed-open")
        return FakeStructural.open_typed_event_atomically(
            self,
            event,
            direct_payload,
            direct_withdrawal,
            requirement_withdrawal,
            direct_roots,
            direct_scopes,
            requirement_roots,
            requirement_root_set_hash,
        )


@dataclass(slots=True)
class _ObservedDirect(FakeDirect):
    plan_calls: int = 0
    run_calls: int = 0

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan:
        self.plan_calls += 1
        return FakeDirect.plan_direct_open(self, event)

    def run_pending_direct(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        self.run_calls += 1
        self.world.operation_log.append("call:direct-runner")
        return FakeDirect.run_pending_direct(
            self,
            epoch_id,
            expected_revision,
            event,
            open_receipt,
        )


@dataclass(slots=True)
class _ObservedRuntime(FakeRuntime):
    result_reads: int = 0
    revision_reads: int = 0

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        self.result_reads += 1
        return FakeRuntime.read_typed_event_result(self, event_id, payload_hash)

    def current_revision(self, epoch_id: int) -> int:
        self.revision_reads += 1
        self.world.operation_log.append("read:current-revision")
        return FakeRuntime.current_revision(self, epoch_id)


@dataclass(slots=True)
class _CrashAtRevisionRuntime(_ObservedRuntime):
    stopped: bool = False

    def current_revision(self, epoch_id: int) -> int:
        revision = _ObservedRuntime.current_revision(self, epoch_id)
        if not self.stopped:
            self.stopped = True
            raise _SyntheticStop("stop after committed typed open")
        return revision


def _install_ports(
    harness: FakeHarness,
    *,
    structural: FakeStructural | None = None,
    direct: FakeDirect | None = None,
    runtime: FakeRuntime | None = None,
) -> None:
    if structural is not None:
        harness.structural = structural
        harness.application.structural = structural
    if direct is not None:
        harness.direct = direct
        harness.application.direct = direct
    if runtime is not None:
        harness.runtime = runtime
        harness.application.runtime = runtime
        harness.application.runtime_reads = runtime


def _prepare_nonterminal_replay(
    harness: FakeHarness, plan: M5TypedEventPlan
) -> OpenEventReceipt:
    runtime = _CrashAtRevisionRuntime(harness.world)
    _install_ports(harness, runtime=runtime)
    with pytest.raises(_SyntheticStop, match="committed typed open"):
        harness.application.run_event(plan)
    epoch = harness.world.epochs_by_event[plan.structural_event_id]
    assert epoch.terminal_result is None
    assert epoch.current_open_receipt is not None
    opened = epoch.current_open_receipt
    harness.world.operation_log.clear()
    return opened


def _terminalize_failed(world: FakeTypedWorld, event_id: str) -> M5EventRunResult:
    epoch = world.epochs_by_event[event_id]
    return FakeRuntime(world).fail_typed_epoch_atomically(
        epoch.epoch_id,
        epoch.revision,
        M5RunFailureReason.RETRIEVAL_ERROR,
        M5RuntimeWork(),
    )


@dataclass(slots=True)
class _CutoffStructural(_ObservedStructural):
    cutoff_call: int = 1
    signal_mode: str = "exact"
    terminalize: bool = False

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        plan = _ObservedStructural.plan_exact_requirement_withdrawal(self, event)
        if len(self.plans) == self.cutoff_call:
            if self.terminalize:
                _terminalize_failed(self.world, event.structural_event_id)
            raise _signal(self.signal_mode)
        return plan


@dataclass(slots=True)
class _CutoffDirect(_ObservedDirect):
    cutoff_site: str = "plan"
    signal_mode: str = "exact"
    terminalize: bool = False

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan:
        plan = _ObservedDirect.plan_direct_open(self, event)
        if self.cutoff_site == "plan":
            raise _signal(self.signal_mode)
        return plan

    def run_pending_direct(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        self.run_calls += 1
        self.world.operation_log.append("call:direct-runner")
        if self.cutoff_site == "run":
            if self.terminalize:
                _terminalize_failed(self.world, event.structural_event_id)
            raise _signal(self.signal_mode)
        return FakeDirect.run_pending_direct(
            self,
            epoch_id,
            expected_revision,
            event,
            open_receipt,
        )


@dataclass(slots=True)
class _ResultReadRuntime(_ObservedRuntime):
    hide_first: bool = False
    corruption: str = "canonical"

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        result = _ObservedRuntime.read_typed_event_result(self, event_id, payload_hash)
        if self.hide_first and self.result_reads == 1:
            return None
        if result is None or self.corruption == "canonical":
            return result
        if self.corruption == "missing":
            return None
        if self.corruption == "wrong_event":
            return _unsafe_set(result, event_id="wrong-event")
        if self.corruption == "wrong_payload":
            return _unsafe_set(result, payload_hash=sha("wrong-payload"))
        if self.corruption == "wrong_epoch":
            return _unsafe_set(result, epoch_id=result.epoch_id + 1)
        if self.corruption == "active_open":
            return _unsafe_set(
                result,
                open_receipt=OpenEventReceipt(result.epoch_id, True, False),
            )
        if self.corruption == "nonzero_call_work":
            return _unsafe_set(
                result,
                call_work=M5RuntimeWork(direct_discovery_call_count=1),
            )
        if self.corruption == "wrong_type":
            return cast(M5EventRunResult, object())
        raise AssertionError(f"unknown result corruption: {self.corruption}")


def _mutate_plan_binding(
    event: M5TypedEventPlan,
    *,
    target_event_id: str,
    target_payload_hash: str,
    mutation: str,
) -> None:
    if mutation in {"event_id", "both"}:
        object.__setattr__(event, "structural_event_id", target_event_id)
    if mutation in {"payload_hash", "both"}:
        object.__setattr__(event, "payload_hash", target_payload_hash)
    if mutation not in {"event_id", "payload_hash", "both"}:
        raise AssertionError(f"unknown event-binding mutation: {mutation}")


@dataclass(slots=True)
class _BindingProbeRuntime(_ObservedRuntime):
    trusted_event_id: str = ""
    trusted_payload_hash: str = ""
    alternate_result: M5EventRunResult | None = None
    hide_first: bool = True
    mutate_during_canonical_read: M5TypedEventPlan | None = None
    mutation_target_event_id: str = ""
    mutation_target_payload_hash: str = ""
    result_coordinates: list[tuple[str, str]] = field(default_factory=list)

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        self.result_reads += 1
        self.result_coordinates.append((event_id, payload_hash))
        if (event_id, payload_hash) == (
            self.trusted_event_id,
            self.trusted_payload_hash,
        ):
            result = FakeRuntime.read_typed_event_result(self, event_id, payload_hash)
        else:
            self.world.operation_log.append("read:terminal-result")
            result = self.alternate_result
        if self.hide_first and self.result_reads == 1:
            return None
        if self.mutate_during_canonical_read is not None:
            _mutate_plan_binding(
                self.mutate_during_canonical_read,
                target_event_id=self.mutation_target_event_id,
                target_payload_hash=self.mutation_target_payload_hash,
                mutation="both",
            )
            self.mutate_during_canonical_read = None
        return result


@dataclass(slots=True)
class _BindingMutatingStructural(_ObservedStructural):
    cutoff_call: int = 1
    trusted_event_id: str = ""
    target_event_id: str = ""
    target_payload_hash: str = ""
    mutation: str = "both"
    terminalize: bool = False

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        plan = _ObservedStructural.plan_exact_requirement_withdrawal(self, event)
        if len(self.plans) == self.cutoff_call:
            if self.terminalize:
                _terminalize_failed(self.world, self.trusted_event_id)
            _mutate_plan_binding(
                event,
                target_event_id=self.target_event_id,
                target_payload_hash=self.target_payload_hash,
                mutation=self.mutation,
            )
            raise _M5D29HydrationTerminalCutoff()
        return plan


@dataclass(slots=True)
class _BindingMutatingDirect(_ObservedDirect):
    cutoff_site: str = "plan"
    trusted_event_id: str = ""
    target_event_id: str = ""
    target_payload_hash: str = ""
    mutation: str = "both"
    terminalize: bool = False

    def plan_direct_open(self, event: M5TypedEventPlan) -> M5DirectOpenPlan:
        plan = _ObservedDirect.plan_direct_open(self, event)
        if self.cutoff_site == "plan":
            _mutate_plan_binding(
                event,
                target_event_id=self.target_event_id,
                target_payload_hash=self.target_payload_hash,
                mutation=self.mutation,
            )
            raise _M5D29HydrationTerminalCutoff()
        return plan

    def run_pending_direct(
        self,
        epoch_id: int,
        expected_revision: int,
        event: M5TypedEventPlan,
        open_receipt: OpenEventReceipt,
    ) -> M5DirectExecutionReceipt:
        self.run_calls += 1
        self.world.operation_log.append("call:direct-runner")
        if self.cutoff_site == "run":
            if self.terminalize:
                _terminalize_failed(self.world, self.trusted_event_id)
            _mutate_plan_binding(
                event,
                target_event_id=self.target_event_id,
                target_payload_hash=self.target_payload_hash,
                mutation=self.mutation,
            )
            raise _M5D29HydrationTerminalCutoff()
        return FakeDirect.run_pending_direct(
            self,
            epoch_id,
            expected_revision,
            event,
            open_receipt,
        )


def _new_insert_plan(harness: FakeHarness, name: str) -> M5TypedEventPlan:
    return make_typed_plan(
        harness.world,
        InsertDocumentEvent(
            event_id=f"event:{name}:insert",
            document_id=f"document:{name}",
            document_version_id=f"version:{name}",
            content_hash=sha(f"content:{name}"),
            chunks=(ChunkInput(f"chunk:{name}", 0, f"evidence for {name}"),),
        ),
    )


def _run_additional_terminal(
    harness: FakeHarness, name: str
) -> tuple[M5TypedEventPlan, M5EventRunResult]:
    plan = _new_insert_plan(harness, name)
    result = harness.application.run_event(plan)
    assert result.state is M5RunState.SEALED
    canonical = FakeRuntime(harness.world).read_typed_event_result(
        plan.structural_event_id, plan.payload_hash
    )
    assert canonical is not None
    assert canonical.state is M5RunState.REPLAYED
    return plan, canonical


def _complete_then_hide_initial_result(
    kind: str, name: str
) -> tuple[FakeHarness, M5TypedEventPlan, _ResultReadRuntime]:
    harness, plan, _ = _document_case(kind, name)
    completed = harness.application.run_event(plan)
    assert completed.state is M5RunState.SEALED
    harness.world.operation_log.clear()
    runtime = _ResultReadRuntime(harness.world, hide_first=True)
    _install_ports(harness, runtime=runtime)
    return harness, plan, runtime


def _relevant_order(log: list[str]) -> list[str]:
    relevant = {
        "read:terminal-result",
        "plan:requirement-withdrawal",
        "plan:direct",
        "call:typed-open",
        "read:current-revision",
        "call:direct-runner",
    }
    return [marker for marker in log if marker in relevant]


def test_d29_private_signal_and_public_surface_are_frozen() -> None:
    signal = _M5D29HydrationTerminalCutoff()
    assert type(signal) is _M5D29HydrationTerminalCutoff
    assert issubclass(_M5D29HydrationTerminalCutoff, ValidationError)
    assert _M5D29HydrationTerminalCutoff.__slots__ == ()
    assert signal.args == ()
    assert vars(signal) == {}
    assert "_M5D29HydrationTerminalCutoff" not in application_module.__all__
    assert tuple(application_module.__all__) == (
        "M5CandidatePolicyPort",
        "M5DirectExecutionReceipt",
        "M5DirectOpenPlan",
        "M5DirectSubgraphPort",
        "M5DiscoveryExecution",
        "M5ExternalWorkFailure",
        "M5PostSealAuditPort",
        "M5RequirementDiscoveryPort",
        "M5RequirementRootDeclaration",
        "M5RequirementVerifierPort",
        "M5RuntimePersistencePort",
        "M5RuntimeReadPort",
        "M5RuntimeMeasurementPort",
        "M5TerminalInvocationTelemetry",
        "M5TypedApplication",
        "M5TypedStructuralPort",
        "M5VerifierExecution",
    )

    assert tuple(field_.name for field_ in fields(M5TypedApplication)) == (
        "policies",
        "structural",
        "direct",
        "runtime",
        "runtime_reads",
        "discovery",
        "verifier",
        "measurements",
        "post_seal_audit",
    )
    assert tuple(field_.name for field_ in fields(M5TypedEventPlan)) == (
        "structural_event_id",
        "event",
        "payload_hash",
        "direct_plan",
        "candidate_policy_id",
        "candidate_policy_manifest_hash",
        "requirement_registry_snapshot",
        "active_chunk_snapshot",
        "expected_previous_published_epoch_id",
    )
    assert tuple(field_.name for field_ in fields(M5RequirementWithdrawalPlan)) == (
        "event_id",
        "deactivated_chunk_version_ids",
        "withdrawn_candidate_pair_digests",
        "withdrawn_observation_ids",
        "cancelled_job_ids",
        "fallback_keys",
        "plan_digest",
    )
    assert tuple(field_.name for field_ in fields(M5DirectExecutionReceipt)) == (
        "resulting_revision",
        "call_work",
        "blocked_reason",
        "terminal_failure_reason",
        "selected_successful_outer_receipt",
        "selected_successful_outer_return_kind",
        "selected_successful_outer_job_id",
        "selected_terminal_acquisition_receipt",
        "selected_checked_combined_failure_receipt",
    )
    assert tuple(field_.name for field_ in fields(M5DirectOpenPlan)) == (
        "structural_payload",
        "withdrawal",
        "root_jobs",
        "discovery_scopes",
    )
    assert tuple(field_.name for field_ in fields(M5DiscoveryExecution)) == (
        "result",
        "attempt_output",
        "eligible_snapshot_exhausted",
        "execution_disposition",
        "call_work",
        "attempt_timing",
    )
    assert tuple(field_.name for field_ in fields(M5RequirementRootDeclaration)) == (
        "scope",
        "job",
    )
    assert tuple(field_.name for field_ in fields(M5TerminalInvocationTelemetry)) == (
        "invocation_id",
        "call_timing",
    )
    assert tuple(field_.name for field_ in fields(M5VerifierExecution)) == (
        "pair_input",
        "artifact",
        "attempt_output",
        "execution_disposition",
        "call_work",
        "attempt_timing",
    )

    expected_parameters = {
        (M5TypedStructuralPort, "plan_exact_requirement_withdrawal"): (
            "self",
            "event",
        ),
        (M5DirectSubgraphPort, "plan_direct_open"): ("self", "event"),
        (M5DirectSubgraphPort, "run_pending_direct"): (
            "self",
            "epoch_id",
            "expected_revision",
            "event",
            "open_receipt",
        ),
        (M5RuntimeReadPort, "current_revision"): ("self", "epoch_id"),
        (M5TypedApplication, "run_event"): ("self", "event"),
    }
    for (owner, method_name), parameters in expected_parameters.items():
        assert tuple(inspect.signature(getattr(owner, method_name)).parameters) == (
            parameters
        )


def test_d29_run_event_has_exactly_four_literal_cutoff_catches() -> None:
    source = inspect.getsource(application_module)
    tree = ast.parse(source)
    application_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "M5TypedApplication"
    )
    run_event = next(
        node
        for node in application_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "run_event"
    )
    handlers = [
        handler
        for handler in ast.walk(run_event)
        if isinstance(handler, ast.ExceptHandler)
        and isinstance(handler.type, ast.Name)
        and handler.type.id == "_M5D29HydrationTerminalCutoff"
    ]
    assert len(handlers) == 4

    caught_calls: list[str] = []
    for handler in handlers:
        parent_try = next(
            node
            for node in ast.walk(run_event)
            if isinstance(node, ast.Try) and handler in node.handlers
        )
        calls = [
            call.func.attr
            for call in ast.walk(ast.Module(body=parent_try.body, type_ignores=[]))
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
        ]
        assert len(calls) == 1
        caught_calls.extend(calls)
    assert sorted(caught_calls) == [
        "plan_direct_open",
        "plan_exact_requirement_withdrawal",
        "plan_exact_requirement_withdrawal",
        "run_pending_direct",
    ]


@pytest.mark.parametrize("kind", ["insert", "delete", "replace"])
@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
def test_document_routes_have_exact_preview_open_hydration_order(
    kind: str, resumed: bool
) -> None:
    harness, plan, _ = _document_case(kind, f"order:{kind}:{resumed}")
    if resumed:
        _prepare_nonterminal_replay(harness, plan)

    structural = _ObservedStructural(harness.world)
    direct = _ObservedDirect(harness.world)
    runtime = _ObservedRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert _relevant_order(harness.world.operation_log) == [
        "read:terminal-result",
        "plan:requirement-withdrawal",
        "plan:direct",
        "call:typed-open",
        *(["plan:requirement-withdrawal"] if resumed else []),
        "read:current-revision",
        "call:direct-runner",
    ]
    assert len(structural.plans) == (2 if resumed else 1)
    assert direct.plan_calls == 1
    assert direct.run_calls == 1
    assert runtime.revision_reads == 1


@dataclass(slots=True)
class _PreviewThenHydratedStructural(_ObservedStructural):
    event_id: str = ""

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        fallback = self.world.fallback_by_event.pop(self.event_id, None)
        try:
            if self.plans:
                assert fallback is not None
                self.world.fallback_by_event[self.event_id] = fallback
            return _ObservedStructural.plan_exact_requirement_withdrawal(self, event)
        finally:
            if fallback is not None:
                self.world.fallback_by_event[self.event_id] = fallback


@pytest.mark.parametrize("kind", ["insert", "delete", "replace"])
def test_replayed_document_hydration_replaces_preview_withdrawal_roots_and_hash(
    kind: str,
) -> None:
    harness, plan, requirement_id = _document_case(
        kind, f"hydrate:{kind}", with_requirement=True
    )
    assert requirement_id is not None
    fallback = M5RequirementFallbackKey(requirement_id, plan.candidate_policy_id)
    harness.world.fallback_by_event[plan.structural_event_id] = (fallback,)
    _prepare_nonterminal_replay(harness, plan)

    structural = _PreviewThenHydratedStructural(
        harness.world, event_id=plan.structural_event_id
    )
    direct = _ObservedDirect(harness.world)
    runtime = _ObservedRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert len(structural.plans) == 2
    preview, hydrated = structural.plans
    assert preview.fallback_keys == ()
    assert hydrated.fallback_keys == (fallback,)
    preview_roots = harness.application._requirement_roots(  # noqa: SLF001
        plan, harness.world.manifest, preview
    )
    hydrated_roots = harness.application._requirement_roots(  # noqa: SLF001
        plan, harness.world.manifest, hydrated
    )
    preview_hash = digests.requirement_root_set_digest(
        declaration.job.logical_job_id for declaration in preview_roots
    )
    hydrated_hash = digests.requirement_root_set_digest(
        declaration.job.logical_job_id for declaration in hydrated_roots
    )
    assert preview_hash != hydrated_hash
    epoch = harness.world.epoch(result.epoch_id)
    assert epoch.root_barrier is not None
    assert epoch.root_barrier.requirement_root_set_hash == hydrated_hash
    assert any(
        marker == "acquire:forward_requirement_retrieval"
        for marker in harness.world.operation_log
    )


@pytest.mark.parametrize("kind", ["insert", "delete", "replace"])
@pytest.mark.parametrize("site", ["requirement", "direct"])
def test_valid_preopen_cutoff_returns_ordinary_canonical_terminal_replay(
    site: str, kind: str
) -> None:
    harness, plan, runtime = _complete_then_hide_initial_result(
        kind, f"preopen:{site}:{kind}"
    )
    structural: FakeStructural
    direct: FakeDirect
    if site == "requirement":
        structural = _CutoffStructural(harness.world)
        direct = _ObservedDirect(harness.world)
    else:
        structural = _ObservedStructural(harness.world)
        direct = _CutoffDirect(harness.world, cutoff_site="plan")
    _install_ports(harness, structural=structural, direct=direct)

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.SEALED
    assert result.open_receipt.replayed
    assert result.open_receipt.already_sealed
    assert result.call_work.is_zero
    assert runtime.result_reads == 2
    assert "call:typed-open" not in harness.world.operation_log
    assert not any(
        marker.startswith("external:") for marker in harness.world.operation_log
    )


@pytest.mark.parametrize("kind", ["insert", "delete", "replace"])
def test_valid_postopen_requirement_hydration_cutoff_uses_active_projection(
    kind: str,
) -> None:
    harness, plan, _ = _document_case(kind, f"postopen:requirement:{kind}")
    _prepare_nonterminal_replay(harness, plan)
    structural = _CutoffStructural(harness.world, cutoff_call=2, terminalize=True)
    direct = _ObservedDirect(harness.world)
    runtime = _ResultReadRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.open_receipt.replayed
    assert not result.open_receipt.already_sealed
    assert not result.open_receipt.already_failed
    assert result.call_work.is_zero
    assert runtime.result_reads == 2
    assert direct.run_calls == 0
    assert not any(
        marker.startswith("external:") for marker in harness.world.operation_log
    )


@pytest.mark.parametrize("kind", ["insert", "delete", "replace"])
def test_valid_postopen_direct_hydration_cutoff_uses_active_projection(
    kind: str,
) -> None:
    harness, plan, _ = _document_case(kind, f"postopen:direct:{kind}")
    _prepare_nonterminal_replay(harness, plan)
    structural = _ObservedStructural(harness.world)
    direct = _CutoffDirect(harness.world, cutoff_site="run", terminalize=True)
    runtime = _ResultReadRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.REPLAYED
    assert result.replayed_outcome is M5ReplayedOutcome.FAILED
    assert result.open_receipt.replayed
    assert not result.open_receipt.already_sealed
    assert not result.open_receipt.already_failed
    assert result.call_work.is_zero
    assert runtime.result_reads == 2
    assert direct.run_calls == 1
    assert not any(
        marker.startswith("external:") for marker in harness.world.operation_log
    )


def test_fresh_direct_signal_is_not_an_existing_event_hydration_origin() -> None:
    harness, plan, _ = _document_case("delete", "fresh-direct-signal")
    structural = _ObservedStructural(harness.world)
    direct = _CutoffDirect(harness.world, cutoff_site="run", terminalize=True)
    runtime = _ResultReadRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    with pytest.raises(ValidationError, match="exact-existing receipt"):
        harness.application.run_event(plan)

    assert runtime.result_reads == 1
    assert harness.world.terminal_telemetry == {}


def _binding_mutation_preopen_case(
    site: str, mutation: str
) -> tuple[
    FakeHarness,
    M5TypedEventPlan,
    _BindingProbeRuntime,
    int,
    tuple[str, str],
]:
    harness, authority_plan, _ = _document_case(
        "delete", f"binding-pre:{site}:{mutation}"
    )
    original = harness.application.run_event(authority_plan)
    assert original.state is M5RunState.SEALED
    redirect_plan, redirect_result = _run_additional_terminal(
        harness, f"binding-pre-redirect:{site}:{mutation}"
    )
    invocation_plan = replace(authority_plan)
    trusted = (
        authority_plan.structural_event_id,
        authority_plan.payload_hash,
    )
    runtime = _BindingProbeRuntime(
        harness.world,
        trusted_event_id=trusted[0],
        trusted_payload_hash=trusted[1],
        alternate_result=redirect_result,
    )
    if site == "pre_requirement":
        structural: FakeStructural = _BindingMutatingStructural(
            harness.world,
            trusted_event_id=trusted[0],
            target_event_id=redirect_plan.structural_event_id,
            target_payload_hash=redirect_plan.payload_hash,
            mutation=mutation,
        )
        direct: FakeDirect = _ObservedDirect(harness.world)
    else:
        structural = _ObservedStructural(harness.world)
        direct = _BindingMutatingDirect(
            harness.world,
            cutoff_site="plan",
            trusted_event_id=trusted[0],
            target_event_id=redirect_plan.structural_event_id,
            target_payload_hash=redirect_plan.payload_hash,
            mutation=mutation,
        )
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )
    harness.world.operation_log.clear()
    return (
        harness,
        invocation_plan,
        runtime,
        len(harness.world.terminal_telemetry),
        trusted,
    )


def _binding_mutation_postopen_case(
    site: str, mutation: str
) -> tuple[
    FakeHarness,
    M5TypedEventPlan,
    _BindingProbeRuntime,
    int,
    tuple[str, str],
]:
    harness = make_harness()
    redirect_plan, redirect_result = _run_additional_terminal(
        harness, f"binding-post-redirect:{site}:{mutation}"
    )
    authority_plan = _new_insert_plan(harness, f"binding-post:{site}:{mutation}")
    stored_plan = replace(authority_plan)
    _prepare_nonterminal_replay(harness, stored_plan)
    invocation_plan = replace(authority_plan)
    trusted = (
        authority_plan.structural_event_id,
        authority_plan.payload_hash,
    )
    runtime = _BindingProbeRuntime(
        harness.world,
        trusted_event_id=trusted[0],
        trusted_payload_hash=trusted[1],
        alternate_result=redirect_result,
    )
    if site == "post_requirement":
        structural: FakeStructural = _BindingMutatingStructural(
            harness.world,
            cutoff_call=2,
            trusted_event_id=trusted[0],
            target_event_id=redirect_plan.structural_event_id,
            target_payload_hash=redirect_plan.payload_hash,
            mutation=mutation,
            terminalize=True,
        )
        direct: FakeDirect = _ObservedDirect(harness.world)
    else:
        structural = _ObservedStructural(harness.world)
        direct = _BindingMutatingDirect(
            harness.world,
            cutoff_site="run",
            trusted_event_id=trusted[0],
            target_event_id=redirect_plan.structural_event_id,
            target_payload_hash=redirect_plan.payload_hash,
            mutation=mutation,
            terminalize=True,
        )
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )
    harness.world.operation_log.clear()
    return (
        harness,
        invocation_plan,
        runtime,
        len(harness.world.terminal_telemetry),
        trusted,
    )


@pytest.mark.parametrize(
    "site",
    ["pre_requirement", "pre_direct", "post_requirement", "post_direct"],
)
@pytest.mark.parametrize("mutation", ["event_id", "payload_hash", "both"])
def test_cutoff_rejects_event_binding_mutation_at_every_authorized_site(
    site: str, mutation: str
) -> None:
    if site.startswith("pre_"):
        harness, plan, runtime, telemetry_count, trusted = (
            _binding_mutation_preopen_case(site, mutation)
        )
    else:
        harness, plan, runtime, telemetry_count, trusted = (
            _binding_mutation_postopen_case(site, mutation)
        )

    with pytest.raises(ValidationError, match="changed its event binding"):
        harness.application.run_event(plan)

    assert runtime.result_coordinates
    assert all(coordinates == trusted for coordinates in runtime.result_coordinates)
    assert len(runtime.result_coordinates) in {1, 2}
    assert len(harness.world.terminal_telemetry) == telemetry_count
    if mutation in {"event_id", "both"}:
        assert plan.structural_event_id != trusted[0]
    else:
        assert plan.structural_event_id == trusted[0]
    if mutation in {"payload_hash", "both"}:
        assert plan.payload_hash != trusted[1]
    else:
        assert plan.payload_hash == trusted[1]


@pytest.mark.parametrize("phase", ["pre", "post"])
def test_cutoff_revalidates_event_binding_after_canonical_reread(
    phase: str,
) -> None:
    if phase == "pre":
        harness, authority_plan, _ = _document_case("delete", "binding-during-read:pre")
        original = harness.application.run_event(authority_plan)
        assert original.state is M5RunState.SEALED
        redirect_plan, redirect_result = _run_additional_terminal(
            harness, "binding-during-read:pre:redirect"
        )
        plan = replace(authority_plan)
        structural: FakeStructural = _CutoffStructural(harness.world)
    else:
        harness = make_harness()
        redirect_plan, redirect_result = _run_additional_terminal(
            harness, "binding-during-read:post:redirect"
        )
        authority_plan = _new_insert_plan(harness, "binding-during-read:post")
        _prepare_nonterminal_replay(harness, replace(authority_plan))
        plan = replace(authority_plan)
        structural = _CutoffStructural(harness.world, cutoff_call=2, terminalize=True)

    trusted = (authority_plan.structural_event_id, authority_plan.payload_hash)
    runtime = _BindingProbeRuntime(
        harness.world,
        trusted_event_id=trusted[0],
        trusted_payload_hash=trusted[1],
        alternate_result=redirect_result,
        mutate_during_canonical_read=plan,
        mutation_target_event_id=redirect_plan.structural_event_id,
        mutation_target_payload_hash=redirect_plan.payload_hash,
    )
    _install_ports(harness, structural=structural, runtime=runtime)
    telemetry_count = len(harness.world.terminal_telemetry)
    harness.world.operation_log.clear()

    with pytest.raises(ValidationError, match="changed its event binding"):
        harness.application.run_event(plan)

    assert runtime.result_coordinates == [trusted, trusted]
    assert len(harness.world.terminal_telemetry) == telemetry_count
    assert (plan.structural_event_id, plan.payload_hash) != trusted


@pytest.mark.parametrize("kind", ["insert", "delete", "replace"])
def test_initial_terminal_result_bypasses_every_d29_planner_and_open(kind: str) -> None:
    harness, plan, _ = _document_case(kind, f"initial-terminal:{kind}")
    completed = harness.application.run_event(plan)
    assert completed.state is M5RunState.SEALED
    harness.world.operation_log.clear()
    structural = _CutoffStructural(harness.world)
    direct = _CutoffDirect(harness.world, cutoff_site="plan")
    runtime = _ObservedRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    replay = harness.application.run_event(plan)

    assert replay.state is M5RunState.REPLAYED
    assert runtime.result_reads == 1
    assert structural.plans == []
    assert direct.plan_calls == 0
    assert "call:typed-open" not in harness.world.operation_log


@pytest.mark.parametrize("mode", ["subclass", "nonempty", "field"])
def test_preopen_cutoff_rejects_nonexact_or_payload_bearing_signal(mode: str) -> None:
    harness, plan, runtime = _complete_then_hide_initial_result(
        "delete", f"bad-signal:{mode}"
    )
    structural = _CutoffStructural(harness.world, signal_mode=mode)
    _install_ports(harness, structural=structural)
    telemetry_count = len(harness.world.terminal_telemetry)

    with pytest.raises(ValidationError) as raised:
        harness.application.run_event(plan)

    assert type(raised.value) is ValidationError
    assert runtime.result_reads == 1
    assert len(harness.world.terminal_telemetry) == telemetry_count


def test_ordinary_validation_error_is_not_a_cutoff_signal() -> None:
    harness, plan, runtime = _complete_then_hide_initial_result(
        "delete", "ordinary-validation"
    )

    @dataclass(slots=True)
    class _OrdinaryFailureStructural(FakeStructural):
        def plan_exact_requirement_withdrawal(
            self, event: M5TypedEventPlan
        ) -> M5RequirementWithdrawalPlan:
            raise ValidationError("ordinary planner failure")

    _install_ports(harness, structural=_OrdinaryFailureStructural(harness.world))

    with pytest.raises(ValidationError, match="ordinary planner failure") as raised:
        harness.application.run_event(plan)

    assert type(raised.value) is ValidationError
    assert runtime.result_reads == 1


@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "wrong_event",
        "wrong_payload",
        "wrong_epoch",
        "active_open",
        "nonzero_call_work",
        "wrong_type",
    ],
)
def test_preopen_cutoff_rejects_missing_or_noncanonical_result(
    corruption: str,
) -> None:
    harness, plan, runtime = _complete_then_hide_initial_result(
        "delete", f"bad-pre-canonical:{corruption}"
    )
    runtime.corruption = corruption
    _install_ports(harness, structural=_CutoffStructural(harness.world))
    telemetry_count = len(harness.world.terminal_telemetry)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert runtime.result_reads == 2
    assert len(harness.world.terminal_telemetry) == telemetry_count
    assert "call:typed-open" not in harness.world.operation_log


@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "wrong_event",
        "wrong_payload",
        "wrong_epoch",
        "active_open",
        "nonzero_call_work",
        "wrong_type",
    ],
)
def test_postopen_cutoff_rejects_missing_or_noncanonical_same_epoch_result(
    corruption: str,
) -> None:
    harness, plan, _ = _document_case("delete", f"bad-post-canonical:{corruption}")
    _prepare_nonterminal_replay(harness, plan)
    structural = _CutoffStructural(harness.world, cutoff_call=2, terminalize=True)
    runtime = _ResultReadRuntime(harness.world, corruption=corruption)
    _install_ports(harness, structural=structural, runtime=runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert runtime.result_reads == 2
    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize("mode", ["subclass", "nonempty", "field"])
def test_postopen_cutoff_rejects_nonexact_or_payload_bearing_signal(mode: str) -> None:
    harness, plan, _ = _document_case("delete", f"bad-post-signal:{mode}")
    _prepare_nonterminal_replay(harness, plan)
    structural = _CutoffStructural(
        harness.world,
        cutoff_call=2,
        signal_mode=mode,
        terminalize=True,
    )
    runtime = _ResultReadRuntime(harness.world)
    _install_ports(harness, structural=structural, runtime=runtime)

    with pytest.raises(ValidationError) as raised:
        harness.application.run_event(plan)

    assert type(raised.value) is ValidationError
    assert runtime.result_reads == 1
    assert harness.world.terminal_telemetry == {}


@dataclass(slots=True)
class _MutatingCanonicalReadRuntime(FakeRuntime):
    opened: OpenEventReceipt
    call_work: M5RuntimeWork
    mutation: str

    def read_typed_event_result(
        self, event_id: str, payload_hash: str
    ) -> M5EventRunResult | None:
        result = FakeRuntime.read_typed_event_result(self, event_id, payload_hash)
        if self.mutation == "held":
            object.__setattr__(self.opened, "replayed", not self.opened.replayed)
        elif self.mutation == "work":
            object.__setattr__(self.call_work, "bytes_hashed", 1)
        else:
            raise AssertionError(f"unknown mutation: {self.mutation}")
        return result


class _OpenReceiptSubclass(OpenEventReceipt):
    """Invalid held-receipt subtype with otherwise inherited behavior."""


class _EqualitySpoofingOpenReceipt(OpenEventReceipt):
    """Invalid subtype that attempts to bypass the held-byte comparison."""

    def __eq__(self, other: object) -> bool:
        return True

    def __ne__(self, other: object) -> bool:
        return False


class _RuntimeWorkSubclass(M5RuntimeWork):
    """Invalid work-vector subtype with otherwise canonical counters."""


class _NonExactZero(int):
    """An integer subtype which compares equal to canonical zero."""


@pytest.mark.parametrize(
    "mutation",
    [
        "held_before",
        "terminal_held",
        "held_subclass",
        "held_equality_spoof",
        "work_before",
        "work_subclass",
        "work_nonexact_counter",
        "held_during",
        "work_during",
    ],
)
def test_postopen_helper_rejects_changed_held_receipt_or_nonzero_work(
    mutation: str,
) -> None:
    harness, plan, _ = _document_case("delete", f"helper-mutation:{mutation}")
    first_opened = _prepare_nonterminal_replay(harness, plan)
    epoch = harness.world.epoch(first_opened.epoch_id)
    opened = OpenEventReceipt(first_opened.epoch_id, True, False)
    epoch.current_open_receipt = opened
    held = replace(opened)
    call_work = M5RuntimeWork()
    _terminalize_failed(harness.world, plan.structural_event_id)

    if mutation == "held_before":
        held = replace(held, replayed=not held.replayed)
    elif mutation == "terminal_held":
        object.__setattr__(opened, "already_failed", True)
        object.__setattr__(opened, "failure_reason", "retrieval_error")
    elif mutation in {"held_subclass", "held_equality_spoof"}:
        held_type = (
            _OpenReceiptSubclass
            if mutation == "held_subclass"
            else _EqualitySpoofingOpenReceipt
        )
        held = held_type(
            held.epoch_id,
            held.replayed,
            held.already_sealed,
            held.publication_id,
            held.already_failed,
            held.failure_reason,
        )
    elif mutation == "work_before":
        call_work = M5RuntimeWork(bytes_hashed=1)
    elif mutation == "work_subclass":
        call_work = _RuntimeWorkSubclass()
    elif mutation == "work_nonexact_counter":
        call_work = _unsafe_set(M5RuntimeWork(), bytes_hashed=_NonExactZero(0))
    else:
        runtime = _MutatingCanonicalReadRuntime(
            harness.world,
            opened,
            call_work,
            mutation.removesuffix("_during"),
        )
        _install_ports(harness, runtime=runtime)

    with pytest.raises(ValidationError):
        harness.application._finish_d29_post_open_terminal_cutoff(  # noqa: SLF001
            plan,
            plan.structural_event_id,
            plan.payload_hash,
            opened,
            held,
            call_work,
            _M5D29HydrationTerminalCutoff(),
        )

    assert harness.world.terminal_telemetry == {}


@dataclass(slots=True)
class _SignalPolicy:
    def candidate_policy(self, candidate_policy_id: str) -> object:
        raise _M5D29HydrationTerminalCutoff()


@dataclass(slots=True)
class _SignalOpenStructural(_ObservedStructural):
    def open_typed_event_atomically(
        self,
        event: M5TypedEventPlan,
        direct_payload: StructuralPayload | None,
        direct_withdrawal: StructuralWithdrawal | None,
        requirement_withdrawal: M5RequirementWithdrawalPlan,
        direct_roots: tuple[M4LogicalJobSpec, ...],
        direct_scopes: tuple[M4DiscoveryScope, ...],
        requirement_roots: tuple[M5RequirementRootDeclaration, ...],
        requirement_root_set_hash: str,
    ) -> OpenEventReceipt:
        raise _M5D29HydrationTerminalCutoff()


@dataclass(slots=True)
class _SignalRevisionRuntime(_ObservedRuntime):
    def current_revision(self, epoch_id: int) -> int:
        raise _M5D29HydrationTerminalCutoff()


@pytest.mark.parametrize("site", ["policy", "open", "revision"])
def test_exact_signal_from_any_unlisted_site_remains_a_conflict(site: str) -> None:
    harness, plan, _ = _document_case("delete", f"wrong-site:{site}")
    runtime = _ObservedRuntime(harness.world)
    _install_ports(harness, runtime=runtime)
    if site == "policy":
        harness.application.policies = cast(M5CandidatePolicyPort, _SignalPolicy())
    elif site == "open":
        _install_ports(harness, structural=_SignalOpenStructural(harness.world))
    else:
        _install_ports(harness, runtime=_SignalRevisionRuntime(harness.world))

    with pytest.raises(_M5D29HydrationTerminalCutoff):
        harness.application.run_event(plan)

    assert harness.world.terminal_telemetry == {}
    assert harness.world.operation_log.count("read:terminal-result") == 1


@dataclass(slots=True)
class _SignalAcquisitionRuntime(_ObservedRuntime):
    def acquire_m5_job(
        self,
        epoch_id: int,
        expected_revision: int,
        job: M5LogicalJobSpec,
    ) -> M5JobLease:
        raise _M5D29HydrationTerminalCutoff()


def test_exact_signal_from_requirement_acquisition_is_not_a_cutoff_origin() -> None:
    harness, plan = _group_case("register", "wrong-site:acquisition")
    runtime = _SignalAcquisitionRuntime(harness.world)
    _install_ports(harness, runtime=runtime)

    with pytest.raises(_M5D29HydrationTerminalCutoff):
        harness.application.run_event(plan)

    assert runtime.result_reads == 1
    assert harness.world.terminal_telemetry == {}
    assert "typed-open" in harness.world.operation_log


@dataclass(slots=True)
class _SignalTerminalMeasurements(FakeMeasurements):
    def terminal_invocation(
        self, event: M5TypedEventPlan, result: M5EventRunResult
    ) -> M5TerminalInvocationTelemetry:
        raise _M5D29HydrationTerminalCutoff()


def test_exact_signal_from_terminal_measurement_is_not_a_cutoff_origin() -> None:
    harness, plan, _ = _document_case("delete", "wrong-site:measurement")
    completed = harness.application.run_event(plan)
    assert completed.state is M5RunState.SEALED
    telemetry_count = len(harness.world.terminal_telemetry)
    harness.world.operation_log.clear()
    runtime = _ObservedRuntime(harness.world)
    measurements = _SignalTerminalMeasurements(harness.world)
    _install_ports(harness, runtime=runtime)
    harness.measurements = measurements
    harness.application.measurements = measurements

    with pytest.raises(_M5D29HydrationTerminalCutoff):
        harness.application.run_event(plan)

    assert runtime.result_reads == 1
    assert len(harness.world.terminal_telemetry) == telemetry_count
    assert harness.world.operation_log.count("read:terminal-result") == 1


@dataclass(slots=True)
class _TerminalizingHydrationStructural(_ObservedStructural):
    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        plan = _ObservedStructural.plan_exact_requirement_withdrawal(self, event)
        if len(self.plans) == 2:
            _terminalize_failed(self.world, event.structural_event_id)
        return plan


@dataclass(slots=True)
class _StopAfterHydrationRuntime(_ObservedRuntime):
    def current_revision(self, epoch_id: int) -> int:
        self.revision_reads += 1
        self.world.operation_log.append("stop:after-normal-hydration")
        raise _SyntheticStop("normal hydration returned")


def test_normal_hydration_return_grants_no_generic_terminal_result_read() -> None:
    harness, plan, _ = _document_case("delete", "normal-hydration-no-authority")
    _prepare_nonterminal_replay(harness, plan)
    structural = _TerminalizingHydrationStructural(harness.world)
    runtime = _StopAfterHydrationRuntime(harness.world)
    _install_ports(harness, structural=structural, runtime=runtime)

    with pytest.raises(_SyntheticStop, match="normal hydration returned"):
        harness.application.run_event(plan)

    assert len(structural.plans) == 2
    assert runtime.result_reads == 1
    assert harness.world.operation_log.count("read:terminal-result") == 1
    assert harness.world.terminal_telemetry == {}


@pytest.mark.parametrize("kind", ["register", "replace", "retire"])
@pytest.mark.parametrize("resumed", [False, True], ids=["fresh", "resumed"])
def test_group_routes_preserve_d28_flow_without_d29_hydration_or_signal_route(
    kind: str, resumed: bool
) -> None:
    harness, plan = _group_case(kind, f"group-order:{kind}:{resumed}")
    if resumed:
        _prepare_nonterminal_replay(harness, plan)

    structural = _ObservedStructural(harness.world)
    direct = _ObservedDirect(harness.world)
    runtime = _ObservedRuntime(harness.world)
    _install_ports(
        harness,
        structural=structural,
        direct=direct,
        runtime=runtime,
    )

    result = harness.application.run_event(plan)

    assert result.state is M5RunState.SEALED
    assert _relevant_order(harness.world.operation_log) == [
        "read:terminal-result",
        "plan:requirement-withdrawal",
        "call:typed-open",
        "read:current-revision",
        "call:direct-runner",
    ]
    assert len(structural.plans) == 1
    assert direct.plan_calls == 0
    assert direct.run_calls == 1
    assert runtime.revision_reads == 1


@pytest.mark.parametrize("kind", ["register", "replace", "retire"])
def test_group_planner_signal_is_never_a_d29_cutoff_origin(kind: str) -> None:
    harness, plan = _group_case(kind, f"group-signal:{kind}")
    structural = _CutoffStructural(harness.world)
    runtime = _ObservedRuntime(harness.world)
    _install_ports(harness, structural=structural, runtime=runtime)

    with pytest.raises(_M5D29HydrationTerminalCutoff):
        harness.application.run_event(plan)

    assert runtime.result_reads == 1
    assert harness.world.terminal_telemetry == {}
    assert "call:typed-open" not in harness.world.operation_log


@pytest.mark.parametrize("kind", ["register", "replace", "retire"])
def test_group_direct_runner_signal_is_never_a_d29_cutoff_origin(kind: str) -> None:
    harness, plan = _group_case(kind, f"group-direct-signal:{kind}")
    direct = _CutoffDirect(harness.world, cutoff_site="run")
    runtime = _ObservedRuntime(harness.world)
    _install_ports(harness, direct=direct, runtime=runtime)

    with pytest.raises(_M5D29HydrationTerminalCutoff):
        harness.application.run_event(plan)

    assert runtime.result_reads == 1
    assert direct.plan_calls == 0
    assert direct.run_calls == 1
    assert harness.world.terminal_telemetry == {}


@dataclass(slots=True)
class _CorruptHydrationStructural(_ObservedStructural):
    corruption: str = "event"

    def plan_exact_requirement_withdrawal(
        self, event: M5TypedEventPlan
    ) -> M5RequirementWithdrawalPlan:
        plan = _ObservedStructural.plan_exact_requirement_withdrawal(self, event)
        if len(self.plans) != 2:
            return plan
        if self.corruption == "event":
            return _unsafe_set(plan, event_id="wrong-event")
        if self.corruption == "deactivated":
            return _unsafe_set(
                plan,
                deactivated_chunk_version_ids=("wrong-chunk",),
            )
        raise AssertionError(f"unknown hydration corruption: {self.corruption}")


@pytest.mark.parametrize("corruption", ["event", "deactivated"])
def test_replayed_document_rejects_invalid_hydrated_plan_binding(
    corruption: str,
) -> None:
    harness, plan, _ = _document_case("delete", f"bad-hydrated-plan:{corruption}")
    _prepare_nonterminal_replay(harness, plan)
    structural = _CorruptHydrationStructural(harness.world, corruption=corruption)
    runtime = _ObservedRuntime(harness.world)
    _install_ports(harness, structural=structural, runtime=runtime)

    with pytest.raises(ValidationError):
        harness.application.run_event(plan)

    assert len(structural.plans) == 2
    assert runtime.revision_reads == 0
    assert "call:direct-runner" not in harness.world.operation_log
