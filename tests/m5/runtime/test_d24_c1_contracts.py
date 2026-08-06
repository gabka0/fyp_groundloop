from __future__ import annotations

from dataclasses import fields, replace

import pytest

from groundloop.errors import ValidationError
from groundloop.m4.application import ObservationCompletionReceipt
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AttemptExecutionEvidence,
    M5DirectAttemptReturnReceipt,
    M5DirectCursorContributionReceipt,
    M5DirectLateCursorContributionReceipt,
    M5DirectLateReturnDisposition,
    M5DirectLateReturnReceipt,
    M5DirectNormalReturnReceipt,
    M5ExecutionEvidenceDisposition,
    M5RequirementAttemptReturnReceipt,
    M5RequirementReturnDisposition,
    M5RuntimeSubgraph,
    M5RuntimeTimingObservation,
    M5RuntimeWork,
    M5RuntimeWorkContributionKind,
    M5TransitionTimingAnchor,
    M5TypedDirectReturnKind,
)

H1 = "1" * 64
H2 = "2" * 64
H3 = "3" * 64
H4 = "4" * 64
H5 = "5" * 64


def _contribution_key(
    epoch_id: int,
    kind: M5RuntimeWorkContributionKind,
    source_id: str,
) -> str:
    return digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=source_id,
    )


def _anchor(
    kind: M5RuntimeWorkContributionKind,
    source_id: str,
    *,
    epoch_id: int = 7,
    revision: int = 11,
) -> M5TransitionTimingAnchor:
    return M5TransitionTimingAnchor.build(
        epoch_id=epoch_id,
        contribution_kind=kind,
        source_id=source_id,
        anchor_revision=revision,
        terminal_transition=False,
    )


def _requirement_receipt(
    disposition: M5RequirementReturnDisposition,
    *,
    exact_replay: bool = False,
    terminal_hash: str | None = None,
    anchor: M5TransitionTimingAnchor | None = None,
    revision: int = 11,
) -> M5RequirementAttemptReturnReceipt:
    return M5RequirementAttemptReturnReceipt(
        disposition=disposition,
        logical_job_id=H1,
        attempt_id=H2,
        resulting_revision=revision,
        exact_replay=exact_replay,
        execution_evidence_digest=H3,
        return_artifact_digest=H4,
        current_terminal_logical_result_hash=terminal_hash,
        transition_anchor=anchor,
    )


def _late_cursor_receipt(
    disposition: M5DirectLateReturnDisposition,
) -> M5DirectLateCursorContributionReceipt:
    preterminal = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
    }
    expired = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
    }
    postterminal = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
    }
    return M5DirectLateCursorContributionReceipt(
        disposition=disposition,
        epoch_id=7,
        job_id="direct-job",
        attempt_id="direct-attempt",
        envelope_digest=H1,
        execution_evidence_digest=H2,
        expired_return_digest=H3 if expired else None,
        attempt_execution_contribution_key_digest=(
            _contribution_key(
                7,
                M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
                "direct-attempt",
            )
            if preterminal
            else None
        ),
        preterminal_late_contribution_key_digest=(
            _contribution_key(
                7,
                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
                "direct-attempt",
            )
            if preterminal
            else None
        ),
        postterminal_logical_result_hash=H4 if postterminal else None,
    )


def _late_return_receipt(
    disposition: M5DirectLateReturnDisposition,
    *,
    exact_replay: bool = False,
    terminal_hash: str | None = None,
    anchor: M5TransitionTimingAnchor | None = None,
    revision: int = 11,
) -> M5DirectLateReturnReceipt:
    expired = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
    }
    return M5DirectLateReturnReceipt(
        disposition=disposition,
        epoch_id=7,
        job_id="direct-job",
        attempt_id="direct-attempt",
        resulting_revision=revision,
        exact_replay=exact_replay,
        envelope_digest=H1,
        execution_evidence_digest=H2,
        expired_return_digest=H3 if expired else None,
        current_terminal_logical_result_hash=terminal_hash,
        transition_anchor=anchor,
    )


def _normal_return_receipt(
    *,
    observation: ObservationCompletionReceipt | None = None,
    exact_replay: bool = False,
    terminal_hash: str | None = None,
    anchor: M5TransitionTimingAnchor | None = None,
    revision: int = 11,
) -> M5DirectNormalReturnReceipt:
    source_id = "direct-transition"
    return M5DirectNormalReturnReceipt(
        epoch_id=7,
        job_id="direct-job",
        attempt_id="direct-attempt",
        resulting_revision=revision,
        exact_replay=exact_replay,
        execution_evidence_digest=H1,
        return_artifact_digest=H2,
        direct_transition_source_id=source_id,
        direct_transition_source_identity_hash=H3,
        direct_transition_contribution_key_digest=_contribution_key(
            7,
            M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            source_id,
        ),
        observation_completion=observation,
        current_terminal_logical_result_hash=terminal_hash,
        transition_anchor=anchor,
    )


def test_c1_wire_enums_and_receipt_topologies_are_exact() -> None:
    assert tuple(item.value for item in M5RequirementReturnDisposition) == (
        "applied",
        "expired_preterminal",
        "expired_postterminal",
        "terminal_audit_preterminal",
        "terminal_audit_postterminal",
    )
    assert tuple(item.value for item in M5DirectLateReturnDisposition) == (
        "expired_preterminal",
        "expired_postterminal",
        "terminal_audit_preterminal",
        "terminal_audit_postterminal",
    )
    expected = {
        M5RequirementAttemptReturnReceipt: (
            "disposition",
            "logical_job_id",
            "attempt_id",
            "resulting_revision",
            "exact_replay",
            "execution_evidence_digest",
            "return_artifact_digest",
            "current_terminal_logical_result_hash",
            "transition_anchor",
        ),
        M5DirectCursorContributionReceipt: (
            "epoch_id",
            "job_id",
            "attempt_id",
            "execution_evidence_digest",
            "attempt_execution_contribution_key_digest",
            "direct_transition_source_id",
            "direct_transition_source_identity_hash",
            "direct_transition_contribution_key_digest",
            "observation_completion",
        ),
        M5DirectLateCursorContributionReceipt: (
            "disposition",
            "epoch_id",
            "job_id",
            "attempt_id",
            "envelope_digest",
            "execution_evidence_digest",
            "expired_return_digest",
            "attempt_execution_contribution_key_digest",
            "preterminal_late_contribution_key_digest",
            "postterminal_logical_result_hash",
        ),
        M5DirectLateReturnReceipt: (
            "disposition",
            "epoch_id",
            "job_id",
            "attempt_id",
            "resulting_revision",
            "exact_replay",
            "envelope_digest",
            "execution_evidence_digest",
            "expired_return_digest",
            "current_terminal_logical_result_hash",
            "transition_anchor",
        ),
        M5DirectNormalReturnReceipt: (
            "epoch_id",
            "job_id",
            "attempt_id",
            "resulting_revision",
            "exact_replay",
            "execution_evidence_digest",
            "return_artifact_digest",
            "direct_transition_source_id",
            "direct_transition_source_identity_hash",
            "direct_transition_contribution_key_digest",
            "observation_completion",
            "current_terminal_logical_result_hash",
            "transition_anchor",
        ),
        M5DirectAttemptReturnReceipt: ("return_kind", "normal", "late"),
    }
    for receipt_type, topology in expected.items():
        assert tuple(field.name for field in fields(receipt_type)) == topology


def test_requirement_receipt_accepts_every_frozen_first_write_branch() -> None:
    root = _requirement_receipt(
        M5RequirementReturnDisposition.APPLIED,
        anchor=_anchor(M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE, H2),
    )
    verifier = _requirement_receipt(
        M5RequirementReturnDisposition.APPLIED,
        anchor=_anchor(M5RuntimeWorkContributionKind.VERIFIER_COMPLETION, H2),
    )
    expired = _requirement_receipt(
        M5RequirementReturnDisposition.EXPIRED_PRETERMINAL,
        anchor=_anchor(M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN, H2),
    )
    terminal_audit = _requirement_receipt(
        M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
        anchor=_anchor(M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN, H2),
    )
    expired_postterminal = _requirement_receipt(
        M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL,
        terminal_hash=H5,
    )
    audit_postterminal = _requirement_receipt(
        M5RequirementReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
        terminal_hash=H5,
    )

    root.validate_anchor_context(
        epoch_id=7,
        expected_kind=M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
    )
    verifier.validate_anchor_context(
        epoch_id=7,
        expected_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
    )
    expired.validate_anchor_context(
        epoch_id=7,
        expected_kind=M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
    )
    assert terminal_audit.transition_anchor is not None
    assert expired_postterminal.transition_anchor is None
    assert audit_postterminal.current_terminal_logical_result_hash == H5


def test_requirement_receipt_replay_keeps_immutable_outcome_but_moves_cutoff() -> None:
    first = _requirement_receipt(
        M5RequirementReturnDisposition.EXPIRED_PRETERMINAL,
        anchor=_anchor(M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN, H2),
    )
    replay_before_terminal = replace(
        first,
        exact_replay=True,
        transition_anchor=None,
    )
    replay_after_terminal = replace(
        replay_before_terminal,
        resulting_revision=14,
        current_terminal_logical_result_hash=H5,
    )

    assert replay_before_terminal.disposition is first.disposition
    assert replay_after_terminal.disposition is first.disposition
    assert (
        replay_after_terminal.execution_evidence_digest
        == first.execution_evidence_digest
    )
    assert replay_after_terminal.return_artifact_digest == first.return_artifact_digest
    assert replay_after_terminal.resulting_revision == 14
    assert replay_after_terminal.transition_anchor is None


def test_requirement_receipt_rejects_illegal_anchor_and_terminal_shapes() -> None:
    first_anchor = _anchor(M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE, H2)
    first = _requirement_receipt(
        M5RequirementReturnDisposition.APPLIED,
        anchor=first_anchor,
    )
    with pytest.raises(ValidationError):
        _requirement_receipt(M5RequirementReturnDisposition.APPLIED)
    with pytest.raises(ValidationError):
        _requirement_receipt(
            M5RequirementReturnDisposition.APPLIED,
            terminal_hash=H5,
            anchor=first_anchor,
        )
    with pytest.raises(ValidationError):
        _requirement_receipt(
            M5RequirementReturnDisposition.APPLIED,
            exact_replay=True,
            anchor=first_anchor,
        )
    with pytest.raises(ValidationError):
        _requirement_receipt(M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL)
    with pytest.raises(ValidationError):
        _requirement_receipt(
            M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL,
            terminal_hash=H5,
            anchor=_anchor(M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN, H2),
        )
    with pytest.raises(ValidationError):
        _requirement_receipt(
            M5RequirementReturnDisposition.EXPIRED_PRETERMINAL,
            anchor=_anchor(M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE, H2),
        )
    with pytest.raises(ValidationError):
        _requirement_receipt(
            M5RequirementReturnDisposition.APPLIED,
            anchor=_anchor(M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE, H1),
        )
    with pytest.raises(ValidationError):
        replace(first, return_artifact_digest="not-a-hash")
    with pytest.raises(ValidationError):
        first.validate_anchor_context(
            epoch_id=8,
            expected_kind=M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE,
        )
    with pytest.raises(ValidationError):
        first.validate_anchor_context(
            epoch_id=7,
            expected_kind=M5RuntimeWorkContributionKind.VERIFIER_COMPLETION,
        )


def test_direct_cursor_receipt_separates_candidates_from_outer_outcome() -> None:
    attempt_key = _contribution_key(
        7,
        M5RuntimeWorkContributionKind.DIRECT_ATTEMPT_EXECUTION,
        "direct-attempt",
    )
    transition_key = _contribution_key(
        7,
        M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
        "direct-transition",
    )
    expansion = M5DirectCursorContributionReceipt(
        epoch_id=7,
        job_id="direct-job",
        attempt_id="direct-attempt",
        execution_evidence_digest=H1,
        attempt_execution_contribution_key_digest=attempt_key,
        direct_transition_source_id="direct-transition",
        direct_transition_source_identity_hash=H2,
        direct_transition_contribution_key_digest=transition_key,
        observation_completion=None,
    )
    observation = ObservationCompletionReceipt(True, True)
    verifier = replace(expansion, observation_completion=observation)
    failure = replace(
        expansion,
        direct_transition_source_id=None,
        direct_transition_source_identity_hash=None,
        direct_transition_contribution_key_digest=None,
    )

    assert verifier.observation_completion is observation
    assert failure.observation_completion is None
    assert not hasattr(expansion, "resulting_revision")
    assert not hasattr(expansion, "exact_replay")
    assert not hasattr(expansion, "transition_anchor")

    with pytest.raises(ValidationError):
        replace(expansion, attempt_execution_contribution_key_digest=H3)
    with pytest.raises(ValidationError):
        replace(expansion, direct_transition_source_identity_hash=None)
    with pytest.raises(ValidationError):
        replace(expansion, direct_transition_contribution_key_digest=H3)
    with pytest.raises(ValidationError):
        replace(failure, observation_completion=observation)


@pytest.mark.parametrize("disposition", tuple(M5DirectLateReturnDisposition))
def test_direct_late_cursor_receipt_pins_every_disposition_shape(
    disposition: M5DirectLateReturnDisposition,
) -> None:
    receipt = _late_cursor_receipt(disposition)
    preterminal = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
    }
    postterminal = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
    }
    expired = disposition in {
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
    }
    assert (receipt.expired_return_digest is not None) is expired
    assert (
        receipt.attempt_execution_contribution_key_digest is not None
    ) is preterminal
    assert (receipt.preterminal_late_contribution_key_digest is not None) is preterminal
    assert (receipt.postterminal_logical_result_hash is not None) is postterminal
    assert not hasattr(receipt, "resulting_revision")
    assert not hasattr(receipt, "transition_anchor")


def test_direct_late_cursor_receipt_rejects_mixed_or_wrong_keys() -> None:
    preterminal = _late_cursor_receipt(
        M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL
    )
    postterminal = _late_cursor_receipt(
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL
    )
    with pytest.raises(ValidationError):
        replace(preterminal, expired_return_digest=None)
    with pytest.raises(ValidationError):
        replace(preterminal, attempt_execution_contribution_key_digest=None)
    with pytest.raises(ValidationError):
        replace(preterminal, preterminal_late_contribution_key_digest=H5)
    with pytest.raises(ValidationError):
        replace(preterminal, postterminal_logical_result_hash=H5)
    with pytest.raises(ValidationError):
        replace(postterminal, attempt_execution_contribution_key_digest=H5)
    with pytest.raises(ValidationError):
        replace(postterminal, postterminal_logical_result_hash=None)


def test_direct_late_outer_receipt_separates_first_write_replay_and_terminal() -> None:
    first = _late_return_receipt(
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_PRETERMINAL,
        anchor=_anchor(
            M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
            "direct-attempt",
        ),
    )
    replay_before_terminal = replace(first, exact_replay=True, transition_anchor=None)
    replay_after_terminal = replace(
        replay_before_terminal,
        resulting_revision=17,
        current_terminal_logical_result_hash=H5,
    )
    postterminal = _late_return_receipt(
        M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL,
        terminal_hash=H5,
    )

    assert replay_after_terminal.disposition is first.disposition
    assert replay_after_terminal.envelope_digest == first.envelope_digest
    assert replay_after_terminal.resulting_revision == 17
    assert postterminal.transition_anchor is None

    with pytest.raises(ValidationError):
        _late_return_receipt(
            M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
            anchor=_anchor(
                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
                "another-attempt",
            ),
        )
    with pytest.raises(ValidationError):
        _late_return_receipt(
            M5DirectLateReturnDisposition.EXPIRED_PRETERMINAL,
            anchor=_anchor(
                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
                "direct-attempt",
                epoch_id=8,
            ),
        )
    with pytest.raises(ValidationError):
        _late_return_receipt(
            M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
            terminal_hash=H5,
            anchor=_anchor(
                M5RuntimeWorkContributionKind.PRETERMINAL_LATE_RETURN,
                "direct-attempt",
            ),
        )
    with pytest.raises(ValidationError):
        _late_return_receipt(M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL)


def test_direct_normal_and_wrapper_receipts_pin_branch_and_m4_receipt() -> None:
    first_discovery = _normal_return_receipt(
        anchor=_anchor(
            M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            "direct-transition",
        )
    )
    discovery = M5DirectAttemptReturnReceipt(
        M5TypedDirectReturnKind.DISCOVERY,
        first_discovery,
        None,
    )
    observation = ObservationCompletionReceipt(True, False)
    first_verifier = _normal_return_receipt(
        observation=observation,
        anchor=_anchor(
            M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
            "direct-transition",
        ),
    )
    verifier = M5DirectAttemptReturnReceipt(
        M5TypedDirectReturnKind.VERIFIER,
        first_verifier,
        None,
    )
    late = _late_return_receipt(
        M5DirectLateReturnDisposition.EXPIRED_POSTTERMINAL,
        terminal_hash=H5,
    )
    late_wrapper = M5DirectAttemptReturnReceipt(
        M5TypedDirectReturnKind.DISCOVERY,
        None,
        late,
    )
    replay_after_terminal = replace(
        first_discovery,
        exact_replay=True,
        resulting_revision=18,
        current_terminal_logical_result_hash=H5,
        transition_anchor=None,
    )

    assert discovery.normal is first_discovery
    assert verifier.normal is first_verifier
    assert verifier.normal.observation_completion is observation
    assert late_wrapper.late is late
    assert replay_after_terminal.return_artifact_digest == H2
    assert replay_after_terminal.transition_anchor is None

    with pytest.raises(ValidationError):
        M5DirectAttemptReturnReceipt(
            M5TypedDirectReturnKind.DISCOVERY,
            first_discovery,
            late,
        )
    with pytest.raises(ValidationError):
        M5DirectAttemptReturnReceipt(
            M5TypedDirectReturnKind.DISCOVERY,
            None,
            None,
        )
    with pytest.raises(ValidationError):
        M5DirectAttemptReturnReceipt(
            M5TypedDirectReturnKind.VERIFIER,
            first_discovery,
            None,
        )
    with pytest.raises(ValidationError):
        M5DirectAttemptReturnReceipt(
            M5TypedDirectReturnKind.DISCOVERY,
            first_verifier,
            None,
        )
    with pytest.raises(ValidationError):
        replace(first_discovery, direct_transition_contribution_key_digest=H4)
    with pytest.raises(ValidationError):
        replace(
            first_discovery,
            transition_anchor=_anchor(
                M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
                "another-transition",
            ),
        )
    with pytest.raises(ValidationError):
        replace(first_discovery, current_terminal_logical_result_hash=H5)


def test_equal_zero_work_returned_and_reused_evidence_are_distinct() -> None:
    observation = M5RuntimeTimingObservation.build(None)
    timing_digest = digests.attempt_runtime_timing_digest(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H2,
        observation_digest=observation.observation_digest,
    )
    returned = M5AttemptExecutionEvidence.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H2,
        disposition=M5ExecutionEvidenceDisposition.RETURNED,
        result_or_error_hash=H3,
        attempt_work=M5RuntimeWork(),
        attempt_timing_digest=timing_digest,
    )
    reused = M5AttemptExecutionEvidence.build(
        epoch_id=7,
        subgraph=M5RuntimeSubgraph.REQUIREMENT,
        attempt_id=H2,
        disposition=M5ExecutionEvidenceDisposition.REUSED_ARTIFACT,
        result_or_error_hash=H3,
        attempt_work=M5RuntimeWork(),
        attempt_timing_digest=timing_digest,
    )

    assert returned.attempt_work == reused.attempt_work
    assert returned.evidence_digest != reused.evidence_digest


def test_receipts_reject_wrong_runtime_types_and_invalid_hashes() -> None:
    with pytest.raises(ValidationError):
        M5RequirementAttemptReturnReceipt(  # type: ignore[arg-type]
            disposition="applied",
            logical_job_id=H1,
            attempt_id=H2,
            resulting_revision=11,
            exact_replay=False,
            execution_evidence_digest=H3,
            return_artifact_digest=H4,
            current_terminal_logical_result_hash=None,
            transition_anchor=_anchor(
                M5RuntimeWorkContributionKind.ROOT_RESULT_STAGE, H2
            ),
        )
    with pytest.raises(ValidationError):
        M5DirectAttemptReturnReceipt(  # type: ignore[arg-type]
            "discovery",
            _normal_return_receipt(
                anchor=_anchor(
                    M5RuntimeWorkContributionKind.DIRECT_TRANSITION,
                    "direct-transition",
                )
            ),
            None,
        )
    with pytest.raises(ValidationError):
        replace(
            _late_cursor_receipt(
                M5DirectLateReturnDisposition.TERMINAL_AUDIT_POSTTERMINAL
            ),
            envelope_digest="bad",
        )
