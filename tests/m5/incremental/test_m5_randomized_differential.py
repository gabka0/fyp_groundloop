"""Frozen Step 7 randomized differential manifest and executable gate."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from run_m5_differential import (
    DEFAULT_CONFIG,
    DEFAULT_MANIFEST,
    FROZEN_COMMITTED_EVENTS,
    FROZEN_SEED,
    FROZEN_SHORT_PREFIX,
    PROPOSAL_KINDS,
    SplitMix64,
    _apply_and_validate,
    _assert_certificate_histories,
    _assert_certificate_surface_changes,
    _assert_current_certificates,
    _assert_overlay_matches_reference,
    _assert_shard_audit,
    _binding_rows_by_owner,
    _build_independent_audit,
    _certificate_history_digest,
    _certificate_image,
    _logical_state_digest,
    _published_history_rows,
    _replay_and_validate,
    _repository_digest,
    _stream_seed,
    _StreamGenerator,
    frozen_summary,
    load_config,
    load_manifest,
    run_randomized_differential,
    verify_manifest_header,
    verify_manifest_summary,
)

from groundloop.domain import DecisionPolicy, StatusDelta, VerificationLabel
from groundloop.events import (
    ChunkInput,
    InsertDocumentEvent,
    PolicyChangeEvent,
    apply_event,
)
from groundloop.m5.claim_certificates import WorkingClaimCertificateBinding
from groundloop.m5.domain import SnapshotPoint
from groundloop.m5.events import (
    ObserveRequirementEvent,
    RegisterGroupEvent,
    apply_m5_event,
)
from groundloop.m5.incremental_overlay import (
    M5IncrementalOverlay,
    M5OverlayWork,
    _history_append,
)
from groundloop.m5.matching import WorkingGroupCertificateBinding
from groundloop.m5.reference import (
    build_reference_group_certificate,
    validate_group_certificate,
)

from .helpers import make_group, make_repository, make_requirement_observation

ROOT = Path(__file__).resolve().parents[3]
EVENT_CLASSES = {
    "DeleteDocumentVersionEvent",
    "InsertDocumentEvent",
    "ObserveEvent",
    "ObserveRequirementEvent",
    "PolicyChangeEvent",
    "RegisterGroupEvent",
    "ReplaceDocumentVersionEvent",
    "ReplaceGroupEvent",
    "RetireGroupEvent",
}


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and set(value) <= set("0123456789abcdef")
    )


def _counter_total(summary: dict[str, object], key: str) -> int:
    values = cast(dict[str, int], summary[key])
    return sum(values.values())


def _assert_generator_accounting(summary: dict[str, object]) -> None:
    assert summary["proposals"] == (
        cast(int, summary["committed_events"])
        + cast(int, summary["exact_replays"])
        + cast(int, summary["generator_rejections"])
    )
    assert _counter_total(summary, "event_type_counts") == summary["committed_events"]
    assert (
        _counter_total(summary, "replay_event_type_counts") == summary["exact_replays"]
    )
    assert (
        _counter_total(summary, "rejection_reason_counts")
        == summary["generator_rejections"]
    )
    assert _counter_total(summary, "proposal_kind_counts") == summary["proposals"]
    assert (
        _counter_total(summary, "committed_proposal_kind_counts")
        == summary["committed_events"]
    )
    assert cast(int, summary["max_proposals_in_stream"]) <= 400
    assert _is_sha256(summary["fixture_stream_sha256"])
    assert _is_sha256(summary["proposal_stream_sha256"])
    assert _is_sha256(summary["committed_event_stream_sha256"])


def test_frozen_config_contract_and_derived_bounds(tmp_path: Path) -> None:
    config = load_config()
    assert config.seed == FROZEN_SEED
    assert config.committed_events == FROZEN_COMMITTED_EVENTS
    assert config.committed_events_per_stream == 100
    assert config.max_proposals_per_stream == 400
    assert config.short_prefix_committed_events == FROZEN_SHORT_PREFIX
    assert config.total_weight == 100
    assert {row.kind for row in config.event_mix} == PROPOSAL_KINDS
    assert config.derived_runtime_bounds == {
        "active_groups": 6,
        "active_chunks": 16,
        "n_obs": 288,
        "r_max": 3,
        "w_g": 48,
        "e_g": 18,
        "h_g": 6,
        "reference_matching_leaf_bound": 2058,
    }
    assert _is_sha256(config.raw_config_sha256)
    assert _is_sha256(config.config_sha256)
    assert _is_sha256(config.event_weights_sha256)

    original = json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    mutations: tuple[tuple[str, object], ...] = (
        ("unknown", 1),
        ("seed", 1 << 64),
        ("max_proposals_per_stream", 99),
        ("short_prefix_committed_events", 249),
    )
    for index, (key, value) in enumerate(mutations):
        mutated = deepcopy(original)
        mutated[key] = value
        path = tmp_path / f"invalid-{index}.json"
        path.write_text(json.dumps(mutated), encoding="utf-8")
        with pytest.raises(ValueError):
            load_config(path)


def test_splitmix64_v1_golden_vectors() -> None:
    generator = SplitMix64(0)
    assert [f"{generator.next_u64():016x}" for _ in range(8)] == [
        "e220a8397b1dcdaf",
        "6e789e6aa1b965f4",
        "06c45d188009454f",
        "f88bb8a8724c81ec",
        "1b39896a51a8749b",
        "53cb9f0c747ea2ea",
        "2c829abe1f4532e1",
        "c584133ac916ab3c",
    ]
    seeded = SplitMix64(FROZEN_SEED)
    assert [f"{seeded.next_u64():016x}" for _ in range(4)] == [
        "8edcbcaee541dc2e",
        "141c011313bc1558",
        "2f663dbb660154a8",
        "24fe42970483d156",
    ]
    rejection_sensitive = SplitMix64(FROZEN_SEED)
    assert [rejection_sensitive.randbelow(2**63 + 1) for _ in range(4)] == [
        1449034361553556824,
        3415485242486641832,
        2665641245833154902,
        4517337008065080265,
    ]
    assert [_stream_seed(FROZEN_SEED, stream_id) for stream_id in range(4)] == [
        12407476148023715191,
        13440045467942312128,
        10505072573342825421,
        7959133838014334505,
    ]
    with pytest.raises(ValueError):
        SplitMix64(0).randbelow(0)


def test_planned_prefix_and_full_match_frozen_manifest() -> None:
    config = load_config()
    manifest, raw_sha256, canonical_sha256 = load_manifest()
    verify_manifest_header(config, manifest)
    assert raw_sha256 == (
        "ea6b6b7e43e8ca39c955b4b35d051de329e12fee2940c5d541a39d37dd61e7a9"
    )
    assert canonical_sha256 == (
        "e98b276043340a43ea13fd3f949aecfedf7df83474f7fe293cf77d45b6d230c5"
    )

    prefix = run_randomized_differential(
        config,
        committed_events=config.short_prefix_committed_events,
        validate=False,
    )
    full = run_randomized_differential(
        config,
        committed_events=config.committed_events,
        validate=False,
    )
    verify_manifest_summary(prefix, manifest, "planned_prefix")
    verify_manifest_summary(full, manifest, "planned_full")
    assert full["short_prefix_checkpoint"] == frozen_summary(prefix)

    for summary in (prefix, full):
        frozen = frozen_summary(summary)
        _assert_generator_accounting(frozen)
        assert set(cast(dict[str, int], frozen["event_type_counts"])) == EVENT_CLASSES
        assert set(cast(dict[str, int], frozen["proposal_kind_counts"])) == (
            PROPOSAL_KINDS
        )
        assert cast(int, frozen["exact_replays"]) > 0
        assert cast(int, frozen["generator_rejections"]) > 0
        assert cast(int, frozen["duplicate_chunks_generated"]) > 0

    drifted = deepcopy(manifest)
    planned_prefix = cast(dict[str, object], drifted["planned_prefix"])
    planned_prefix["proposal_stream_sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        verify_manifest_summary(prefix, drifted, "planned_prefix")


def test_short_validated_prefix_matches_manifest_and_runtime_bounds() -> None:
    config = load_config()
    manifest, _, _ = load_manifest()
    result = run_randomized_differential(
        config,
        committed_events=config.short_prefix_committed_events,
        validate=True,
    )
    verify_manifest_summary(result, manifest, "planned_prefix")
    _assert_generator_accounting(frozen_summary(result))
    assert result["validated"] is True
    assert result["state_checks"] == config.short_prefix_committed_events
    assert result["replay_checks"] == result["exact_replays"]
    assert result["generator_rejection_checks"] == result["generator_rejections"]
    assert result["shard_audits"] == 3
    assert result["matching_index_audits"] == 3
    assert result["history_audits"] == 3
    assert cast(int, result["current_certificate_checks"]) > 0
    assert cast(int, result["public_certificate_checks"]) > 0
    assert _is_sha256(result["execution_trace_sha256"])

    observed = cast(dict[str, int], result["observed_runtime_bounds"])
    bounds = config.derived_runtime_bounds
    assert observed["max_active_groups"] <= bounds["active_groups"]
    assert observed["max_transaction_n_obs"] <= bounds["n_obs"]
    assert observed["max_observed_r"] <= bounds["r_max"]
    assert observed["max_group_w"] <= bounds["w_g"]
    assert observed["max_group_e"] <= bounds["e_g"]
    assert observed["max_group_h"] <= bounds["h_g"]
    assert observed["structural_versions_audited"] > 0


@pytest.mark.parametrize("proposal_kind", ("policy_change", "register_group"))
def test_replay_and_generator_rejection_are_state_noops(
    proposal_kind: str,
) -> None:
    config = load_config()
    generator = _StreamGenerator(
        config,
        0,
        SplitMix64(_stream_seed(config.seed, 0)),
    )
    repository, _ = generator.fixture_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    before_audit = _build_independent_audit(repository)
    before_shadow = generator.shadow_checkpoint()
    proposal = generator.propose(proposal_kind, f"event-{proposal_kind}")
    committed_shadow = generator.shadow_checkpoint()
    generator.restore_shadow(before_shadow)
    assert proposal.event is not None
    result, _, _ = _apply_and_validate(
        repository,
        overlay,
        proposal.event,
        before_audit,
    )
    assert not result.replayed
    generator.restore_shadow(committed_shadow)
    generator.record_committed(proposal.event)

    before_replay = (
        _repository_digest(repository),
        _logical_state_digest(overlay),
        _certificate_history_digest(overlay),
    )
    replay = _replay_and_validate(repository, overlay, proposal.event)
    assert replay.replayed
    assert replay.work == M5OverlayWork()
    assert (
        _repository_digest(repository),
        _logical_state_digest(overlay),
        _certificate_history_digest(overlay),
    ) == before_replay

    rejection_shadow = generator.shadow_checkpoint()
    rejection = generator.propose("generator_rejection", "rejected-event")
    assert rejection.event is None
    assert rejection.rejection_reason == "scheduled_generator_rejection"
    assert generator.shadow_checkpoint() == rejection_shadow
    assert (
        _repository_digest(repository),
        _logical_state_digest(overlay),
        _certificate_history_digest(overlay),
    ) == before_replay


def test_certificate_audit_rejects_stale_bindings_and_false_dirtiness() -> None:
    config = load_config()
    generator = _StreamGenerator(
        config,
        0,
        SplitMix64(_stream_seed(config.seed, 0)),
    )
    repository, _ = generator.fixture_repository()
    overlay = M5IncrementalOverlay.from_repository(repository)
    initial_audit = _build_independent_audit(repository)

    stale_binding = next(iter(overlay._claim_bindings.values()))
    overlay._claim_bindings["unrelated-stale-key"] = stale_binding
    with pytest.raises(AssertionError, match="binding keyset"):
        _assert_current_certificates(repository, overlay, initial_audit)
    with pytest.raises(AssertionError, match="binding keyset"):
        _assert_certificate_histories(repository, overlay)
    with pytest.raises(AssertionError, match="binding keyset"):
        _assert_shard_audit(repository, overlay)
    del overlay._claim_bindings["unrelated-stale-key"]

    before_repository = deepcopy(repository)
    before_certificates = _certificate_image(overlay)
    event = PolicyChangeEvent(
        event_id="certificate-dirtiness-policy",
        policy=DecisionPolicy("certificate-dirtiness-policy-v2", 0.8, 0.8),
    )
    apply_event(repository.base, event)
    _ = repository.current_point
    result = overlay.apply_committed_event(event, before_repository, repository)
    after_audit = _build_independent_audit(repository)
    _assert_overlay_matches_reference(
        repository,
        overlay,
        event,
        initial_audit,
        before_certificates,
        after_audit,
        result,
    )
    assert result.certificate_only_claim_ids

    false_dirtiness = replace(result, certificate_only_claim_ids=())
    with pytest.raises(AssertionError, match="independently exact"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            initial_audit,
            before_certificates,
            after_audit,
            false_dirtiness,
        )
    false_work = replace(
        result,
        work=replace(result.work, claim_certificate_only_changes=0),
    )
    with pytest.raises(AssertionError, match="work counter"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            initial_audit,
            before_certificates,
            after_audit,
            false_work,
        )
    fabricated_delta = StatusDelta(
        event_id=event.event_id,
        object_type="claim",
        object_id=next(iter(after_audit.states.claims)),
        old_status="unsupported",
        new_status="supported",
        reason=f"event={event.event_id} op={type(event).__name__}",
    )
    false_deltas = replace(
        result,
        deltas=(fabricated_delta,),
        work=replace(
            result.work,
            matching=replace(result.work.matching, claim_status_changes=1),
            public_status_deltas=1,
        ),
    )
    with pytest.raises(AssertionError, match="public status deltas"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            initial_audit,
            before_certificates,
            after_audit,
            false_deltas,
        )
    with pytest.raises(AssertionError, match="result event ID"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            initial_audit,
            before_certificates,
            after_audit,
            replace(result, event_id="wrong-event-id"),
        )
    with pytest.raises(AssertionError, match="result point"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            initial_audit,
            before_certificates,
            after_audit,
            replace(result, point=before_repository.current_point),
        )
    closed = WorkingClaimCertificateBinding(
        epoch_id=1,
        claim_id="claim-order",
        valid_from_revision=0,
        valid_to_revision=1,
        certificate_digest="a" * 64,
    )
    opened = WorkingClaimCertificateBinding(
        epoch_id=1,
        claim_id="claim-order",
        valid_from_revision=1,
        valid_to_revision=None,
        certificate_digest="b" * 64,
    )
    assert _binding_rows_by_owner(
        (closed, opened),
        owner_attribute="claim_id",
    ) != _binding_rows_by_owner(
        (opened, closed),
        owner_attribute="claim_id",
    )

    prior_epoch = WorkingGroupCertificateBinding(
        epoch_id=5,
        group_version_id="group-carry",
        valid_from_revision=0,
        valid_to_revision=None,
        certificate_digest="c" * 64,
    )
    carried = WorkingGroupCertificateBinding(
        epoch_id=6,
        group_version_id="group-carry",
        valid_from_revision=0,
        valid_to_revision=None,
        certificate_digest="c" * 64,
    )
    assert _published_history_rows(
        {"group-carry": (prior_epoch,)},
        {"group-carry": (prior_epoch, carried)},
        name="group binding",
        point=SnapshotPoint(6, 0),
    ) == (carried,)
    with pytest.raises(AssertionError, match="prior epoch"):
        _published_history_rows(
            {"group-carry": (prior_epoch,)},
            {
                "group-carry": (
                    replace(prior_epoch, valid_to_revision=1),
                    carried,
                )
            },
            name="group binding",
            point=SnapshotPoint(6, 0),
        )


def test_certificate_audit_rejects_internally_consistent_unrelated_rebinding() -> None:
    repository = make_repository()
    apply_m5_event(
        repository,
        RegisterGroupEvent(event_id="audit-register-group", group=make_group()),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="audit-complete-group",
            observation=make_requirement_observation(
                observation_id="audit-selected-support",
                requirement_id="group-a-requirement-0",
            ),
        ),
    )
    apply_m5_event(
        repository,
        ObserveRequirementEvent(
            event_id="audit-add-alternative-support",
            observation=make_requirement_observation(
                observation_id="audit-alternative-support",
                requirement_id="group-a-requirement-0",
                chunk_id="chunk-b",
            ),
        ),
    )
    overlay = M5IncrementalOverlay.from_repository(repository)
    before_repository = deepcopy(repository)
    before_audit = _build_independent_audit(repository)
    before_certificates = _certificate_image(overlay)
    event = InsertDocumentEvent(
        event_id="audit-unrelated-document",
        document_id="audit-unrelated-document",
        document_version_id="audit-unrelated-version",
        content_hash="f" * 64,
        chunks=(ChunkInput("audit-unrelated-chunk", 0, "unrelated text"),),
    )
    apply_event(repository.base, event)
    _ = repository.current_point
    result = overlay.apply_committed_event(event, before_repository, repository)
    after_audit = _build_independent_audit(repository)
    _assert_overlay_matches_reference(
        repository,
        overlay,
        event,
        before_audit,
        before_certificates,
        after_audit,
        result,
    )
    assert result.changed_group_ids == ()
    assert result.changed_claim_ids == ()

    claim_id = next(iter(overlay._claim_bindings))
    old_claim_binding = overlay._claim_bindings[claim_id]
    old_claim_history = overlay._claim_binding_history[claim_id]
    extra_claim_binding = WorkingClaimCertificateBinding(
        epoch_id=repository.current_point.epoch_id,
        claim_id=claim_id,
        valid_from_revision=repository.current_point.revision,
        valid_to_revision=None,
        certificate_digest=old_claim_binding.certificate_digest,
    )
    overlay._claim_bindings[claim_id] = extra_claim_binding
    overlay._claim_binding_history[claim_id] = _history_append(
        old_claim_history,
        extra_claim_binding,
    )
    false_claim_result = replace(
        result,
        changed_claim_ids=(claim_id,),
        certificate_only_claim_ids=(claim_id,),
        published_claim_bindings=(extra_claim_binding,),
        work=replace(
            result.work,
            matching=replace(
                result.work.matching,
                claims_touched=result.work.matching.claims_touched + 1,
            ),
            claim_certificate_only_changes=(
                result.work.claim_certificate_only_changes + 1
            ),
        ),
    )
    with pytest.raises(AssertionError, match="claim certificate surface"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            before_audit,
            before_certificates,
            after_audit,
            false_claim_result,
        )
    overlay._claim_bindings[claim_id] = old_claim_binding
    overlay._claim_binding_history[claim_id] = old_claim_history

    group_id = next(iter(overlay._group_bindings))
    old_group_binding = overlay._group_bindings[group_id]
    old_group_history = overlay._group_binding_history[group_id]
    extra_group_binding = WorkingGroupCertificateBinding(
        epoch_id=repository.current_point.epoch_id,
        group_version_id=group_id,
        valid_from_revision=repository.current_point.revision,
        valid_to_revision=None,
        certificate_digest=old_group_binding.certificate_digest,
    )
    overlay._group_bindings[group_id] = extra_group_binding
    overlay._group_binding_history[group_id] = _history_append(
        overlay._group_binding_history[group_id],
        extra_group_binding,
    )
    false_group_result = replace(
        result,
        changed_group_ids=(group_id,),
        certificate_only_group_ids=(group_id,),
        published_group_bindings=(extra_group_binding,),
        work=replace(
            result.work,
            matching=replace(
                result.work.matching,
                groups_touched=result.work.matching.groups_touched + 1,
            ),
            group_certificate_only_changes=(
                result.work.group_certificate_only_changes + 1
            ),
        ),
    )
    with pytest.raises(AssertionError, match="group certificate surface"):
        _assert_overlay_matches_reference(
            repository,
            overlay,
            event,
            before_audit,
            before_certificates,
            after_audit,
            false_group_result,
        )
    overlay._group_bindings[group_id] = old_group_binding
    overlay._group_binding_history[group_id] = old_group_history

    after_certificates = _certificate_image(overlay)
    current_artifact = after_certificates.group_artifacts[group_id]
    selected_id = current_artifact.rows[0].selected_observation_id
    alternative = next(
        indexed
        for indexed in after_audit.indexed_by_id.values()
        if indexed.group_version_id == group_id
        and indexed.label is VerificationLabel.SUPPORT
        and indexed.observation_id != selected_id
    )
    alternative_row = replace(
        current_artifact.rows[0],
        text_hash=alternative.text_hash,
        selected_observation_id=alternative.observation_id,
    )
    alternative_artifact = replace(
        current_artifact,
        rows=(alternative_row,),
        certificate_digest="",
    )
    assert validate_group_certificate(repository, alternative_artifact)
    false_artifact_image = replace(
        after_certificates,
        group_artifacts={
            **after_certificates.group_artifacts,
            group_id: alternative_artifact,
        },
    )
    with pytest.raises(AssertionError, match="group artifact"):
        _assert_certificate_surface_changes(
            before_certificates,
            false_artifact_image,
            before_audit,
            after_audit,
            event,
            repository.current_point,
            set(),
            set(),
            result,
        )


def test_certificate_audit_accepts_alternate_valid_build_identity() -> None:
    repository = make_repository()
    apply_event(
        repository.base,
        PolicyChangeEvent(
            event_id="ambiguous-high-policy",
            policy=DecisionPolicy("policy-high", 0.95, 0.95),
        ),
    )
    _ = repository.current_point
    apply_m5_event(
        repository,
        RegisterGroupEvent(
            event_id="ambiguous-register-group",
            group=make_group(texts=("r0", "r1")),
        ),
    )
    for ordinal in range(2):
        for chunk_id in ("chunk-a", "chunk-b"):
            apply_m5_event(
                repository,
                ObserveRequirementEvent(
                    event_id=f"ambiguous-observe-{ordinal}-{chunk_id}",
                    observation=make_requirement_observation(
                        observation_id=f"ambiguous-{ordinal}-{chunk_id}",
                        requirement_id=f"group-a-requirement-{ordinal}",
                        chunk_id=chunk_id,
                        scores=(0.9, 0.05, 0.05),
                    ),
                ),
            )

    overlay = M5IncrementalOverlay.from_repository(repository)
    assert overlay.group_certificates == {}
    before_repository = deepcopy(repository)
    before_audit = _build_independent_audit(repository)
    before_certificates = _certificate_image(overlay)
    event = PolicyChangeEvent(
        event_id="ambiguous-low-policy",
        policy=DecisionPolicy("policy-low", 0.8, 0.8),
    )
    apply_event(repository.base, event)
    _ = repository.current_point
    result = overlay.apply_committed_event(event, before_repository, repository)
    after_audit = _build_independent_audit(repository)
    _assert_overlay_matches_reference(
        repository,
        overlay,
        event,
        before_audit,
        before_certificates,
        after_audit,
        result,
    )

    actual = overlay.group_certificates["group-a"]
    reference = build_reference_group_certificate(repository, "group-a")
    assert reference is not None
    assert actual != reference
    assert validate_group_certificate(repository, actual)
    assert validate_group_certificate(repository, reference)


@pytest.mark.parametrize("hash_seed", ("0", "1", "42", "8675309"))
def test_validated_prefix_is_pythonhashseed_stable(hash_seed: str) -> None:
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    python_paths = (str(ROOT / "src"), str(ROOT / "experiments/streams"))
    environment["PYTHONPATH"] = os.pathsep.join(
        (*python_paths, *((existing,) if existing else ()))
    )
    environment["PYTHONHASHSEED"] = hash_seed
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "experiments/streams/run_m5_differential.py"),
            "--mode",
            "prefix",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    actual = json.loads(completed.stdout)
    config = load_config()
    expected = run_randomized_differential(
        config,
        committed_events=config.short_prefix_committed_events,
        validate=True,
    )
    manifest, raw_sha256, canonical_sha256 = load_manifest()
    expected["manifest_raw_sha256"] = raw_sha256
    expected["manifest_canonical_sha256"] = canonical_sha256
    expected["verified_manifest_section"] = "planned_prefix"
    for result in (actual, expected):
        result.pop("elapsed_seconds")
        result.pop("events_per_second")
    assert actual == expected
    verify_manifest_summary(actual, manifest, "planned_prefix")


@pytest.mark.skipif(
    os.environ.get("GROUNDLOOP_RUN_M5_100K_DIFFERENTIAL") != "1",
    reason="the full frozen M5 randomized gate is explicit opt-in evidence",
)
def test_frozen_100k_gate_matches_manifest() -> None:
    config = load_config()
    manifest, _, _ = load_manifest(DEFAULT_MANIFEST)
    result = run_randomized_differential(
        config,
        committed_events=config.committed_events,
        validate=True,
    )
    verify_manifest_summary(result, manifest, "planned_full")
    assert result["state_checks"] == FROZEN_COMMITTED_EVENTS
    assert result["mismatches"] == 0
