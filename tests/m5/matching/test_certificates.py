from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace

import pytest

import groundloop.m5.domain as m5_domain
import groundloop.m5.matching as matching_module
from groundloop.errors import ValidationError
from groundloop.m5.matching import (
    CertificateRebuildRequired,
    CertificateSnapshot,
    CertificateTransitionKind,
    CertificateTransitionResult,
    GroupMatchingCertificateRow,
    build_or_rebuild_certificate,
    carry_forward_certificate_epoch,
    close_incomplete_certificate,
    compute_group_certificate_digest,
    initialize_hall_mask_state,
    policy_range_probe_work,
    rebind_certificate_policy,
    reconstruct_certificate,
    repair_selected_observations,
    validate_bound_certificate,
    validate_certificate_artifact,
)


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _snapshot(
    *,
    edge_observations: dict[tuple[int, str], tuple[str, ...]],
    requirement_count: int,
    revision: int,
    policy: str = "policy-v1",
    epoch_id: int = 11,
) -> CertificateSnapshot:
    masks: dict[str, int] = {}
    for (ordinal, text_hash), observations in edge_observations.items():
        if observations:
            masks[text_hash] = masks.get(text_hash, 0) | (1 << ordinal)
    return CertificateSnapshot.from_primitives(
        epoch_id=epoch_id,
        revision=revision,
        decision_policy_version=policy,
        group_version_id="group-v1",
        requirement_version_ids=tuple(
            f"requirement-{ordinal}" for ordinal in range(requirement_count)
        ),
        hash_masks=masks,
        observation_ids_by_edge=edge_observations,
    )


def _initial_transition(snapshot: CertificateSnapshot) -> CertificateTransitionResult:
    result = build_or_rebuild_certificate(snapshot)
    assert result.kind is CertificateTransitionKind.BUILD
    assert result.artifact is not None
    assert result.open_binding is not None
    return result


def test_certificate_digest_matches_frozen_golden_vector() -> None:
    rows = (
        GroupMatchingCertificateRow(0, "req-0", "0" * 64, "obs-0"),
        GroupMatchingCertificateRow(1, "req-1", "f" * 64, "obs-1"),
    )

    digest, framed_bytes = compute_group_certificate_digest(
        decision_policy_version="policy-v1",
        group_version_id="group-v1",
        requirement_count=2,
        rows=rows,
    )

    assert digest == "8eb7faa3fb7d626bfe33c32365980c9d292ebae9529a5498674d57b775f57ac3"
    assert framed_bytes == 528


def test_certificate_construction_hashes_once_and_reports_exact_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text_hash = _hash("content")
    snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a",)},
        requirement_count=1,
        revision=1,
    )
    original = m5_domain.stable_m5_digest
    digest_input_sizes: list[int] = []

    def counted_digest(
        domain_tag: str,
        *values: tuple[str, ...],
    ) -> str:
        fields = (domain_tag, *(field for value in values for field in value))
        if domain_tag == "m5-group-certificate-v1":
            digest_input_sizes.append(
                sum(8 + len(field.encode("utf-8")) for field in fields)
            )
        return original(domain_tag, *values)

    monkeypatch.setattr(m5_domain, "stable_m5_digest", counted_digest)

    reconstructed = reconstruct_certificate(snapshot)

    assert reconstructed.artifact is not None
    assert digest_input_sizes == [reconstructed.work.certificate_digest_input_bytes]

    digest_input_sizes.clear()
    built = build_or_rebuild_certificate(snapshot)

    assert built.artifact is not None
    assert digest_input_sizes == [built.work.certificate_digest_input_bytes]


def test_reconstruction_is_deterministic_and_snapshot_valid() -> None:
    first_hash = _hash("first")
    second_hash = _hash("second")
    edge_observations = {
        (0, first_hash): ("obs-0-first",),
        (0, second_hash): ("obs-0-second",),
        (1, first_hash): ("obs-1-first",),
        (1, second_hash): ("obs-1-second",),
    }
    snapshot = _snapshot(
        edge_observations=edge_observations,
        requirement_count=2,
        revision=1,
    )

    first = reconstruct_certificate(snapshot)
    second = reconstruct_certificate(snapshot)

    assert first == second
    assert first.matching.complete
    assert first.artifact is not None
    assert validate_certificate_artifact(first.artifact, snapshot).valid
    assert tuple(row.requirement_ordinal for row in first.artifact.rows) == (0, 1)
    assert len({row.text_hash for row in first.artifact.rows}) == 2
    assert all(
        row.selected_observation_id
        == snapshot.observations_for(row.requirement_ordinal, row.text_hash)[0]
        for row in first.artifact.rows
    )


def test_reconstruction_reads_only_r_hashes_from_one_large_mask_bucket() -> None:
    requirement_count = 8
    hashes = tuple(_hash(f"content-{ordinal}") for ordinal in range(200))
    edge_observations = {
        (requirement_ordinal, text_hash): (f"obs-{requirement_ordinal}-{text_hash}",)
        for text_hash in hashes
        for requirement_ordinal in range(requirement_count)
    }
    snapshot = _snapshot(
        edge_observations=edge_observations,
        requirement_count=requirement_count,
        revision=1,
    )

    result = reconstruct_certificate(snapshot)

    assert result.artifact is not None
    assert result.matching.complete
    assert result.work.representative_hashes_read == requirement_count
    assert result.work.representative_observations_read == requirement_count
    assert result.work.certificate_reconstructions == 1


def test_hall_counterexample_cannot_emit_a_covering_certificate() -> None:
    hashes = tuple(_hash(label) for label in ("a", "b", "c", "d"))
    masks = (0b0111, 0b0111, 0b1000, 0b1000)
    edge_observations = {
        (ordinal, text_hash): (f"obs-{ordinal}-{text_hash}",)
        for text_hash, mask in zip(hashes, masks, strict=True)
        for ordinal in range(4)
        if mask & (1 << ordinal)
    }
    snapshot = _snapshot(
        edge_observations=edge_observations,
        requirement_count=4,
        revision=1,
    )

    result = reconstruct_certificate(snapshot)

    assert result.matching.matching_size == 3
    assert not result.matching.complete
    assert result.artifact is None
    assert result.work.certificate_reconstructions == 1


def test_selected_observation_two_to_one_repairs_without_hall_rebuild() -> None:
    text_hash = _hash("content")
    old_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a", "obs-b")},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    assert initial.artifact.rows[0].selected_observation_id == "obs-a"
    new_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-b",)},
        requirement_count=1,
        revision=2,
    )

    repaired = repair_selected_observations(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert repaired.kind is CertificateTransitionKind.REPAIR
    assert repaired.artifact is not None
    assert repaired.closed_binding is not None
    assert repaired.open_binding is not None
    assert repaired.artifact.rows[0].selected_observation_id == "obs-b"
    assert repaired.closed_binding.valid_to_revision == 2
    assert repaired.open_binding.valid_from_revision == 2
    assert repaired.work.certificate_repairs == 1
    assert repaired.work.certificate_reconstructions == 0
    assert repaired.work.hash_mask_transitions == 0
    assert repaired.work.group_local_state_operations == 1
    assert repaired.work.groups_touched == 0
    assert validate_bound_certificate(
        repaired.artifact,
        repaired.open_binding,
        new_snapshot,
    ).valid


def test_removing_nonselected_duplicate_retains_artifact_and_binding() -> None:
    text_hash = _hash("content")
    old_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a", "obs-b")},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a",)},
        requirement_count=1,
        revision=2,
    )

    retained = repair_selected_observations(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert retained.kind is CertificateTransitionKind.RETAIN
    assert retained.artifact is initial.artifact
    assert retained.open_binding is initial.open_binding
    assert retained.work.certificate_repairs == 0
    assert retained.work.groups_touched == 0


def test_retain_performs_no_digest_work_for_long_selected_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text_hash = _hash("content")
    selected_observation_id = "observation-" + "x" * 10_000
    old_snapshot = _snapshot(
        edge_observations={(0, text_hash): (selected_observation_id,)},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None

    def unexpected_digest(**_: object) -> tuple[str, int]:
        raise AssertionError("retain must trust the frozen artifact digest")

    monkeypatch.setattr(
        matching_module,
        "compute_group_certificate_digest",
        unexpected_digest,
    )
    new_snapshot = _snapshot(
        edge_observations={(0, text_hash): (selected_observation_id,)},
        requirement_count=1,
        revision=2,
    )

    retained = repair_selected_observations(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert retained.kind is CertificateTransitionKind.RETAIN
    assert retained.work.certificate_digest_input_bytes == 0


def test_selected_edge_loss_with_alternating_cover_rebuilds() -> None:
    hashes = (_hash("a"), _hash("b"))
    old_edges = {
        (ordinal, text_hash): (f"obs-{ordinal}-{text_hash}",)
        for ordinal in range(2)
        for text_hash in hashes
    }
    old_snapshot = _snapshot(
        edge_observations=old_edges,
        requirement_count=2,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    selected = initial.artifact.rows[0]
    new_edges = dict(old_edges)
    del new_edges[(selected.requirement_ordinal, selected.text_hash)]
    new_snapshot = _snapshot(
        edge_observations=new_edges,
        requirement_count=2,
        revision=2,
    )
    assert initialize_hall_mask_state(
        2,
        dict(new_snapshot.hash_masks),
    ).state.complete

    with pytest.raises(CertificateRebuildRequired):
        repair_selected_observations(
            new_snapshot,
            prior_artifact=initial.artifact,
            prior_binding=initial.open_binding,
        )
    rebuilt = build_or_rebuild_certificate(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert rebuilt.kind is CertificateTransitionKind.REBUILD
    assert rebuilt.artifact is not None
    assert rebuilt.open_binding is not None
    assert rebuilt.artifact.certificate_digest != initial.artifact.certificate_digest
    assert rebuilt.work.certificate_reconstructions == 1
    assert rebuilt.work.certificate_repairs == 0
    assert rebuilt.work.group_local_state_operations == 1
    assert rebuilt.work.groups_touched == 0
    assert rebuilt.work.claim_status_changes == 0
    assert rebuilt.work.answer_status_changes == 0
    assert validate_bound_certificate(
        rebuilt.artifact,
        rebuilt.open_binding,
        new_snapshot,
    ).valid


def test_incomplete_group_closes_binding_without_reconstruction() -> None:
    first_hash = _hash("first")
    second_hash = _hash("second")
    old_edges = {
        (0, first_hash): ("obs-first",),
        (1, second_hash): ("obs-second",),
    }
    old_snapshot = _snapshot(
        edge_observations=old_edges,
        requirement_count=2,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_edges = {(1, second_hash): ("obs-second",)}
    new_snapshot = _snapshot(
        edge_observations=new_edges,
        requirement_count=2,
        revision=2,
    )
    hall_state = initialize_hall_mask_state(
        2,
        dict(new_snapshot.hash_masks),
    ).state
    assert not hall_state.complete

    closed = close_incomplete_certificate(
        new_snapshot,
        hall_state=hall_state,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert closed.kind is CertificateTransitionKind.CLOSE
    assert closed.artifact is None
    assert closed.closed_binding is not None
    assert closed.open_binding is None
    assert closed.work.certificate_reconstructions == 0
    assert closed.work.group_local_state_operations == 1
    assert closed.work.groups_touched == 0


def test_policy_change_with_zero_flips_rebinds_and_charges_empty_probes() -> None:
    text_hash = _hash("content")
    edges = {(0, text_hash): ("obs-a",)}
    old_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=1,
        policy="policy-v1",
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=2,
        policy="policy-v2",
    )

    probe_work = policy_range_probe_work(
        changed_threshold_dimensions=2,
        candidate_observations=0,
    )
    rebound = rebind_certificate_policy(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )
    total_work = probe_work + rebound.work

    assert rebound.kind is CertificateTransitionKind.REBIND
    assert rebound.artifact is not None
    assert rebound.closed_binding is not None
    assert rebound.open_binding is not None
    assert rebound.artifact.rows == initial.artifact.rows
    assert rebound.artifact.decision_policy_version == "policy-v2"
    assert rebound.artifact.certificate_digest != initial.artifact.certificate_digest
    assert rebound.work.policy_rebindings == 1
    assert rebound.work.certificate_reconstructions == 0
    assert total_work.ordered_policy_range_probes == 2
    assert total_work.policy_candidate_observations == 0
    assert total_work.claim_status_changes == 0
    assert total_work.answer_status_changes == 0
    assert validate_bound_certificate(
        rebound.artifact,
        rebound.open_binding,
        new_snapshot,
    ).valid
    assert validate_bound_certificate(
        initial.artifact,
        rebound.closed_binding,
        old_snapshot,
    ).valid
    assert not validate_bound_certificate(
        initial.artifact,
        rebound.closed_binding,
        new_snapshot,
    ).valid


def test_policy_rebind_rebuilds_when_selected_edge_also_disappears() -> None:
    hashes = (_hash("a"), _hash("b"))
    old_edges = {
        (ordinal, text_hash): (f"obs-{ordinal}-{text_hash}",)
        for ordinal in range(2)
        for text_hash in hashes
    }
    old_snapshot = _snapshot(
        edge_observations=old_edges,
        requirement_count=2,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    selected = initial.artifact.rows[0]
    new_edges = dict(old_edges)
    del new_edges[(selected.requirement_ordinal, selected.text_hash)]
    new_snapshot = _snapshot(
        edge_observations=new_edges,
        requirement_count=2,
        revision=2,
        policy="policy-v2",
    )

    rebound = rebind_certificate_policy(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert rebound.kind is CertificateTransitionKind.REBIND_REBUILD
    assert rebound.artifact is not None
    assert rebound.work.policy_rebindings == 1
    assert rebound.work.certificate_reconstructions == 1
    assert rebound.open_binding is not None
    assert validate_bound_certificate(
        rebound.artifact,
        rebound.open_binding,
        new_snapshot,
    ).valid


def test_policy_rebind_locally_repairs_provenance_when_edge_survives() -> None:
    text_hash = _hash("content")
    old_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a", "obs-b")},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-b",)},
        requirement_count=1,
        revision=2,
        policy="policy-v2",
    )

    rebound = rebind_certificate_policy(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert rebound.kind is CertificateTransitionKind.REBIND_REPAIR
    assert rebound.artifact is not None
    assert rebound.artifact.rows[0].selected_observation_id == "obs-b"
    assert rebound.work.policy_rebindings == 1
    assert rebound.work.certificate_repairs == 1
    assert rebound.work.certificate_reconstructions == 0
    assert rebound.open_binding is not None
    assert validate_bound_certificate(
        rebound.artifact,
        rebound.open_binding,
        new_snapshot,
    ).valid


def test_new_epoch_carry_forward_retains_artifact_without_closing_history() -> None:
    text_hash = _hash("content")
    edges = {(0, text_hash): ("obs-a",)}
    old_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=1,
        epoch_id=11,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=0,
        epoch_id=12,
    )

    carried = carry_forward_certificate_epoch(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert carried.kind is CertificateTransitionKind.EPOCH_RETAIN
    assert carried.artifact is initial.artifact
    assert carried.closed_binding is None
    assert carried.open_binding is not None
    assert carried.open_binding.epoch_id == 12
    assert carried.open_binding.valid_from_revision == 0
    assert initial.open_binding.open


def test_new_epoch_carry_forward_rejects_closed_historical_binding() -> None:
    text_hash = _hash("content")
    edges = {(0, text_hash): ("obs-a",)}
    old_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=1,
        epoch_id=11,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    closed = replace(initial.open_binding, valid_to_revision=2)
    new_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=0,
        epoch_id=12,
    )

    with pytest.raises(ValidationError, match="binding must be open"):
        carry_forward_certificate_epoch(
            new_snapshot,
            prior_artifact=initial.artifact,
            prior_binding=closed,
        )


def test_new_epoch_carry_forward_repairs_selected_observation() -> None:
    text_hash = _hash("content")
    old_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a", "obs-b")},
        requirement_count=1,
        revision=2,
        epoch_id=11,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-b",)},
        requirement_count=1,
        revision=0,
        epoch_id=12,
    )

    carried = carry_forward_certificate_epoch(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert carried.kind is CertificateTransitionKind.EPOCH_REPAIR
    assert carried.artifact is not None
    assert carried.artifact.rows[0].selected_observation_id == "obs-b"
    assert carried.closed_binding is None
    assert carried.work.certificate_repairs == 1
    assert carried.work.certificate_reconstructions == 0


def test_new_epoch_carry_forward_rebuilds_alternating_cover() -> None:
    hashes = (_hash("a"), _hash("b"))
    old_edges = {
        (ordinal, text_hash): (f"obs-{ordinal}-{text_hash}",)
        for ordinal in range(2)
        for text_hash in hashes
    }
    old_snapshot = _snapshot(
        edge_observations=old_edges,
        requirement_count=2,
        revision=2,
        epoch_id=11,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    selected = initial.artifact.rows[0]
    new_edges = dict(old_edges)
    del new_edges[(selected.requirement_ordinal, selected.text_hash)]
    new_snapshot = _snapshot(
        edge_observations=new_edges,
        requirement_count=2,
        revision=0,
        epoch_id=12,
    )

    carried = carry_forward_certificate_epoch(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert carried.kind is CertificateTransitionKind.EPOCH_REBUILD
    assert carried.artifact is not None
    assert carried.open_binding is not None
    assert carried.closed_binding is None
    assert carried.work.certificate_reconstructions == 1
    assert validate_bound_certificate(
        carried.artifact,
        carried.open_binding,
        new_snapshot,
    ).valid


def test_new_epoch_carry_forward_records_completeness_loss_without_close() -> None:
    first_hash = _hash("first")
    second_hash = _hash("second")
    old_snapshot = _snapshot(
        edge_observations={
            (0, first_hash): ("obs-first",),
            (1, second_hash): ("obs-second",),
        },
        requirement_count=2,
        revision=2,
        epoch_id=11,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations={(1, second_hash): ("obs-second",)},
        requirement_count=2,
        revision=0,
        epoch_id=12,
    )

    carried = carry_forward_certificate_epoch(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert carried.kind is CertificateTransitionKind.EPOCH_INCOMPLETE
    assert carried.artifact is None
    assert carried.closed_binding is None
    assert carried.open_binding is None
    assert carried.work.certificate_reconstructions == 1
    assert initial.open_binding.open


def test_new_epoch_zero_flip_policy_change_rebinds_at_revision_zero() -> None:
    text_hash = _hash("content")
    edges = {(0, text_hash): ("obs-a",)}
    old_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=2,
        policy="policy-v1",
        epoch_id=11,
    )
    initial = _initial_transition(old_snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    new_snapshot = _snapshot(
        edge_observations=edges,
        requirement_count=1,
        revision=0,
        policy="policy-v2",
        epoch_id=12,
    )

    carried = carry_forward_certificate_epoch(
        new_snapshot,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )

    assert carried.kind is CertificateTransitionKind.EPOCH_REBIND
    assert carried.artifact is not None
    assert carried.artifact.decision_policy_version == "policy-v2"
    assert carried.artifact.rows == initial.artifact.rows
    assert carried.closed_binding is None
    assert carried.open_binding is not None
    assert carried.open_binding.valid_from_revision == 0
    assert carried.work.policy_rebindings == 1
    assert carried.work.certificate_reconstructions == 0


def test_multiple_transitions_in_one_epoch_preserve_exact_binding_history() -> None:
    hashes = (_hash("a"), _hash("b"))
    initial_edges = {
        (ordinal, text_hash): (
            f"obs-a-{ordinal}-{text_hash}",
            f"obs-b-{ordinal}-{text_hash}",
        )
        for ordinal in range(2)
        for text_hash in hashes
    }
    revision_one = _snapshot(
        edge_observations=initial_edges,
        requirement_count=2,
        revision=1,
    )
    initial = _initial_transition(revision_one)
    assert initial.artifact is not None
    assert initial.open_binding is not None

    selected_observation = initial.artifact.rows[0]
    revision_two_edges = dict(initial_edges)
    revision_two_edges[
        (
            selected_observation.requirement_ordinal,
            selected_observation.text_hash,
        )
    ] = (
        f"obs-b-{selected_observation.requirement_ordinal}"
        f"-{selected_observation.text_hash}",
    )
    revision_two = _snapshot(
        edge_observations=revision_two_edges,
        requirement_count=2,
        revision=2,
    )
    repaired = repair_selected_observations(
        revision_two,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )
    assert repaired.artifact is not None
    assert repaired.closed_binding is not None
    assert repaired.open_binding is not None

    selected_edge = repaired.artifact.rows[0]
    revision_three_edges = dict(revision_two_edges)
    del revision_three_edges[
        (selected_edge.requirement_ordinal, selected_edge.text_hash)
    ]
    revision_three = _snapshot(
        edge_observations=revision_three_edges,
        requirement_count=2,
        revision=3,
    )
    rebuilt = build_or_rebuild_certificate(
        revision_three,
        prior_artifact=repaired.artifact,
        prior_binding=repaired.open_binding,
    )
    assert rebuilt.kind is CertificateTransitionKind.REBUILD
    assert rebuilt.artifact is not None
    assert rebuilt.closed_binding is not None
    assert rebuilt.open_binding is not None

    revision_four_edges = {
        edge: observations
        for edge, observations in revision_three_edges.items()
        if edge[0] != 0
    }
    revision_four = _snapshot(
        edge_observations=revision_four_edges,
        requirement_count=2,
        revision=4,
    )
    hall = initialize_hall_mask_state(
        2,
        dict(revision_four.hash_masks),
    ).state
    closed = close_incomplete_certificate(
        revision_four,
        hall_state=hall,
        prior_artifact=rebuilt.artifact,
        prior_binding=rebuilt.open_binding,
    )
    assert closed.closed_binding is not None

    assert (
        repaired.closed_binding.valid_from_revision,
        repaired.closed_binding.valid_to_revision,
    ) == (1, 2)
    assert (
        rebuilt.closed_binding.valid_from_revision,
        rebuilt.closed_binding.valid_to_revision,
    ) == (2, 3)
    assert (
        closed.closed_binding.valid_from_revision,
        closed.closed_binding.valid_to_revision,
    ) == (3, 4)
    assert validate_bound_certificate(
        initial.artifact,
        repaired.closed_binding,
        revision_one,
    ).valid
    assert validate_bound_certificate(
        repaired.artifact,
        rebuilt.closed_binding,
        revision_two,
    ).valid
    assert validate_bound_certificate(
        rebuilt.artifact,
        closed.closed_binding,
        revision_three,
    ).valid


def test_validator_detects_digest_and_binding_snapshot_corruption() -> None:
    text_hash = _hash("content")
    snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a",)},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    with pytest.raises(ValidationError, match="digest does not match"):
        replace(initial.artifact, certificate_digest="0" * 64)
    future = replace(snapshot, revision=2)
    closed = replace(initial.open_binding, valid_to_revision=2)
    assert not validate_bound_certificate(
        initial.artifact,
        closed,
        future,
    ).valid


def test_public_validator_detects_hostile_in_process_digest_corruption() -> None:
    text_hash = _hash("content")
    snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a",)},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(snapshot)
    assert initial.artifact is not None
    object.__setattr__(initial.artifact, "certificate_digest", "0" * 64)

    validation = validate_certificate_artifact(initial.artifact, snapshot)

    assert not validation.valid
    assert "certificate_digest_mismatch" in validation.issues


def test_artifacts_rows_and_bindings_are_immutable() -> None:
    text_hash = _hash("content")
    snapshot = _snapshot(
        edge_observations={(0, text_hash): ("obs-a",)},
        requirement_count=1,
        revision=1,
    )
    initial = _initial_transition(snapshot)
    assert initial.artifact is not None
    assert initial.open_binding is not None

    with pytest.raises(FrozenInstanceError):
        initial.artifact.decision_policy_version = "mutated"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        initial.artifact.rows[0].selected_observation_id = "mutated"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        initial.open_binding.valid_to_revision = 2  # type: ignore[misc]


def test_policy_probe_counter_validates_frozen_range() -> None:
    with pytest.raises(ValidationError, match="at most 2"):
        policy_range_probe_work(
            changed_threshold_dimensions=3,
            candidate_observations=0,
        )
