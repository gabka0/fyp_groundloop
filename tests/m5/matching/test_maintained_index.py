from __future__ import annotations

import hashlib
from copy import deepcopy

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.domain import (
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
    GroupMatchingCertificateArtifact,
    RequirementWitness,
    SnapshotPoint,
)
from groundloop.m5.matching import (
    CertificateTransitionKind,
    MaintainedCertificateIndex,
    ObservationMembershipDelta,
    PersistentStringSet,
    apply_hash_mask_transitions,
    build_or_rebuild_certificate,
    initialize_hall_mask_state,
    reconstruct_certificate,
    repair_selected_observations,
    validate_certificate_artifact,
)


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _group(requirement_count: int) -> EvidenceGroupVersion:
    group_id = "group-v1"
    requirements = tuple(
        EvidenceRequirementVersion(
            requirement_version_id=f"requirement-{ordinal}",
            group_version_id=group_id,
            ordinal=ordinal,
            requirement_text=f"requirement text {ordinal}",
        )
        for ordinal in range(requirement_count)
    )
    return EvidenceGroupVersion(
        group_version_id=group_id,
        group_family_id="family-1",
        owner_claim_id="claim-1",
        requirements=requirements,
        construction_source_id="matching-test-fixture",
    )


def test_checked_requirement_witness_adapter_builds_shared_certificate() -> None:
    group = _group(2)
    first_hash = _hash("first")
    second_hash = _hash("second")
    witnesses = (
        RequirementWitness(
            "requirement-1",
            1,
            second_hash,
            ("obs-1",),
        ),
        RequirementWitness(
            "requirement-0",
            0,
            first_hash,
            ("obs-0",),
        ),
    )

    built = MaintainedCertificateIndex.from_requirement_witnesses(
        group,
        witnesses,
    )
    assert not built.index.audit_issues()
    assert built.work.hash_masks_initialized == 2
    view = built.index.current_view(
        point=SnapshotPoint(1, 0),
        decision_policy_version="policy-v1",
    )
    reconstruction = reconstruct_certificate(view)

    assert isinstance(reconstruction.artifact, GroupMatchingCertificateArtifact)
    assert reconstruction.artifact is not None
    assert validate_certificate_artifact(reconstruction.artifact, view).valid
    audit_snapshot = built.index.audit_snapshot(
        point=SnapshotPoint(1, 0),
        decision_policy_version="policy-v1",
    )
    assert reconstruct_certificate(audit_snapshot).artifact == reconstruction.artifact


def test_requirement_witness_adapter_rejects_invalid_inputs() -> None:
    group = _group(2)
    text_hash = _hash("content")
    with pytest.raises(ValidationError, match="does not match"):
        MaintainedCertificateIndex.from_requirement_witnesses(
            group,
            (
                RequirementWitness(
                    "requirement-1",
                    0,
                    text_hash,
                    ("obs-0",),
                ),
            ),
        )
    with pytest.raises(ValidationError, match="at most 1"):
        MaintainedCertificateIndex.from_requirement_witnesses(
            group,
            (
                RequirementWitness(
                    "outside-requirement",
                    2,
                    text_hash,
                    ("obs-0",),
                ),
            ),
        )
    with pytest.raises(ValidationError, match="duplicate RequirementWitness"):
        MaintainedCertificateIndex.from_requirement_witnesses(
            group,
            (
                RequirementWitness(
                    "requirement-0",
                    0,
                    text_hash,
                    ("obs-a",),
                ),
                RequirementWitness(
                    "requirement-0",
                    0,
                    text_hash,
                    ("obs-b",),
                ),
            ),
        )


def test_high_multiplicity_update_repairs_without_full_image_or_hall_work() -> None:
    group = _group(1)
    text_hash = _hash("high-degree")
    observation_ids = tuple(f"obs-{ordinal:05d}" for ordinal in range(4_096))
    built = MaintainedCertificateIndex.from_requirement_witnesses(
        group,
        (
            RequirementWitness(
                "requirement-0",
                0,
                text_hash,
                observation_ids,
            ),
        ),
    )
    old_view = built.index.current_view(
        point=SnapshotPoint(10, 1),
        decision_policy_version="policy-v1",
    )
    initial = build_or_rebuild_certificate(old_view)
    assert initial.artifact is not None
    assert initial.open_binding is not None
    assert initial.artifact.rows[0].selected_observation_id == observation_ids[0]

    updated = built.index.apply_observation_deltas(
        (
            ObservationMembershipDelta(0, text_hash, observation_ids[0], -1),
            ObservationMembershipDelta(0, text_hash, "obs-new-last", 1),
        )
    )

    assert not updated.transitions
    assert updated.work.ordered_index_operations == 2
    assert updated.work.edge_refcount_keys_updated == 1
    assert updated.work.hash_mask_transitions == 0
    assert not built.index.audit_issues()
    with pytest.raises(ValidationError, match="stale"):
        old_view.least_observation_id(0, text_hash)

    new_view = built.index.current_view(
        point=SnapshotPoint(10, 2),
        decision_policy_version="policy-v1",
    )
    repaired = repair_selected_observations(
        new_view,
        prior_artifact=initial.artifact,
        prior_binding=initial.open_binding,
    )
    assert repaired.kind is CertificateTransitionKind.REPAIR
    assert repaired.artifact is not None
    assert repaired.artifact.rows[0].selected_observation_id == observation_ids[1]
    assert repaired.work.certificate_reconstructions == 0
    assert repaired.work.ordered_index_operations == 1


def test_maintained_index_coalesces_edge_crossings_into_hash_masks() -> None:
    group = _group(2)
    index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=tuple(
            requirement.requirement_version_id for requirement in group.requirements
        ),
    )
    shared_hash = _hash("shared")
    second_hash = _hash("second")
    first = index.apply_observation_deltas(
        (
            ObservationMembershipDelta(0, shared_hash, "obs-0", 1),
            ObservationMembershipDelta(1, shared_hash, "obs-1", 1),
        )
    )
    assert len(first.transitions) == 1
    assert first.transitions[0].old_mask == 0
    assert first.transitions[0].new_mask == 0b11
    hall = initialize_hall_mask_state(2, {}).state
    hall = apply_hash_mask_transitions(hall, first.transitions).state
    assert hall.matching_size == 1

    second = index.apply_observation_deltas(
        (ObservationMembershipDelta(1, second_hash, "obs-2", 1),)
    )
    hall = apply_hash_mask_transitions(hall, second.transitions).state
    assert hall.complete
    assert not index.audit_issues()


def test_maintained_index_rejection_is_failure_atomic() -> None:
    group = _group(1)
    index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=("requirement-0",),
    )
    text_hash = _hash("content")
    generation = index.generation

    with pytest.raises(ValidationError, match="not active"):
        index.apply_observation_deltas(
            (
                ObservationMembershipDelta(0, text_hash, "obs-valid", 1),
                ObservationMembershipDelta(0, text_hash, "obs-missing", -1),
            )
        )

    assert index.generation == generation
    assert not index.audit_issues()
    view = index.current_view(
        point=SnapshotPoint(1, 0),
        decision_policy_version="policy-v1",
    )
    assert view.least_observation_id(0, text_hash) is None


def test_public_persistent_string_set_is_ordered_balanced_and_path_copied() -> None:
    empty = PersistentStringSet()
    populated = empty
    for ordinal in range(2_048):
        populated, changed = populated.add(f"key-{ordinal:05d}")
        assert changed

    assert len(empty) == 0
    assert populated.least() == "key-00000"
    assert populated.first(3) == ("key-00000", "key-00001", "key-00002")
    assert populated.items() == tuple(f"key-{ordinal:05d}" for ordinal in range(2_048))
    assert not populated.audit_issues()
    unchanged, changed = populated.add("key-00000")
    assert unchanged is not populated
    assert unchanged.root is populated.root
    assert not changed
    reduced, changed = populated.remove("key-01024")
    assert changed
    assert populated.contains("key-01024")
    assert not reduced.contains("key-01024")
    assert len(reduced) == len(populated) - 1
    assert not reduced.audit_issues()


def test_maintained_index_deepcopy_is_independent_and_rejection_atomic() -> None:
    group = _group(1)
    text_hash = _hash("content")
    built = MaintainedCertificateIndex.from_requirement_witnesses(
        group,
        (
            RequirementWitness(
                "requirement-0",
                0,
                text_hash,
                ("obs-original",),
            ),
        ),
    )
    original = built.index
    original_view = original.current_view(
        point=SnapshotPoint(1, 0),
        decision_policy_version="policy-v1",
    )
    cloned = deepcopy(original)

    cloned.apply_observation_deltas(
        (ObservationMembershipDelta(0, text_hash, "obs-clone-only", 1),)
    )
    assert original_view.least_observation_id(0, text_hash) == "obs-original"
    assert not original_view.observation_active(0, text_hash, "obs-clone-only")
    assert cloned.current_view(
        point=SnapshotPoint(1, 1),
        decision_policy_version="policy-v1",
    ).observation_active(0, text_hash, "obs-clone-only")

    before_rejection = cloned.audit_snapshot(
        point=SnapshotPoint(1, 1),
        decision_policy_version="policy-v1",
    )
    generation = cloned.generation
    with pytest.raises(ValidationError, match="not active"):
        cloned.apply_observation_deltas(
            (
                ObservationMembershipDelta(0, text_hash, "obs-never-committed", 1),
                ObservationMembershipDelta(0, text_hash, "obs-missing", -1),
            )
        )
    after_rejection = cloned.audit_snapshot(
        point=SnapshotPoint(1, 1),
        decision_policy_version="policy-v1",
    )
    assert cloned.generation == generation
    assert after_rejection == before_rejection
    assert not original.audit_issues()
    assert not cloned.audit_issues()
