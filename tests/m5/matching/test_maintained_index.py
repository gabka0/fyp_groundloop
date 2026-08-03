from __future__ import annotations

import ast
import hashlib
import inspect
import random
import textwrap
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


def test_prepared_patch_previews_applies_and_rolls_back_exactly() -> None:
    group = _group(2)
    first_hash = _hash("prepared-first")
    second_hash = _hash("prepared-second")
    deltas = (
        ObservationMembershipDelta(0, first_hash, "obs-first", 1),
        ObservationMembershipDelta(1, second_hash, "obs-second", 1),
    )
    index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=("requirement-0", "requirement-1"),
    )
    baseline = deepcopy(index)
    point = SnapshotPoint(20, 3)
    before = index.audit_snapshot(
        point=point,
        decision_policy_version="policy-v1",
    )
    generation = index.generation

    patch = index.prepare_observation_deltas(deltas)
    preview = patch.preview_view(
        point=point,
        decision_policy_version="policy-v1",
    )
    preview_certificate = reconstruct_certificate(preview)

    assert preview_certificate.artifact is not None
    assert preview_certificate.matching.complete
    assert preview.hall_histogram() == (0, 1, 1, 0)
    assert index.generation == generation
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == before
    )

    expected_update = baseline.apply_observation_deltas(deltas)
    token = index.apply_prepared_observation_deltas(patch)
    assert (patch.transitions, patch.work) == (
        expected_update.transitions,
        expected_update.work,
    )
    assert index.audit_snapshot(
        point=point,
        decision_policy_version="policy-v1",
    ) == baseline.audit_snapshot(
        point=point,
        decision_policy_version="policy-v1",
    )
    with pytest.raises(ValidationError, match="stale|already"):
        index.apply_prepared_observation_deltas(patch)
    with pytest.raises(ValidationError, match="stale"):
        preview.hall_histogram()

    index.rollback_prepared_observation_deltas(token)
    assert index.generation == generation
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == before
    )
    with pytest.raises(ValidationError, match="consumed"):
        index.rollback_prepared_observation_deltas(token)


def test_prepared_patch_rejects_stale_and_underflow_without_mutation() -> None:
    group = _group(1)
    text_hash = _hash("prepared-content")
    index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=("requirement-0",),
    )
    point = SnapshotPoint(21, 0)
    before = index.audit_snapshot(
        point=point,
        decision_policy_version="policy-v1",
    )
    generation = index.generation
    first = index.prepare_observation_deltas(
        (ObservationMembershipDelta(0, text_hash, "obs-first", 1),)
    )
    stale = index.prepare_observation_deltas(
        (ObservationMembershipDelta(0, text_hash, "obs-stale", 1),)
    )
    token = index.apply_prepared_observation_deltas(first)
    applied = index.audit_snapshot(
        point=point,
        decision_policy_version="policy-v1",
    )
    with pytest.raises(ValidationError, match="stale"):
        index.apply_prepared_observation_deltas(stale)
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == applied
    )
    index.rollback_prepared_observation_deltas(token)
    assert index.generation == generation
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == before
    )

    with pytest.raises(ValidationError, match="not active"):
        index.prepare_observation_deltas(
            (ObservationMembershipDelta(0, text_hash, "obs-missing", -1),)
        )
    assert index.generation == generation
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == before
    )


def test_prepared_noop_patch_preserves_generation_and_is_single_use() -> None:
    group = _group(1)
    text_hash = _hash("prepared-noop")
    index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=("requirement-0",),
    )
    point = SnapshotPoint(22, 0)
    before = index.audit_snapshot(
        point=point,
        decision_policy_version="policy-v1",
    )
    generation = index.generation
    deltas = (
        ObservationMembershipDelta(0, text_hash, "obs-net-zero", 1),
        ObservationMembershipDelta(0, text_hash, "obs-net-zero", -1),
    )

    patch = index.prepare_observation_deltas(deltas)
    preview = patch.preview_view(
        point=point,
        decision_policy_version="policy-v1",
    )

    assert not patch.mutates
    assert patch.transitions == ()
    assert patch.work.contribution_additions == 1
    assert patch.work.contribution_removals == 1
    assert preview.hall_histogram() == (0, 0)

    token = index.apply_prepared_observation_deltas(patch)
    assert index.generation == generation
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == before
    )
    with pytest.raises(ValidationError, match="already"):
        index.apply_prepared_observation_deltas(patch)
    with pytest.raises(ValidationError, match="stale"):
        preview.hall_histogram()

    index.rollback_prepared_observation_deltas(token)
    assert index.generation == generation
    assert (
        index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        == before
    )


def test_prepared_patch_randomized_twin_matches_compatibility_path() -> None:
    rng = random.Random(0x5A4E)
    group = _group(4)
    requirement_ids = tuple(
        requirement.requirement_version_id for requirement in group.requirements
    )
    prepared_index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=requirement_ids,
    )
    compatibility_index = MaintainedCertificateIndex(
        group_version_id=group.group_version_id,
        requirement_version_ids=requirement_ids,
    )
    hashes = tuple(_hash(f"randomized-hash-{ordinal}") for ordinal in range(6))
    observation_ids = tuple(f"randomized-obs-{ordinal:02d}" for ordinal in range(48))
    active: dict[str, tuple[int, str]] = {}
    saw_net_zero_batch = False
    saw_edge_multiplicity = False

    for step in range(96):
        deltas: list[ObservationMembershipDelta] = []
        selected: set[str] = set()
        if step == 0:
            initial_edges = (
                (0, hashes[0]),
                (0, hashes[0]),
                (1, hashes[1]),
                (2, hashes[2]),
                (3, hashes[3]),
                (0, hashes[4]),
                (1, hashes[5]),
            )
            for observation_id, edge in zip(
                observation_ids[: len(initial_edges)],
                initial_edges,
                strict=True,
            ):
                selected.add(observation_id)
                active[observation_id] = edge
                deltas.append(ObservationMembershipDelta(*edge, observation_id, 1))
        else:
            mutation_count = 0 if step % 11 == 0 else rng.randint(1, 5)
            for observation_id in rng.sample(observation_ids, mutation_count):
                selected.add(observation_id)
                if observation_id in active:
                    current_edge = active.pop(observation_id)
                    delta = -1
                else:
                    current_edge = (rng.randrange(4), rng.choice(hashes))
                    active[observation_id] = current_edge
                    delta = 1
                deltas.append(
                    ObservationMembershipDelta(*current_edge, observation_id, delta)
                )

        if step % 3 == 0:
            net_zero_candidates = tuple(
                observation_id
                for observation_id in observation_ids
                if observation_id not in selected
            )
            observation_id = rng.choice(net_zero_candidates)
            net_zero_edge = active.get(observation_id)
            if net_zero_edge is None:
                net_zero_edge = (rng.randrange(4), rng.choice(hashes))
                first_delta = 1
            else:
                first_delta = -1
            deltas.extend(
                (
                    ObservationMembershipDelta(
                        *net_zero_edge,
                        observation_id,
                        first_delta,
                    ),
                    ObservationMembershipDelta(
                        *net_zero_edge,
                        observation_id,
                        -first_delta,
                    ),
                )
            )
            saw_net_zero_batch = True

        edge_counts: dict[tuple[int, str], int] = {}
        for edge in active.values():
            edge_counts[edge] = edge_counts.get(edge, 0) + 1
        saw_edge_multiplicity |= any(count > 1 for count in edge_counts.values())

        point = SnapshotPoint(30, step)
        before_generation = prepared_index.generation
        before_snapshot = prepared_index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        patch = prepared_index.prepare_observation_deltas(deltas)
        preview = patch.preview_view(
            point=point,
            decision_policy_version="policy-v1",
        )
        compatibility_update = compatibility_index.apply_observation_deltas(deltas)
        compatibility_view = compatibility_index.current_view(
            point=point,
            decision_policy_version="policy-v1",
        )

        assert (patch.transitions, patch.work) == (
            compatibility_update.transitions,
            compatibility_update.work,
        )
        assert preview.hall_histogram() == compatibility_view.hall_histogram()
        assert (
            preview.representative_hash_masks()
            == compatibility_view.representative_hash_masks()
        )
        assert reconstruct_certificate(preview) == reconstruct_certificate(
            compatibility_view
        )
        for observation_id, edge in active.items():
            assert preview.observation_active(*edge, observation_id)

        token = prepared_index.apply_prepared_observation_deltas(patch)
        assert prepared_index.generation == before_generation + int(patch.mutates)
        assert prepared_index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        ) == compatibility_index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )

        prepared_index.rollback_prepared_observation_deltas(token)
        assert prepared_index.generation == before_generation
        assert (
            prepared_index.audit_snapshot(
                point=point,
                decision_policy_version="policy-v1",
            )
            == before_snapshot
        )

        replacement = prepared_index.prepare_observation_deltas(deltas)
        replacement_preview = replacement.preview_view(
            point=point,
            decision_policy_version="policy-v1",
        )
        assert reconstruct_certificate(replacement_preview) == reconstruct_certificate(
            compatibility_view
        )
        prepared_index.apply_prepared_observation_deltas(replacement)
        assert prepared_index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        ) == compatibility_index.audit_snapshot(
            point=point,
            decision_policy_version="policy-v1",
        )
        assert not prepared_index.audit_issues()

    assert saw_net_zero_batch
    assert saw_edge_multiplicity


def test_prepare_path_has_no_deepcopy_or_full_dictionary_copy() -> None:
    tree = ast.parse(
        textwrap.dedent(
            inspect.getsource(MaintainedCertificateIndex.prepare_observation_deltas)
        )
    )
    calls = tuple(node for node in ast.walk(tree) if isinstance(node, ast.Call))

    assert not any(
        isinstance(call.func, ast.Name)
        and call.func.id in {"deepcopy", "dict", "sorted"}
        for call in calls
    )
    assert not any(
        isinstance(call.func, ast.Attribute) and call.func.attr == "copy"
        for call in calls
    )
