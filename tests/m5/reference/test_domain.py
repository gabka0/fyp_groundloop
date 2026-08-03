from __future__ import annotations

from dataclasses import replace

import pytest

from groundloop.errors import ValidationError
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    ConstructionKind,
    EvidenceGroupType,
    EvidenceGroupVersion,
    EvidenceRequirementVersion,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
    SnapshotPoint,
)

from .conftest import make_group, sha


@pytest.mark.parametrize(
    "epoch,revision",
    ((True, 0), (0, False), (0.5, 0), (0, 0.5), (-1, 0), (0, -1)),
)
def test_snapshot_point_requires_nonnegative_integers(
    epoch: object, revision: object
) -> None:
    with pytest.raises(ValidationError, match="nonnegative integer"):
        SnapshotPoint(epoch, revision)  # type: ignore[arg-type]


def _requirement(
    requirement_id: str,
    group_id: str,
    ordinal: int,
    text: str,
) -> EvidenceRequirementVersion:
    return EvidenceRequirementVersion(
        requirement_version_id=requirement_id,
        group_version_id=group_id,
        ordinal=ordinal,
        requirement_text=text,
    )


def test_group_cardinality_dense_ordinals_and_unique_normalized_text() -> None:
    assert len(make_group(texts=("one",)).requirements) == 1
    assert len(make_group(texts=tuple(f"r{i}" for i in range(8))).requirements) == 8

    with pytest.raises(ValidationError, match="one to eight"):
        EvidenceGroupVersion(
            group_version_id="g0",
            group_family_id="f0",
            owner_claim_id="c1",
            requirements=(),
            construction_source_id="source",
        )
    with pytest.raises(ValidationError, match="one to eight"):
        make_group(texts=tuple(f"r{i}" for i in range(9)))
    group = make_group(texts=("one",))
    with pytest.raises(ValidationError, match="immutable tuple"):
        replace(
            group,
            requirements=list(group.requirements),  # type: ignore[arg-type]
            semantic_structure_hash="",
            record_payload_hash="",
        )
    with pytest.raises(ValidationError, match="nonnegative"):
        _requirement("r", "g", -1, "text")

    for ordinals in ((1, 2), (0, 2)):
        requirements = tuple(
            _requirement(f"r{index}", "g", ordinal, f"text {index}")
            for index, ordinal in enumerate(ordinals)
        )
        with pytest.raises(ValidationError, match="dense and ordered"):
            EvidenceGroupVersion(
                group_version_id="g",
                group_family_id="f",
                owner_claim_id="c1",
                requirements=requirements,
                construction_source_id="source",
            )

    duplicate_texts = (
        _requirement("r0", "g", 0, " alpha\t beta "),
        _requirement("r1", "g", 1, "alpha beta"),
    )
    with pytest.raises(ValidationError, match="texts must be unique"):
        EvidenceGroupVersion(
            group_version_id="g",
            group_family_id="f",
            owner_claim_id="c1",
            requirements=duplicate_texts,
            construction_source_id="source",
        )


def test_requirement_normalizes_text_and_validates_hash_and_containment() -> None:
    requirement = _requirement("r0", "g", 0, "\u00a0alpha\t\tbeta\u3000")
    assert requirement.requirement_text == "alpha beta"
    assert requirement.requirement_text_hash == sha("alpha beta")
    with pytest.raises(ValidationError, match="does not match"):
        replace(requirement, requirement_text_hash="0" * 64)
    with pytest.raises(ValidationError, match="must be nonempty"):
        _requirement("empty", "g", 0, "\t\u3000\n")
    with pytest.raises(ValidationError, match="containing group"):
        EvidenceGroupVersion(
            group_version_id="different",
            group_family_id="f",
            owner_claim_id="c1",
            requirements=(requirement,),
            construction_source_id="source",
        )


def test_semantic_hash_is_reorder_and_constructor_invariant_but_record_is_not() -> None:
    first = make_group(
        group_id="g1",
        family_id="f1",
        texts=("alpha", "beta"),
        requirement_ids=("r-a", "r-b"),
        source_id="source-a",
    )
    reordered = make_group(
        group_id="g2",
        family_id="f2",
        texts=("beta", "alpha"),
        requirement_ids=("r-b2", "r-a2"),
        source_id="source-b",
    )
    assert first.semantic_structure_hash == reordered.semantic_structure_hash
    assert first.record_payload_hash != reordered.record_payload_hash
    assert first.group_type is EvidenceGroupType.SUPPORT_CONJUNCTION
    with pytest.raises(ValidationError, match="semantic_structure_hash"):
        replace(first, semantic_structure_hash="0" * 64, record_payload_hash="")
    with pytest.raises(ValidationError, match="record_payload_hash"):
        replace(first, record_payload_hash="0" * 64)


def test_constructor_provenance_is_total_and_children_equal_parent() -> None:
    for kind in (ConstructionKind.GOLD, ConstructionKind.CONTROLLED):
        with pytest.raises(ValidationError, match="cannot name"):
            make_group(
                construction_kind=kind,
                model_triple=("model", "v1", "p1"),
            )

    with pytest.raises(ValidationError, match="require a complete"):
        make_group(construction_kind=ConstructionKind.MODEL_PROPOSED)

    proposed = make_group(
        construction_kind=ConstructionKind.MODEL_PROPOSED,
        model_triple=("model", "v1", "p1"),
    )
    assert proposed.requirements[0].constructor_model_id == "model"
    mismatched_child = replace(
        proposed.requirements[0],
        constructor_model_id="another",
        requirement_text_hash="",
    )
    with pytest.raises(ValidationError, match="must equal"):
        replace(
            proposed,
            requirements=(mismatched_child, *proposed.requirements[1:]),
            semantic_structure_hash="",
            record_payload_hash="",
        )
    with pytest.raises(ValidationError, match="construction_kind"):
        replace(
            make_group(),
            construction_kind="invented",  # type: ignore[arg-type]
            record_payload_hash="",
        )
    with pytest.raises(ValidationError, match="group_type"):
        replace(
            make_group(),
            group_type="invented",  # type: ignore[arg-type]
            record_payload_hash="",
        )


def test_self_predecessors_and_partial_model_triples_are_rejected() -> None:
    with pytest.raises(ValidationError, match="cannot supersede itself"):
        EvidenceRequirementVersion(
            requirement_version_id="r1",
            group_version_id="g1",
            ordinal=0,
            requirement_text="text",
            supersedes_requirement_version_id="r1",
        )
    with pytest.raises(ValidationError, match="cannot supersede itself"):
        replace(
            make_group(),
            supersedes_group_version_id="g1",
            record_payload_hash="",
        )
    with pytest.raises(ValidationError, match="partial"):
        EvidenceRequirementVersion(
            requirement_version_id="r1",
            group_version_id="g1",
            ordinal=0,
            requirement_text="text",
            constructor_model_id="model",
        )


def _group_certificate() -> GroupMatchingCertificateArtifact:
    return GroupMatchingCertificateArtifact(
        decision_policy_version="policy-v1",
        group_version_id="g1",
        rows=(
            GroupCertificateRow(
                requirement_ordinal=0,
                requirement_version_id="r1",
                text_hash=sha("alpha"),
                selected_observation_id="o1",
            ),
        ),
    )


def test_certificate_shapes_and_digests_are_strict() -> None:
    group_certificate = _group_certificate()
    assert len(group_certificate.certificate_digest) == 64
    with pytest.raises(ValidationError, match="one to eight"):
        GroupMatchingCertificateArtifact(
            decision_policy_version="policy-v1",
            group_version_id="g1",
            rows=(),
        )
    with pytest.raises(ValidationError, match="digest"):
        replace(group_certificate, certificate_digest="0" * 64)
    with pytest.raises(ValidationError, match="unique"):
        GroupMatchingCertificateArtifact(
            decision_policy_version="policy-v1",
            group_version_id="g1",
            rows=(
                group_certificate.rows[0],
                GroupCertificateRow(
                    requirement_ordinal=1,
                    requirement_version_id="r2",
                    text_hash=sha("alpha"),
                    selected_observation_id="o2",
                ),
            ),
        )

    valid_shapes = (
        ClaimCertificateArtifact(
            claim_id="c1",
            decision_policy_version="policy-v1",
            support_kind=ClaimSupportKind.NONE,
        ),
        ClaimCertificateArtifact(
            claim_id="c1",
            decision_policy_version="policy-v1",
            support_kind=ClaimSupportKind.DIRECT,
            direct_support_observation_id="direct-o",
        ),
        ClaimCertificateArtifact(
            claim_id="c1",
            decision_policy_version="policy-v1",
            support_kind=ClaimSupportKind.GROUP,
            group_version_id="g1",
            group_certificate_digest=group_certificate.certificate_digest,
        ),
    )
    assert len({certificate.certificate_digest for certificate in valid_shapes}) == 3

    with pytest.raises(ValidationError, match="NONE"):
        replace(valid_shapes[0], direct_support_observation_id="o")
    with pytest.raises(ValidationError, match="DIRECT"):
        replace(valid_shapes[1], group_version_id="g1")
    with pytest.raises(ValidationError, match="GROUP"):
        replace(valid_shapes[2], group_certificate_digest=None)
    with pytest.raises(ValidationError, match="support_kind"):
        replace(
            valid_shapes[0],
            support_kind="invented",  # type: ignore[arg-type]
            certificate_digest="",
        )
