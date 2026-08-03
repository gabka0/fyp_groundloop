"""Typed-subject, normalization, lifecycle, and eligibility constraints."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.m5.digests import normalize_text_v1

from .helpers import (
    force_deferred_checks,
    insert_observation,
    insert_published_group,
    install_test_activation_barrier,
    make_group,
    open_m5_update,
    seed_base,
    sha,
)

WHITESPACE_V1 = tuple(
    [*range(0x0009, 0x000E), *range(0x001C, 0x0021)]
    + [0x0085, 0x00A0, 0x1680]
    + list(range(0x2000, 0x200B))
    + [0x2028, 0x2029, 0x202F, 0x205F, 0x3000]
)


def _insert_staged_successor(
    connection: Connection[Any],
    *,
    group: Any,
    epoch_id: int,
) -> None:
    connection.execute(
        """
        INSERT INTO groundloop_m5_group_version (
            group_version_id, group_family_id, creator_epoch_id,
            lifecycle_state, group_type, construction_kind,
            construction_source_id, constructor_model_id,
            constructor_model_version, constructor_prompt_version,
            supersedes_group_version_id, semantic_structure_hash,
            record_payload_hash
        ) VALUES (
            %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            group.group_version_id,
            group.group_family_id,
            epoch_id,
            group.group_type.value,
            group.construction_kind.value,
            group.construction_source_id,
            group.constructor_model_id,
            group.constructor_model_version,
            group.constructor_prompt_version,
            group.supersedes_group_version_id,
            group.semantic_structure_hash,
            group.record_payload_hash,
        ),
    )
    for requirement in group.requirements:
        connection.execute(
            """
            INSERT INTO groundloop_m5_requirement_version (
                requirement_version_id, group_version_id, creator_epoch_id,
                lifecycle_state, ordinal, requirement_text,
                requirement_text_hash, constructor_model_id,
                constructor_model_version, constructor_prompt_version,
                supersedes_requirement_version_id
            ) VALUES (
                %s, %s, %s, 'STAGED', %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                requirement.requirement_version_id,
                requirement.group_version_id,
                epoch_id,
                requirement.ordinal,
                requirement.requirement_text,
                requirement.requirement_text_hash,
                requirement.constructor_model_id,
                requirement.constructor_model_version,
                requirement.constructor_prompt_version,
                requirement.supersedes_requirement_version_id,
            ),
        )


def test_sql_normalization_matches_python_for_all_frozen_code_points(
    m5_connection: Connection[Any],
) -> None:
    assert len(WHITESPACE_V1) == 29
    vectors = [
        f"{chr(point)}alpha{chr(point)}{chr(point)}beta{chr(point)}"
        for point in WHITESPACE_V1
    ]
    vectors.extend(("a\u200bb", "λ  evidence\u3000group", "plain"))
    for value in vectors:
        row = m5_connection.execute(
            "SELECT groundloop_normalize_text_v1(%s)", (value,)
        ).fetchone()
        assert row is not None
        assert str(row[0]) == normalize_text_v1(value)
    assert m5_connection.execute(
        "SELECT groundloop_normalize_text_v1(NULL)"
    ).fetchone() == (None,)


def test_future_claim_and_requirement_subjects_are_registered_by_type(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="subjects")
    group = make_group(
        group_id="subjects-group",
        family_id="subjects-family",
        claim_id=base.claim_ids[0],
        texts=("one requirement",),
        requirement_ids=(base.claim_ids[0],),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    force_deferred_checks(m5_connection)

    rows = m5_connection.execute(
        """
        SELECT subject_kind::text, subject_id
        FROM groundloop_semantic_subject
        WHERE subject_id = %s
        ORDER BY subject_kind::text
        """,
        (base.claim_ids[0],),
    ).fetchall()
    assert rows == [
        ("claim", base.claim_ids[0]),
        ("requirement", base.claim_ids[0]),
    ]


def test_orphan_registry_and_wrong_typed_observation_fail_deferred_integrity(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="orphan")
    with pytest.raises(errors.RaiseException, match="no requirement subtype"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                INSERT INTO groundloop_semantic_subject (
                    subject_kind, subject_id, registered_epoch
                ) VALUES ('requirement', 'missing-requirement', %s)
                """,
                (base.epoch_id,),
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")

    with pytest.raises(errors.ForeignKeyViolation):
        with m5_connection.transaction():
            insert_observation(
                m5_connection,
                observation_id="wrong-kind-observation",
                subject_kind="requirement",
                subject_id=base.claim_ids[0],
                chunk_id=base.chunk_ids[0],
                produced_epoch=base.epoch_id,
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_group_hashes_are_cross_language_exact_and_semantic_duplicates_overlap(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="group-hash")
    group = make_group(
        group_id="group-hash-v1",
        family_id="group-hash-family",
        claim_id=base.claim_ids[0],
        texts=("alpha  requirement", "β requirement"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    force_deferred_checks(m5_connection)
    row = m5_connection.execute(
        """
        SELECT groundloop_m5_expected_semantic_structure(%s),
               groundloop_m5_expected_group_record(%s)
        """,
        (group.group_version_id, group.group_version_id),
    ).fetchone()
    assert row is not None
    assert tuple(str(value).strip() for value in row) == (
        group.semantic_structure_hash,
        group.record_payload_hash,
    )

    duplicate = make_group(
        group_id="group-hash-duplicate",
        family_id="group-hash-duplicate-family",
        claim_id=base.claim_ids[0],
        texts=("β requirement", "alpha requirement"),
    )
    with pytest.raises(errors.ExclusionViolation):
        with m5_connection.transaction():
            insert_published_group(
                m5_connection,
                group=duplicate,
                epoch_id=base.epoch_id,
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_dense_group_and_record_digest_tampering_are_rejected_atomically(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="tamper")
    with pytest.raises(errors.RaiseException, match="dense"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_family (
                    group_family_id, claim_id, creator_epoch_id, lifecycle_state
                ) VALUES ('tamper-family', %s, %s, 'PUBLISHED')
                """,
                (base.claim_ids[0], base.epoch_id),
            )
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_version (
                    group_version_id, group_family_id, creator_epoch_id,
                    lifecycle_state, group_type, construction_kind,
                    construction_source_id, constructor_model_id,
                    constructor_model_version, constructor_prompt_version,
                    supersedes_group_version_id, semantic_structure_hash,
                    record_payload_hash
                ) VALUES (
                    'tamper-group', 'tamper-family', %s, 'PUBLISHED',
                    'support_conjunction', 'controlled', 'fixture',
                    NULL, NULL, NULL, NULL, %s, %s
                )
                """,
                (base.epoch_id, "a" * 64, "b" * 64),
            )
            for ordinal in (0, 2):
                text = f"requirement {ordinal}"
                m5_connection.execute(
                    """
                    INSERT INTO groundloop_m5_requirement_version (
                        requirement_version_id, group_version_id,
                        creator_epoch_id, lifecycle_state, ordinal,
                        requirement_text, requirement_text_hash,
                        constructor_model_id, constructor_model_version,
                        constructor_prompt_version,
                        supersedes_requirement_version_id
                    ) VALUES (%s, 'tamper-group', %s, 'PUBLISHED', %s,
                              %s, %s, NULL, NULL, NULL, NULL)
                    """,
                    (
                        f"tamper-r{ordinal}",
                        base.epoch_id,
                        ordinal,
                        text,
                        sha(text),
                    ),
                )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert m5_connection.execute(
        "SELECT count(*) FROM groundloop_m5_group_family"
    ).fetchone() == (0,)


def test_empty_group_is_rejected_by_deferred_whole_group_constraint(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="empty-group")
    with pytest.raises(errors.RaiseException, match="one-to-eight dense"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_family (
                    group_family_id, claim_id, creator_epoch_id, lifecycle_state
                ) VALUES ('empty-family', %s, %s, 'PUBLISHED')
                """,
                (base.claim_ids[0], base.epoch_id),
            )
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_version (
                    group_version_id, group_family_id, creator_epoch_id,
                    lifecycle_state, group_type, construction_kind,
                    construction_source_id, supersedes_group_version_id,
                    semantic_structure_hash, record_payload_hash
                ) VALUES (
                    'empty-group', 'empty-family', %s, 'PUBLISHED',
                    'support_conjunction', 'controlled', 'fixture', NULL,
                    %s, %s
                )
                """,
                (base.epoch_id, "a" * 64, "b" * 64),
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_ninth_requirement_is_rejected_by_the_physical_ordinal_bound(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="nine-requirements")
    with pytest.raises(errors.CheckViolation):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_family (
                    group_family_id, claim_id, creator_epoch_id, lifecycle_state
                ) VALUES ('nine-family', %s, %s, 'PUBLISHED')
                """,
                (base.claim_ids[0], base.epoch_id),
            )
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_version (
                    group_version_id, group_family_id, creator_epoch_id,
                    lifecycle_state, group_type, construction_kind,
                    construction_source_id, supersedes_group_version_id,
                    semantic_structure_hash, record_payload_hash
                ) VALUES (
                    'nine-group', 'nine-family', %s, 'PUBLISHED',
                    'support_conjunction', 'controlled', 'fixture', NULL,
                    %s, %s
                )
                """,
                (base.epoch_id, "a" * 64, "b" * 64),
            )
            for ordinal in range(9):
                text = f"requirement {ordinal}"
                m5_connection.execute(
                    """
                    INSERT INTO groundloop_m5_requirement_version (
                        requirement_version_id, group_version_id,
                        creator_epoch_id, lifecycle_state, ordinal,
                        requirement_text, requirement_text_hash
                    ) VALUES (
                        %s, 'nine-group', %s, 'PUBLISHED', %s, %s, %s
                    )
                    """,
                    (
                        f"nine-requirement-{ordinal}",
                        base.epoch_id,
                        ordinal,
                        text,
                        sha(text),
                    ),
                )


@pytest.mark.parametrize(
    ("witness_hashes", "supporting_ids", "witness_count", "satisfied"),
    (
        ([None], ["observation"], 1, True),
        (["not-a-sha256"], ["observation"], 1, True),
        ([], [None], 0, False),
        ([], [""], 0, False),
    ),
)
def test_materialized_requirement_arrays_reject_null_blank_and_non_hash_values(
    m5_connection: Connection[Any],
    witness_hashes: list[str | None],
    supporting_ids: list[str | None],
    witness_count: int,
    satisfied: bool,
) -> None:
    base = seed_base(m5_connection, prefix="hostile-array")
    group = make_group(
        group_id="hostile-array-group",
        family_id="hostile-array-family",
        claim_id=base.claim_ids[0],
        texts=("one requirement",),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    force_deferred_checks(m5_connection)
    with pytest.raises(errors.CheckViolation):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_requirement_state_materialized (
                    requirement_version_id, witness_hashes,
                    supporting_observation_ids, witness_count, satisfied,
                    decision_policy_version, updated_epoch, updated_revision
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0)
                """,
                (
                    group.requirements[0].requirement_version_id,
                    witness_hashes,
                    supporting_ids,
                    witness_count,
                    satisfied,
                    base.policy_version,
                    base.epoch_id,
                ),
            )


def test_ineligible_observation_is_immutable_and_rejected_on_shared_surfaces(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="ineligible")
    insert_observation(
        m5_connection,
        observation_id="ineligible-observation",
        subject_kind="claim",
        subject_id=base.claim_ids[0],
        chunk_id=base.chunk_ids[0],
        produced_epoch=base.epoch_id,
        task_type="nli",
        eligible=False,
    )
    with pytest.raises(errors.RaiseException, match="immutable"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                UPDATE groundloop_semantic_observation
                SET eligible_for_currency = true
                WHERE observation_id = 'ineligible-observation'
                """
            )

    statements = (
        """
        INSERT INTO groundloop_observation_currency (
            subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, installed_revision
        ) VALUES ('claim', %s, %s, 'nli', 'ineligible-observation', 0)
        """,
        """
        INSERT INTO groundloop_published_observation_currency (
            subject_kind, subject_id, chunk_version_id, task_type,
            observation_id, valid_from_epoch, valid_to_epoch
        ) VALUES ('claim', %s, %s, 'nli', 'ineligible-observation', %s, NULL)
        """,
        """
        INSERT INTO groundloop_working_observation_delta (
            epoch_id, subject_kind, subject_id, chunk_version_id, task_type,
            base_observation_id, working_observation_id, installed_revision
        ) VALUES (%s, 'claim', %s, %s, 'nli',
                  'ineligible-observation', NULL, 0)
        """,
    )
    parameters = (
        (base.claim_ids[0], base.chunk_ids[0]),
        (base.claim_ids[0], base.chunk_ids[0], base.epoch_id),
        (base.epoch_id, base.claim_ids[0], base.chunk_ids[0]),
    )
    for statement, values in zip(statements, parameters, strict=True):
        with pytest.raises(errors.RaiseException, match="not eligible"):
            with m5_connection.transaction():
                m5_connection.execute(statement, values)


def test_failed_replacement_preserves_published_truth_and_does_not_consume_lineage(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="replacement")
    original = make_group(
        group_id="replacement-original",
        family_id="replacement-family",
        claim_id=base.claim_ids[0],
        texts=("original requirement",),
    )
    insert_published_group(
        m5_connection,
        group=original,
        epoch_id=base.epoch_id,
    )
    install_test_activation_barrier(m5_connection, base)

    failed_epoch = open_m5_update(
        m5_connection,
        base,
        event_id="replacement-failed",
        update_kind="replace_group",
    )
    failed = make_group(
        group_id="replacement-failed-version",
        family_id=original.group_family_id,
        claim_id=base.claim_ids[0],
        texts=("failed successor",),
        predecessors=(original.requirements[0].requirement_version_id,),
        supersedes_group_id=original.group_version_id,
    )
    _insert_staged_successor(m5_connection, group=failed, epoch_id=failed_epoch)
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_group_deactivation (
            epoch_id, group_version_id, action,
            successor_group_version_id, event_id
        ) VALUES (%s, %s, 'REPLACE', %s, 'replacement-failed')
        """,
        (failed_epoch, original.group_version_id, failed.group_version_id),
    )
    force_deferred_checks(m5_connection)
    m5_connection.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s
        """,
        (failed.group_version_id,),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'FAILED'
        WHERE group_version_id = %s
        """,
        (failed.group_version_id,),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_epoch
        SET semantic_status = 'failed', evaluation_state = 'failed'
        WHERE epoch_id = %s
        """,
        (failed_epoch,),
    )
    force_deferred_checks(m5_connection)
    assert m5_connection.execute(
        """
        SELECT valid_to_epoch
        FROM groundloop_m5_group_validity
        WHERE group_version_id = %s
        """,
        (original.group_version_id,),
    ).fetchone() == (None,)

    retry_epoch = open_m5_update(
        m5_connection,
        base,
        event_id="replacement-retry",
        update_kind="replace_group",
    )
    retry = make_group(
        group_id="replacement-retry-version",
        family_id=original.group_family_id,
        claim_id=base.claim_ids[0],
        texts=("valid successor",),
        predecessors=(original.requirements[0].requirement_version_id,),
        supersedes_group_id=original.group_version_id,
    )
    _insert_staged_successor(m5_connection, group=retry, epoch_id=retry_epoch)
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_group_deactivation (
            epoch_id, group_version_id, action,
            successor_group_version_id, event_id
        ) VALUES (%s, %s, 'REPLACE', %s, 'replacement-retry')
        """,
        (retry_epoch, original.group_version_id, retry.group_version_id),
    )
    force_deferred_checks(m5_connection)

    m5_connection.execute(
        """
        UPDATE groundloop_m5_group_validity
        SET valid_to_epoch = %s
        WHERE group_version_id = %s
        """,
        (retry_epoch, original.group_version_id),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_requirement_version
        SET lifecycle_state = 'PUBLISHED'
        WHERE group_version_id = %s
        """,
        (retry.group_version_id,),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_group_version
        SET lifecycle_state = 'PUBLISHED'
        WHERE group_version_id = %s
        """,
        (retry.group_version_id,),
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_group_validity (
            group_version_id, group_family_id, claim_id,
            semantic_structure_hash, supersedes_group_version_id,
            valid_from_epoch, valid_to_epoch
        ) VALUES (%s, %s, %s, %s, %s, %s, NULL)
        """,
        (
            retry.group_version_id,
            retry.group_family_id,
            base.claim_ids[0],
            retry.semantic_structure_hash,
            original.group_version_id,
            retry_epoch,
        ),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_epoch
        SET semantic_status = 'sealed', evaluation_state = 'complete',
            publication_mode = 'strict', sealed_at = now()
        WHERE epoch_id = %s
        """,
        (retry_epoch,),
    )
    force_deferred_checks(m5_connection)
    assert m5_connection.execute(
        """
        SELECT group_version_id
        FROM groundloop_m5_group_validity
        WHERE valid_to_epoch IS NULL
        """
    ).fetchall() == [(retry.group_version_id,)]


def test_two_live_successors_are_rejected_as_an_active_lineage_fork(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="fork")
    original = make_group(
        group_id="fork-original",
        family_id="fork-family",
        claim_id=base.claim_ids[0],
        texts=("original",),
    )
    insert_published_group(m5_connection, group=original, epoch_id=base.epoch_id)
    install_test_activation_barrier(m5_connection, base)
    epoch_id = open_m5_update(
        m5_connection,
        base,
        event_id="fork-update",
        update_kind="replace_group",
    )
    successors = tuple(
        make_group(
            group_id=f"fork-successor-{suffix}",
            family_id=original.group_family_id,
            claim_id=base.claim_ids[0],
            texts=(f"successor {suffix}",),
            predecessors=(original.requirements[0].requirement_version_id,),
            supersedes_group_id=original.group_version_id,
        )
        for suffix in ("a", "b")
    )
    with pytest.raises(errors.RaiseException, match="active lineage contains a fork"):
        with m5_connection.transaction():
            for successor in successors:
                _insert_staged_successor(
                    m5_connection,
                    group=successor,
                    epoch_id=epoch_id,
                )
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_deactivation (
                    epoch_id, group_version_id, action,
                    successor_group_version_id, event_id
                ) VALUES (%s, %s, 'REPLACE', %s, 'fork-update')
                """,
                (
                    epoch_id,
                    original.group_version_id,
                    successors[0].group_version_id,
                ),
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_durable_retirement_prevents_reopen_and_later_successor(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="retirement")
    original = make_group(
        group_id="retirement-original",
        family_id="retirement-family",
        claim_id=base.claim_ids[0],
        texts=("retired requirement",),
    )
    insert_published_group(m5_connection, group=original, epoch_id=base.epoch_id)
    install_test_activation_barrier(m5_connection, base)
    retired_epoch = open_m5_update(
        m5_connection,
        base,
        event_id="retirement-event",
        update_kind="retire_group",
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_group_deactivation (
            epoch_id, group_version_id, action,
            successor_group_version_id, event_id
        ) VALUES (%s, %s, 'RETIRE', NULL, 'retirement-event')
        """,
        (retired_epoch, original.group_version_id),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_group_validity
        SET valid_to_epoch = %s
        WHERE group_version_id = %s
        """,
        (retired_epoch, original.group_version_id),
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_group_family_retirement (
            group_family_id, retired_epoch_id, event_id
        ) VALUES (%s, %s, 'retirement-event')
        """,
        (original.group_family_id, retired_epoch),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_epoch
        SET semantic_status = 'sealed', evaluation_state = 'complete',
            publication_mode = 'strict', sealed_at = now()
        WHERE epoch_id = %s
        """,
        (retired_epoch,),
    )
    force_deferred_checks(m5_connection)

    with pytest.raises(errors.RaiseException, match="close exactly once"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                UPDATE groundloop_m5_group_validity
                SET valid_to_epoch = NULL
                WHERE group_version_id = %s
                """,
                (original.group_version_id,),
            )

    later_epoch = open_m5_update(
        m5_connection,
        base,
        event_id="retirement-later-successor",
        update_kind="replace_group",
    )
    successor = make_group(
        group_id="retirement-illegal-successor",
        family_id=original.group_family_id,
        claim_id=base.claim_ids[0],
        texts=("illegal successor",),
        predecessors=(original.requirements[0].requirement_version_id,),
        supersedes_group_id=original.group_version_id,
    )
    with pytest.raises(errors.RaiseException, match="retired M5 family"):
        with m5_connection.transaction():
            _insert_staged_successor(
                m5_connection,
                group=successor,
                epoch_id=later_epoch,
            )
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_group_deactivation (
                    epoch_id, group_version_id, action,
                    successor_group_version_id, event_id
                ) VALUES (
                    %s, %s, 'REPLACE', %s,
                    'retirement-later-successor'
                )
                """,
                (
                    later_epoch,
                    original.group_version_id,
                    successor.group_version_id,
                ),
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
