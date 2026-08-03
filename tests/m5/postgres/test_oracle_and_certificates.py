"""Independent base-edge Hall oracle and persisted certificate tests."""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import Connection, errors

from groundloop.domain import SubjectKind
from groundloop.m5.digests import normalized_text_hash_v1
from groundloop.m5.domain import (
    ClaimCertificateArtifact,
    ClaimSupportKind,
    GroupCertificateRow,
    GroupMatchingCertificateArtifact,
)
from groundloop.postgres.m5 import (
    build_m5_bootstrap_projection,
    persist_claim_certificate,
    persist_group_certificate,
    read_m5_assignment_audits,
    read_m5_mismatch_counts,
    read_m5_oracle_states,
    replace_m5_working_currency,
    write_m5_materialized_states,
)

from .helpers import (
    force_deferred_checks,
    insert_observation,
    insert_published_group,
    install_current_currency,
    install_test_activation_barrier,
    make_group,
    open_m5_update,
    seed_base,
)


def _install_edge(
    connection: Connection[Any],
    *,
    requirement_id: str,
    chunk_id: str,
    epoch_id: int,
    ordinal: int,
    edge: int,
    task_type: str = "verify_requirement_v1",
    scores: tuple[float, float, float] = (0.9, 0.05, 0.05),
) -> str:
    observation_id = f"observation-{ordinal}-{edge}-{requirement_id}"
    insert_observation(
        connection,
        observation_id=observation_id,
        subject_kind="requirement",
        subject_id=requirement_id,
        chunk_id=chunk_id,
        produced_epoch=epoch_id,
        task_type=task_type,
        scores=scores,
    )
    install_current_currency(
        connection,
        observation_id=observation_id,
        subject_kind="requirement",
        subject_id=requirement_id,
        chunk_id=chunk_id,
        task_type=task_type,
        epoch_id=epoch_id,
    )
    return observation_id


def test_hall_counterexample_and_recursive_assignment_agree_at_matching_three(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(
        m5_connection,
        prefix="hall",
        chunk_texts=("a", "b", "c", "d"),
    )
    group = make_group(
        group_id="hall-group",
        family_id="hall-family",
        claim_id=base.claim_ids[0],
        texts=("r1", "r2", "r3", "r4"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    edges = {
        0: (0, 1),
        1: (0, 1),
        2: (0, 1),
        3: (2, 3),
    }
    for ordinal, chunk_ordinals in edges.items():
        for edge, chunk_ordinal in enumerate(chunk_ordinals):
            _install_edge(
                m5_connection,
                requirement_id=group.requirements[ordinal].requirement_version_id,
                chunk_id=base.chunk_ids[chunk_ordinal],
                epoch_id=base.epoch_id,
                ordinal=ordinal,
                edge=edge,
            )
    force_deferred_checks(m5_connection)

    states = read_m5_oracle_states(m5_connection)
    assert states.groups[group.group_version_id].matching_size == 3
    assert not states.groups[group.group_version_id].complete
    audit = read_m5_assignment_audits(m5_connection)[0]
    assert audit.hall_matching_size == 3
    assert audit.assignment_matching_size == 3
    assert audit.audit_status == "ASSIGNMENT_AUDIT_OK"


def test_isolated_left_vertex_keeps_partial_matching_and_noncanonical_is_inert(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="isolated", chunk_texts=("a", "b"))
    group = make_group(
        group_id="isolated-group",
        family_id="isolated-family",
        claim_id=base.claim_ids[0],
        texts=("isolated", "supported"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    _install_edge(
        m5_connection,
        requirement_id=group.requirements[1].requirement_version_id,
        chunk_id=base.chunk_ids[0],
        epoch_id=base.epoch_id,
        ordinal=1,
        edge=0,
    )
    _install_edge(
        m5_connection,
        requirement_id=group.requirements[0].requirement_version_id,
        chunk_id=base.chunk_ids[1],
        epoch_id=base.epoch_id,
        ordinal=0,
        edge=0,
        task_type="noncanonical-task",
    )
    force_deferred_checks(m5_connection)
    states = read_m5_oracle_states(m5_connection)
    requirement_state = states.requirements[
        group.requirements[0].requirement_version_id
    ]
    assert requirement_state.witness_count == 0
    assert states.groups[group.group_version_id].matching_size == 1
    assert read_m5_assignment_audits(m5_connection)[0].audit_status == (
        "ASSIGNMENT_AUDIT_OK"
    )


def test_complete_group_supports_claim_without_direct_scores_or_refutation(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="composition", chunk_texts=("a", "b"))
    group = make_group(
        group_id="composition-group",
        family_id="composition-family",
        claim_id=base.claim_ids[0],
        texts=("first", "second"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    for ordinal in range(2):
        _install_edge(
            m5_connection,
            requirement_id=group.requirements[ordinal].requirement_version_id,
            chunk_id=base.chunk_ids[ordinal],
            epoch_id=base.epoch_id,
            ordinal=ordinal,
            edge=0,
        )
    # Requirement REFUTE is auditable but cannot refute the owner claim.
    _install_edge(
        m5_connection,
        requirement_id=group.requirements[0].requirement_version_id,
        chunk_id=base.chunk_ids[1],
        epoch_id=base.epoch_id,
        ordinal=0,
        edge=1,
        task_type="requirement-refute-audit",
        scores=(0.05, 0.9, 0.05),
    )
    force_deferred_checks(m5_connection)
    states = read_m5_oracle_states(m5_connection)
    claim_state = states.claims[base.claim_ids[0]]
    assert claim_state.support_count == 0
    assert claim_state.refute_count == 0
    assert claim_state.best_support_score is None
    assert claim_state.best_refute_score is None
    assert claim_state.complete_group_ids == (group.group_version_id,)
    assert claim_state.status.value == "supported"
    assert states.answers[base.answer_id].status.value == "valid"


def test_assignment_audit_reports_cap_without_weakening_base_edge_hall_oracle(
    m5_connection: Connection[Any],
) -> None:
    texts = tuple(f"hash-{index:02d}" for index in range(17))
    base = seed_base(m5_connection, prefix="cap", chunk_texts=texts)
    group = make_group(
        group_id="cap-group",
        family_id="cap-family",
        claim_id=base.claim_ids[0],
        texts=tuple(f"requirement {index}" for index in range(8)),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    for edge, chunk_id in enumerate(base.chunk_ids):
        _install_edge(
            m5_connection,
            requirement_id=group.requirements[0].requirement_version_id,
            chunk_id=chunk_id,
            epoch_id=base.epoch_id,
            ordinal=0,
            edge=edge,
        )
    force_deferred_checks(m5_connection)
    state = read_m5_oracle_states(m5_connection).groups[group.group_version_id]
    assert state.matching_size == 1
    audit = read_m5_assignment_audits(m5_connection)[0]
    assert audit.hash_count == 17
    assert audit.audit_status == "ASSIGNMENT_AUDIT_CAP_EXCEEDED"
    assert audit.assignment_matching_size is None


def test_current_certificates_and_materialized_states_match_independent_oracle(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="certificate", chunk_texts=("a", "b"))
    group = make_group(
        group_id="certificate-group",
        family_id="certificate-family",
        claim_id=base.claim_ids[0],
        texts=("first", "second"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    observation_ids = tuple(
        _install_edge(
            m5_connection,
            requirement_id=group.requirements[ordinal].requirement_version_id,
            chunk_id=base.chunk_ids[ordinal],
            epoch_id=base.epoch_id,
            ordinal=ordinal,
            edge=0,
        )
        for ordinal in range(2)
    )
    group_certificate = GroupMatchingCertificateArtifact(
        decision_policy_version=base.policy_version,
        group_version_id=group.group_version_id,
        rows=tuple(
            GroupCertificateRow(
                requirement_ordinal=ordinal,
                requirement_version_id=group.requirements[
                    ordinal
                ].requirement_version_id,
                text_hash=normalized_text_hash_v1(("a", "b")[ordinal]),
                selected_observation_id=observation_ids[ordinal],
            )
            for ordinal in range(2)
        ),
    )
    claim_certificate = ClaimCertificateArtifact(
        claim_id=base.claim_ids[0],
        decision_policy_version=base.policy_version,
        support_kind=ClaimSupportKind.GROUP,
        group_version_id=group.group_version_id,
        group_certificate_digest=group_certificate.certificate_digest,
    )
    persist_group_certificate(m5_connection, group_certificate)
    persist_claim_certificate(m5_connection, claim_certificate)
    force_deferred_checks(m5_connection)
    validity = m5_connection.execute(
        """
        SELECT certificate_valid
        FROM groundloop_m5_group_certificate_validity_oracle
        WHERE certificate_digest = %s
        """,
        (group_certificate.certificate_digest,),
    ).fetchone()
    assert validity == (True,)

    projection = build_m5_bootstrap_projection(m5_connection)
    write_m5_materialized_states(
        m5_connection,
        states=projection.states,
        decision_policy_version=projection.decision_policy_version,
        epoch_id=projection.epoch_id,
        revision=projection.revision,
        group_certificates=projection.group_certificates,
        claim_certificates=projection.claim_certificates,
        publish=False,
    )
    force_deferred_checks(m5_connection)
    assert read_m5_mismatch_counts(m5_connection) == type(
        read_m5_mismatch_counts(m5_connection)
    )(0, 0, 0, 0, 0, 0, 0)


def test_as_of_certificate_detects_selected_edge_loss_but_preserves_history(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="asof", chunk_texts=("a", "b"))
    group = make_group(
        group_id="asof-group",
        family_id="asof-family",
        claim_id=base.claim_ids[0],
        texts=("first", "second"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    for ordinal in range(2):
        _install_edge(
            m5_connection,
            requirement_id=group.requirements[ordinal].requirement_version_id,
            chunk_id=base.chunk_ids[ordinal],
            epoch_id=base.epoch_id,
            ordinal=ordinal,
            edge=0,
        )
    install_test_activation_barrier(m5_connection, base)
    digest_row = m5_connection.execute(
        """
        SELECT certificate_digest
        FROM groundloop_m5_group_state_materialized
        WHERE group_version_id = %s
        """,
        (group.group_version_id,),
    ).fetchone()
    assert digest_row is not None
    certificate_digest = str(digest_row[0]).strip()
    claim_digest_row = m5_connection.execute(
        """
        SELECT certificate_digest
        FROM groundloop_m5_claim_state_materialized
        WHERE claim_id = %s
        """,
        (base.claim_ids[0],),
    ).fetchone()
    assert claim_digest_row is not None
    claim_certificate_digest = str(claim_digest_row[0]).strip()
    selected = m5_connection.execute(
        """
        SELECT observation.subject_kind::text, observation.subject_id,
               observation.chunk_version_id, observation.task_type,
               observation.observation_id
        FROM groundloop_m5_group_certificate_artifact_row AS artifact_row
        JOIN groundloop_semantic_observation AS observation
          ON observation.observation_id = artifact_row.selected_observation_id
        WHERE artifact_row.certificate_digest = %s
        ORDER BY artifact_row.requirement_ordinal
        LIMIT 1
        """,
        (certificate_digest,),
    ).fetchone()
    assert selected is not None

    epoch_id = open_m5_update(m5_connection, base, event_id="asof-update")
    assert m5_connection.execute(
        "SELECT groundloop_m5_group_certificate_valid_at(%s, %s, 0)",
        (certificate_digest, epoch_id),
    ).fetchone() == (True,)
    assert m5_connection.execute(
        "SELECT groundloop_m5_claim_certificate_valid_at(%s, %s, 0)",
        (claim_certificate_digest, epoch_id),
    ).fetchone() == (True,)
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_group_certificate_binding (
            epoch_id, group_version_id, valid_from_revision,
            valid_to_revision, certificate_digest
        ) VALUES (%s, %s, 0, NULL, %s)
        """,
        (epoch_id, group.group_version_id, certificate_digest),
    )
    force_deferred_checks(m5_connection)

    m5_connection.execute(
        "UPDATE groundloop_epoch SET revision = 1 WHERE epoch_id = %s", (epoch_id,)
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_working_group_certificate_binding
        SET valid_to_revision = 1
        WHERE epoch_id = %s AND group_version_id = %s
        """,
        (epoch_id, group.group_version_id),
    )
    replace_m5_working_currency(
        m5_connection,
        epoch_id=epoch_id,
        subject_kind=SubjectKind(str(selected[0])),
        subject_id=str(selected[1]),
        chunk_version_id=str(selected[2]),
        task_type=str(selected[3]),
        observation_id=None,
        revision=1,
    )
    force_deferred_checks(m5_connection)
    assert m5_connection.execute(
        "SELECT groundloop_m5_group_certificate_valid_at(%s, %s, 0)",
        (certificate_digest, epoch_id),
    ).fetchone() == (True,)
    assert m5_connection.execute(
        "SELECT groundloop_m5_group_certificate_valid_at(%s, %s, 1)",
        (certificate_digest, epoch_id),
    ).fetchone() == (False,)
    assert m5_connection.execute(
        "SELECT groundloop_m5_claim_certificate_valid_at(%s, %s, 1)",
        (claim_certificate_digest, epoch_id),
    ).fetchone() == (False,)

    with pytest.raises(errors.RaiseException, match="invalid M5 working group"):
        with m5_connection.transaction():
            m5_connection.execute(
                """
                INSERT INTO groundloop_m5_working_group_certificate_binding (
                    epoch_id, group_version_id, valid_from_revision,
                    valid_to_revision, certificate_digest
                ) VALUES (%s, %s, 1, NULL, %s)
                """,
                (epoch_id, group.group_version_id, certificate_digest),
            )
            m5_connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


def test_claim_as_of_validator_uses_bound_certificate_not_latest_state_pointer(
    m5_connection: Connection[Any],
) -> None:
    base = seed_base(m5_connection, prefix="binding-asof", chunk_texts=("a", "b", "c"))
    group = make_group(
        group_id="binding-asof-group",
        family_id="binding-asof-family",
        claim_id=base.claim_ids[0],
        texts=("first", "second"),
    )
    insert_published_group(m5_connection, group=group, epoch_id=base.epoch_id)
    _install_edge(
        m5_connection,
        requirement_id=group.requirements[0].requirement_version_id,
        chunk_id=base.chunk_ids[0],
        epoch_id=base.epoch_id,
        ordinal=0,
        edge=0,
    )
    _install_edge(
        m5_connection,
        requirement_id=group.requirements[0].requirement_version_id,
        chunk_id=base.chunk_ids[2],
        epoch_id=base.epoch_id,
        ordinal=0,
        edge=1,
    )
    _install_edge(
        m5_connection,
        requirement_id=group.requirements[1].requirement_version_id,
        chunk_id=base.chunk_ids[1],
        epoch_id=base.epoch_id,
        ordinal=1,
        edge=0,
    )
    install_test_activation_barrier(m5_connection, base)
    old_group_digest = str(
        m5_connection.execute(
            """
            SELECT certificate_digest
            FROM groundloop_m5_group_state_materialized
            WHERE group_version_id = %s
            """,
            (group.group_version_id,),
        ).fetchone()[0]
    ).strip()
    old_claim_digest = str(
        m5_connection.execute(
            """
            SELECT certificate_digest
            FROM groundloop_m5_claim_state_materialized
            WHERE claim_id = %s
            """,
            (base.claim_ids[0],),
        ).fetchone()[0]
    ).strip()
    old_rows = m5_connection.execute(
        """
        SELECT requirement_ordinal, requirement_version_id, text_hash,
               selected_observation_id
        FROM groundloop_m5_group_certificate_artifact_row
        WHERE certificate_digest = %s
        ORDER BY requirement_ordinal
        """,
        (old_group_digest,),
    ).fetchall()
    old_first_observation = str(old_rows[0][3])
    alternative = m5_connection.execute(
        """
        SELECT edge.text_hash, edge.active_observation_ids[1]
        FROM groundloop_m5_active_requirement_edge_oracle AS edge
        WHERE edge.requirement_version_id = %s
          AND edge.text_hash <> %s
        ORDER BY edge.text_hash
        LIMIT 1
        """,
        (group.requirements[0].requirement_version_id, str(old_rows[0][2])),
    ).fetchone()
    assert alternative is not None

    epoch_id = open_m5_update(
        m5_connection,
        base,
        event_id="binding-asof-update",
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_group_certificate_binding (
            epoch_id, group_version_id, valid_from_revision,
            valid_to_revision, certificate_digest
        ) VALUES (%s, %s, 0, NULL, %s)
        """,
        (epoch_id, group.group_version_id, old_group_digest),
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_claim_certificate_binding (
            epoch_id, claim_id, valid_from_revision,
            valid_to_revision, certificate_digest
        ) VALUES (%s, %s, 0, NULL, %s)
        """,
        (epoch_id, base.claim_ids[0], old_claim_digest),
    )
    force_deferred_checks(m5_connection)
    m5_connection.execute(
        "UPDATE groundloop_epoch SET revision = 1 WHERE epoch_id = %s",
        (epoch_id,),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_working_group_certificate_binding
        SET valid_to_revision = 1
        WHERE epoch_id = %s AND group_version_id = %s
        """,
        (epoch_id, group.group_version_id),
    )
    m5_connection.execute(
        """
        UPDATE groundloop_m5_working_claim_certificate_binding
        SET valid_to_revision = 1
        WHERE epoch_id = %s AND claim_id = %s
        """,
        (epoch_id, base.claim_ids[0]),
    )
    old_observation = m5_connection.execute(
        """
        SELECT subject_kind::text, subject_id, chunk_version_id, task_type
        FROM groundloop_semantic_observation
        WHERE observation_id = %s
        """,
        (old_first_observation,),
    ).fetchone()
    assert old_observation is not None
    replace_m5_working_currency(
        m5_connection,
        epoch_id=epoch_id,
        subject_kind=SubjectKind(str(old_observation[0])),
        subject_id=str(old_observation[1]),
        chunk_version_id=str(old_observation[2]),
        task_type=str(old_observation[3]),
        observation_id=None,
        revision=1,
    )
    new_rows = tuple(
        GroupCertificateRow(
            requirement_ordinal=int(row[0]),
            requirement_version_id=str(row[1]),
            text_hash=(str(alternative[0]) if int(row[0]) == 0 else str(row[2])),
            selected_observation_id=(
                str(alternative[1]) if int(row[0]) == 0 else str(row[3])
            ),
        )
        for row in old_rows
    )
    new_group_certificate = GroupMatchingCertificateArtifact(
        decision_policy_version=base.policy_version,
        group_version_id=group.group_version_id,
        rows=new_rows,
    )
    new_claim_certificate = ClaimCertificateArtifact(
        claim_id=base.claim_ids[0],
        decision_policy_version=base.policy_version,
        support_kind=ClaimSupportKind.GROUP,
        group_version_id=group.group_version_id,
        group_certificate_digest=new_group_certificate.certificate_digest,
    )
    persist_group_certificate(m5_connection, new_group_certificate)
    persist_claim_certificate(m5_connection, new_claim_certificate)
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_group_certificate_binding (
            epoch_id, group_version_id, valid_from_revision,
            valid_to_revision, certificate_digest
        ) VALUES (%s, %s, 1, NULL, %s)
        """,
        (epoch_id, group.group_version_id, new_group_certificate.certificate_digest),
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_group_state (
            epoch_id, group_version_id, requirement_count, satisfied_count,
            matching_size, complete, decision_policy_version,
            certificate_digest, updated_revision
        ) VALUES (%s, %s, 2, 2, 2, true, %s, %s, 1)
        """,
        (
            epoch_id,
            group.group_version_id,
            base.policy_version,
            new_group_certificate.certificate_digest,
        ),
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_claim_certificate_binding (
            epoch_id, claim_id, valid_from_revision,
            valid_to_revision, certificate_digest
        ) VALUES (%s, %s, 1, NULL, %s)
        """,
        (
            epoch_id,
            base.claim_ids[0],
            new_claim_certificate.certificate_digest,
        ),
    )
    m5_connection.execute(
        """
        INSERT INTO groundloop_m5_working_claim_state (
            epoch_id, claim_id, support_count, refute_count,
            best_support_score, best_refute_score,
            supporting_observation_ids, refuting_observation_ids,
            complete_group_count, complete_group_ids, status,
            decision_policy_version, certificate_digest, updated_revision
        ) VALUES (
            %s, %s, 0, 0, NULL, NULL, ARRAY[]::text[], ARRAY[]::text[],
            1, ARRAY[%s]::text[], 'supported', %s, %s, 1
        )
        """,
        (
            epoch_id,
            base.claim_ids[0],
            group.group_version_id,
            base.policy_version,
            new_claim_certificate.certificate_digest,
        ),
    )
    force_deferred_checks(m5_connection)

    assert m5_connection.execute(
        "SELECT groundloop_m5_claim_certificate_valid_at(%s, %s, 0)",
        (old_claim_digest, epoch_id),
    ).fetchone() == (True,)
    assert m5_connection.execute(
        "SELECT groundloop_m5_claim_certificate_valid_at(%s, %s, 0)",
        (new_claim_certificate.certificate_digest, epoch_id),
    ).fetchone() == (False,)
    assert m5_connection.execute(
        "SELECT groundloop_m5_claim_certificate_valid_at(%s, %s, 1)",
        (old_claim_digest, epoch_id),
    ).fetchone() == (False,)
    assert m5_connection.execute(
        "SELECT groundloop_m5_claim_certificate_valid_at(%s, %s, 1)",
        (new_claim_certificate.certificate_digest, epoch_id),
    ).fetchone() == (True,)
