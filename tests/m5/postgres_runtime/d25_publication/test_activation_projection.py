"""Activation-only projection tests for the D25 public helper."""

from __future__ import annotations

from typing import TYPE_CHECKING

from groundloop.m5.runtime.postgres_matching_audit import (
    M5MatchingAuditProjection,
    audit_matching_actual_image,
)
from groundloop.m5.runtime.postgres_matching_publication import (
    install_matching_activation_projection,
)
from groundloop.postgres.m5 import (
    build_m5_bootstrap_projection,
    write_m5_materialized_states,
)
from tests.m5.postgres.helpers import force_deferred_checks, sha

if TYPE_CHECKING:
    from .conftest import D25PublicationDatabase


def test_empty_activation_projection_is_cursor_local_and_exact(
    d25_publication_db: D25PublicationDatabase,
) -> None:
    connection = d25_publication_db.connection
    base = d25_publication_db.base
    receipt = install_matching_activation_projection(
        connection.cursor(),
        expected_m4_head_epoch_id=base.epoch_id,
        expected_head_epoch_revision=0,
        decision_policy_version=base.policy_version,
    )
    assert receipt.mode == "activation"
    assert receipt.epoch_id == base.epoch_id
    assert receipt.revision == 0
    assert (
        receipt.observation_writes,
        receipt.edge_writes,
        receipt.mask_writes,
        receipt.hall_writes,
    ) == (0, 0, 0, 0)
    assert connection.execute(
        """SELECT decision_policy_version,installed_epoch_id,
                  installed_revision
             FROM groundloop_m5_matching_image_current"""
    ).fetchone() == (base.policy_version, base.epoch_id, 0)
    for relation in (
        "groundloop_m5_matching_observation_current",
        "groundloop_m5_matching_edge_current",
        "groundloop_m5_matching_hash_mask_current",
        "groundloop_m5_matching_hall_current",
    ):
        assert connection.execute(f"SELECT count(*) FROM {relation}").fetchone() == (0,)
    # The helper deliberately does not install public heads/activation or own
    # commit.  Roll back this incomplete coordinator transaction explicitly.
    connection.rollback()


def test_committed_empty_activation_passes_actual_image_and_provenance_audit(
    d25_publication_db: D25PublicationDatabase,
) -> None:
    connection = d25_publication_db.connection
    base = d25_publication_db.base
    with connection.transaction():
        projection = build_m5_bootstrap_projection(connection)
        write_m5_materialized_states(
            connection,
            states=projection.states,
            decision_policy_version=projection.decision_policy_version,
            epoch_id=projection.epoch_id,
            revision=projection.revision,
            group_certificates=projection.group_certificates,
            claim_certificates=projection.claim_certificates,
            publish=True,
        )
        install_matching_activation_projection(
            connection.cursor(),
            expected_m4_head_epoch_id=base.epoch_id,
            expected_head_epoch_revision=0,
            decision_policy_version=base.policy_version,
        )
        connection.execute(
            """INSERT INTO groundloop_m5_publication_head
                 (singleton,epoch_id,sealed_revision,updated_at)
               VALUES (true,%s,%s,now())""",
            (base.epoch_id, projection.revision),
        )
        connection.execute(
            """INSERT INTO groundloop_m5_activation
                 (singleton,activation_id,payload_hash,base_m4_epoch_id,activated_at)
               VALUES (true,%s,%s,%s,now())""",
            (
                "d25-publication-activation",
                sha("activation:d25-publication-activation"),
                base.epoch_id,
            ),
        )
        assert (
            connection.execute(
                """UPDATE groundloop_runtime_mode
                  SET mode='m5_active',mode_revision=mode_revision+1,
                      updated_at=now()
                WHERE singleton AND mode='v1_only' AND mode_revision=0"""
            ).rowcount
            == 1
        )
        force_deferred_checks(connection)
    expected = M5MatchingAuditProjection(
        head_epoch_id=base.epoch_id,
        head_revision=0,
        decision_policy_version=base.policy_version,
        observations=(),
        edges=(),
        masks=(),
        halls=(),
    )
    artifact = audit_matching_actual_image(
        connection.cursor(),
        python_expected=expected,
        sql_expected=expected,
    )
    assert artifact.passed
    connection.rollback()
