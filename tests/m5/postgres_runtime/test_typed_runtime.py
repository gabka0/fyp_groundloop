"""Live acceptance checks for public M5 activation and bootstrap replay."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m5.runtime.contracts import M5ActivationRequest, M5StateReferenceKind
from groundloop.m5.runtime.persistence import (
    PostgresM5RuntimeStore,
    build_m5_bootstrap_changed_state_references,
)
from groundloop.postgres.m5 import build_m5_bootstrap_projection
from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_runtime_bundle,
)
from tests.m5.postgres.helpers import (
    force_deferred_checks,
    insert_observation,
    insert_published_group,
    install_current_currency,
    make_group,
    seed_base,
)


def _database_url() -> str:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


@contextmanager
def _runtime_schema() -> Iterator[Connection[Any]]:
    dsn = _database_url()
    schema_name = f"groundloop_m5_activation_{uuid.uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema_name)
                )
            )
            connection.commit()
            with connection.transaction():
                apply_legacy_migrations(connection)
            install_m5_core_bundle(connection)
            install_m5_runtime_bundle(connection)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


def _activation_snapshot(connection: Connection[Any]) -> tuple[object, ...]:
    relation_names = (
        "groundloop_epoch",
        "groundloop_m5_activation",
        "groundloop_m5_publication_head",
        "groundloop_m5_requirement_state_materialized",
        "groundloop_m5_group_state_materialized",
        "groundloop_m5_claim_state_materialized",
        "groundloop_m5_answer_state_materialized",
        "groundloop_m5_published_requirement_state",
        "groundloop_m5_published_group_state",
        "groundloop_m5_published_claim_state",
        "groundloop_m5_published_answer_state",
        "groundloop_m5_group_certificate_artifact",
        "groundloop_m5_claim_certificate_artifact",
        "groundloop_m5_published_group_certificate_binding",
        "groundloop_m5_published_claim_certificate_binding",
    )
    counts = tuple(
        int(
            connection.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(name))
            ).fetchone()[0]
        )
        for name in relation_names
    )
    mode = connection.execute(
        "SELECT mode, mode_revision, xmin::text FROM groundloop_runtime_mode"
    ).fetchone()
    m4_head = connection.execute(
        "SELECT epoch_id, xmin::text FROM groundloop_m4_publication_head"
    ).fetchone()
    m5_head = connection.execute(
        "SELECT epoch_id, sealed_revision, xmin::text "
        "FROM groundloop_m5_publication_head"
    ).fetchone()
    activation = connection.execute(
        "SELECT activation_id, payload_hash, base_m4_epoch_id, xmin::text "
        "FROM groundloop_m5_activation"
    ).fetchone()
    connection.commit()
    return counts, mode, m4_head, m5_head, activation


def _published_state_artifact_hashes(
    connection: Connection[Any],
) -> dict[tuple[str, str], str]:
    rows = connection.execute(
        """
        SELECT 'requirement_state', state.requirement_version_id,
               groundloop_m5_runtime_requirement_state_artifact(
                   state.requirement_version_id, state.witness_hashes,
                   state.supporting_observation_ids, state.witness_count,
                   state.satisfied, state.decision_policy_version
               )
        FROM groundloop_m5_published_requirement_state AS state
        UNION ALL
        SELECT 'group_state', state.group_version_id,
               groundloop_m5_runtime_group_state_artifact(
                   state.group_version_id, state.requirement_count,
                   state.satisfied_count, state.matching_size, state.complete,
                   state.decision_policy_version, state.certificate_digest
               )
        FROM groundloop_m5_published_group_state AS state
        UNION ALL
        SELECT 'group_certificate', binding.group_version_id,
               binding.certificate_digest
        FROM groundloop_m5_published_group_certificate_binding AS binding
        UNION ALL
        SELECT 'claim_state', state.claim_id,
               groundloop_m5_runtime_claim_state_artifact(
                   state.claim_id, state.support_count, state.refute_count,
                   state.best_support_score, state.best_refute_score,
                   state.supporting_observation_ids,
                   state.refuting_observation_ids, state.complete_group_count,
                   state.complete_group_ids, state.status,
                   state.decision_policy_version, state.certificate_digest
               )
        FROM groundloop_m5_published_claim_state AS state
        UNION ALL
        SELECT 'claim_certificate', binding.claim_id,
               binding.certificate_digest
        FROM groundloop_m5_published_claim_certificate_binding AS binding
        UNION ALL
        SELECT 'answer_state', state.answer_version_id,
               groundloop_m5_runtime_answer_state_artifact(
                   state.answer_version_id, state.required_claim_count,
                   state.supported_count, state.unsupported_count,
                   state.refuted_count, state.conflicted_count, state.status
               )
        FROM groundloop_m5_published_answer_state AS state
        """
    ).fetchall()
    return {
        (str(kind), str(object_id)): str(artifact_hash).strip()
        for kind, object_id, artifact_hash in rows
    }


def test_activation_bootstrap_exact_replay_and_conflict() -> None:
    with _runtime_schema() as connection:
        with connection.transaction():
            base = seed_base(connection, prefix="activation", claim_count=2)
            group = make_group(
                group_id="activation-group",
                family_id="activation-family",
                claim_id=base.claim_ids[0],
                texts=("need alpha", "need beta"),
            )
            insert_published_group(
                connection,
                group=group,
                epoch_id=base.epoch_id,
            )
            for ordinal, requirement in enumerate(group.requirements):
                observation_id = f"activation-requirement-observation-{ordinal}"
                insert_observation(
                    connection,
                    observation_id=observation_id,
                    subject_kind="requirement",
                    subject_id=requirement.requirement_version_id,
                    chunk_id=base.chunk_ids[ordinal],
                    produced_epoch=base.epoch_id,
                )
                install_current_currency(
                    connection,
                    observation_id=observation_id,
                    subject_kind="requirement",
                    subject_id=requirement.requirement_version_id,
                    chunk_id=base.chunk_ids[ordinal],
                    task_type="verify_requirement_v1",
                    epoch_id=base.epoch_id,
                )
            force_deferred_checks(connection)

        store = PostgresM5RuntimeStore(connection)
        projection = build_m5_bootstrap_projection(connection)
        references = build_m5_bootstrap_changed_state_references(projection)
        connection.commit()
        assert {
            kind: sum(reference.kind is kind for reference in references)
            for kind in M5StateReferenceKind
        } == {
            M5StateReferenceKind.REQUIREMENT_STATE: 2,
            M5StateReferenceKind.GROUP_STATE: 1,
            M5StateReferenceKind.GROUP_CERTIFICATE: 1,
            M5StateReferenceKind.CLAIM_STATE: 2,
            M5StateReferenceKind.CLAIM_CERTIFICATE: 2,
            M5StateReferenceKind.ANSWER_STATE: 1,
        }
        request = store.prepare_activation_request("activation-request")
        before_epoch_count = int(
            connection.execute("SELECT count(*) FROM groundloop_epoch").fetchone()[0]
        )
        connection.commit()

        receipt = store.activate(request)

        assert receipt.replayed is False
        assert receipt.base_m4_epoch_id == base.epoch_id
        assert receipt.m5_publication_epoch_id == base.epoch_id
        assert receipt.mode_revision == 1
        assert (
            connection.execute("SELECT count(*) FROM groundloop_epoch").fetchone()[0]
            == before_epoch_count
        )
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_published_claim_state"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_published_claim_certificate_binding"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT count(*) FROM groundloop_m5_published_answer_state"
        ).fetchone()[0] == 1
        assert _published_state_artifact_hashes(connection) == {
            (reference.kind.value, reference.object_id): reference.state_artifact_hash
            for reference in references
        }
        connection.commit()
        activated_snapshot = _activation_snapshot(connection)

        replay = store.activate(request)
        assert replay.replayed is True
        assert replay.receipt_hash == receipt.receipt_hash
        assert _activation_snapshot(connection) == activated_snapshot

        conflicting = M5ActivationRequest.build(
            activation_id=request.activation_id,
            expected_mode_revision=request.expected_mode_revision,
            expected_base_m4_epoch_id=request.expected_base_m4_epoch_id,
            core_schema_bundle_sha256=request.core_schema_bundle_sha256,
            bootstrap_state_hash="f" * 64,
        )
        with pytest.raises(EventConflictError):
            store.activate(conflicting)
        assert _activation_snapshot(connection) == activated_snapshot


@pytest.mark.parametrize(
    "failure_point",
    (
        "activation_before_bootstrap",
        "activation_after_bootstrap",
        "activation_after_m5_head",
        "activation_after_record",
        "activation_after_mode",
        "activation_after_constraints",
    ),
)
def test_activation_is_failure_atomic(failure_point: str) -> None:
    with _runtime_schema() as connection:
        with connection.transaction():
            seed_base(connection, prefix=f"failure-{failure_point}")
        store = PostgresM5RuntimeStore(connection)
        request = store.prepare_activation_request(f"request-{failure_point}")
        before = _activation_snapshot(connection)

        def inject(point: str) -> None:
            if point == failure_point:
                raise RuntimeError(point)

        with pytest.raises(RuntimeError, match=failure_point):
            store.activate(request, failure_injector=inject)

        assert _activation_snapshot(connection) == before


def test_activation_rejects_wrong_bootstrap_without_writes() -> None:
    with _runtime_schema() as connection:
        with connection.transaction():
            seed_base(connection, prefix="wrong-bootstrap")
        store = PostgresM5RuntimeStore(connection)
        request = store.prepare_activation_request("wrong-bootstrap-request")
        wrong = M5ActivationRequest.build(
            activation_id=request.activation_id,
            expected_mode_revision=request.expected_mode_revision,
            expected_base_m4_epoch_id=request.expected_base_m4_epoch_id,
            core_schema_bundle_sha256=request.core_schema_bundle_sha256,
            bootstrap_state_hash="e" * 64,
        )
        before = _activation_snapshot(connection)

        with pytest.raises(InvalidEventError, match="bootstrap state hash mismatch"):
            store.activate(wrong)

        assert _activation_snapshot(connection) == before
