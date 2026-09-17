"""Isolated migration-017 fixtures for the D25 store-core checkpoint."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from psycopg import Connection

from groundloop.m5.runtime.contracts import (
    M5RuntimeOperationalConfig,
    M5RuntimeWork,
)
from groundloop.m5.runtime.postgres_recovery import (
    persist_structural_open_accounting,
    persist_structural_open_identity,
)
from groundloop.postgres.migrations import install_m5_persisted_matching_bundle
from tests.m5.postgres_runtime import test_migration_017 as schema_fixture


@dataclass(frozen=True, slots=True)
class EmptyStructuralDatabase:
    connection: Connection[Any]
    base_epoch_id: int
    base_revision: int
    policy_version: str
    epoch_id: int
    event_id: str
    payload_hash: str


@dataclass(frozen=True, slots=True)
class RichStructuralDatabase(EmptyStructuralDatabase):
    snapshot: schema_fixture._B3ActivatedSnapshot


def _install_d24_open_accounting(
    connection: Connection[Any], *, epoch_id: int, event_id: str, payload_hash: str
) -> None:
    with connection.cursor() as cursor:
        persist_structural_open_identity(
            cursor,
            epoch_id=epoch_id,
            config=M5RuntimeOperationalConfig.build(30_000),
            root_fallback_required={},
        )
        persist_structural_open_accounting(
            cursor,
            epoch_id=epoch_id,
            structural_event_id=event_id,
            payload_hash=payload_hash,
            structural_work=M5RuntimeWork(),
        )


@pytest.fixture
def empty_structural_database() -> Iterator[EmptyStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        base, policy = schema_fixture._seed_b2_runtime_base(
            connection, "d25-store-empty"
        )
        epoch, event, payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-empty-open",
            base,
            policy,
            direct_bridge=True,
            update_kind="document_insert",
        )
        _install_d24_open_accounting(
            connection, epoch_id=epoch, event_id=event, payload_hash=payload
        )
        yield EmptyStructuralDatabase(
            connection,
            base,
            0,
            policy,
            epoch,
            event,
            payload,
        )
        connection.rollback()


@pytest.fixture
def rich_structural_database() -> Iterator[RichStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection, "d25-store-rich"
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        epoch, event, payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-rich-open",
            snapshot.epoch_id,
            snapshot.policy_version,
            direct_bridge=True,
            update_kind="document_insert",
        )
        _install_d24_open_accounting(
            connection, epoch_id=epoch, event_id=event, payload_hash=payload
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event,
            payload,
            snapshot,
        )
        connection.rollback()


@pytest.fixture
def rich_retirement_database() -> Iterator[RichStructuralDatabase]:
    with schema_fixture._pre017_schema() as (connection, _):
        snapshot = schema_fixture._seed_b3_activated_snapshot(
            connection, "d25-store-retire"
        )
        assert install_m5_persisted_matching_bundle(connection).applied
        connection.commit()
        schema_fixture._register_d26_candidate_policy(connection, snapshot)
        connection.commit()
        predecessor = schema_fixture._d26_predecessor_from_published_image(
            connection,
            group_id=snapshot.partial_group_id,
            base=snapshot.epoch_id,
        )
        event = "d25-store-retire-open-event"
        payload, successor = schema_fixture._d26_structural_payload(
            connection,
            group_id=predecessor.group_id,
            event_id=event,
            action="RETIRE",
        )
        assert successor is None
        epoch, actual_event, actual_payload = schema_fixture._open_b2_runtime_epoch(
            connection,
            "d25-store-retire-open",
            snapshot.epoch_id,
            snapshot.policy_version,
            update_kind="retire_group",
            payload_override=payload,
        )
        assert (actual_event, actual_payload) == (event, payload)
        schema_fixture._stage_b2_group_deactivation(
            connection,
            epoch=epoch,
            event=event,
            group=predecessor.group_id,
            action="RETIRE",
        )
        absent_states = tuple(
            (
                "requirement_state",
                requirement_id,
                state_hash,
            )
            for requirement_id, state_hash in zip(
                predecessor.requirement_ids,
                predecessor.requirement_state_hashes,
                strict=True,
            )
        ) + (("group_state", predecessor.group_id, predecessor.group_state_hash),)
        schema_fixture._apply_empty_b2_structural(
            connection,
            epoch=epoch,
            event=event,
            payload=payload,
            base=snapshot.epoch_id,
            base_revision=snapshot.revision,
            policy=snapshot.policy_version,
            absent_states=absent_states,
            remove_current_group=predecessor.group_id,
        )
        yield RichStructuralDatabase(
            connection,
            snapshot.epoch_id,
            snapshot.revision,
            snapshot.policy_version,
            epoch,
            event,
            payload,
            snapshot,
        )
        connection.rollback()
