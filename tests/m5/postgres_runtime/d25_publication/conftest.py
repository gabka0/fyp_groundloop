"""Isolated migration-017 fixture for Lane-B publication tests."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
    install_m5_persisted_matching_bundle,
    install_m5_runtime_bundle,
    install_m5_runtime_recovery_bundle,
)
from tests.m5.postgres.helpers import SeededBase, seed_base


def _database_url() -> str:
    value = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if value is None:
        pytest.skip("live PostgreSQL test database is not configured")
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


@dataclass(frozen=True, slots=True)
class D25PublicationDatabase:
    connection: Connection[Any]
    base: SeededBase
    schema_name: str


@pytest.fixture
def d25_publication_db() -> Iterator[D25PublicationDatabase]:
    dsn = _database_url()
    schema_name = f"groundloop_d25_publication_{uuid.uuid4().hex}"
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
            apply_legacy_migrations(connection)
            connection.commit()
            install_m5_core_bundle(connection)
            install_m5_runtime_bundle(connection)
            install_m5_runtime_recovery_bundle(connection)
            install_m5_persisted_matching_bundle(connection)
            with connection.transaction():
                base = seed_base(
                    connection,
                    prefix="d25-publication",
                    claim_count=1,
                    chunk_texts=("alpha",),
                )
            yield D25PublicationDatabase(connection, base, schema_name)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )
