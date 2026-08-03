"""One installed schema, with a rollback-only connection per M5.3 test."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.postgres.migrations import (
    apply_legacy_migrations,
    install_m5_core_bundle,
)


@dataclass(frozen=True, slots=True)
class M5Schema:
    dsn: str
    name: str


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


@pytest.fixture(scope="session")
def m5_schema() -> Iterator[M5Schema]:
    dsn = _database_url()
    schema_name = f"groundloop_m5_postgres_{uuid.uuid4().hex}"
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
            connection.commit()
        yield M5Schema(dsn=dsn, name=schema_name)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
            )


@pytest.fixture
def m5_connection(
    m5_schema: M5Schema,
) -> Iterator[Connection[tuple[object, ...]]]:
    with psycopg.connect(m5_schema.dsn) as connection:
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(m5_schema.name)
            )
        )
        try:
            yield connection
        finally:
            connection.rollback()
