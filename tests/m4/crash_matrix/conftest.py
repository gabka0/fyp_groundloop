from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.postgres import apply_m2_schema


@dataclass(frozen=True, slots=True)
class CommittedM4Schema:
    """A PostgreSQL schema whose setup and test operations really commit."""

    url: str
    schema_name: str

    def connect(self) -> Connection[tuple[object, ...]]:
        connection: Connection[tuple[object, ...]] = psycopg.connect(
            self.url, autocommit=True
        )
        connection.execute(
            sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(self.schema_name)
            )
        )
        return connection


@pytest.fixture
def committed_m4_schema() -> Iterator[CommittedM4Schema]:
    """Create one committed schema per crash case, never an outer rollback.

    A failed production microtransaction is followed by a new physical
    connection.  This catches state that an assertion on the original session
    could miss and prevents an enclosing pytest transaction from proving the
    rollback on the implementation's behalf.
    """

    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip(
            "live PostgreSQL test: set GROUNDLOOP_TEST_DATABASE_URL or "
            "GROUNDLOOP_DATABASE_URL"
        )
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    schema_name = f"groundloop_m4_crash_{uuid.uuid4().hex}"
    harness = CommittedM4Schema(psycopg_url, schema_name)
    with psycopg.connect(psycopg_url, autocommit=True) as administrator:
        administrator.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
    try:
        with harness.connect() as connection:
            with connection.transaction():
                apply_m2_schema(connection)
        yield harness
    finally:
        with psycopg.connect(psycopg_url, autocommit=True) as administrator:
            administrator.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )
