from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.postgres import apply_m2_schema


@pytest.fixture
def m3_m4_activation_connection() -> Iterator[Connection[tuple[object, ...]]]:
    """Provide a committed schema because activation owns its transactions."""
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for M3-to-M4 activation tests")
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    schema = f"groundloop_m3_m4_activation_{uuid.uuid4().hex}"
    with psycopg.connect(psycopg_url, autocommit=True) as connection:
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
            )
            with connection.transaction():
                apply_m2_schema(connection)
            yield connection
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )
