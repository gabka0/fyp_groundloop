from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.postgres import apply_m2_schema


@pytest.fixture
def incrementality_connection() -> Iterator[Connection[Any]]:
    """Create an isolated live schema for physical-work acceptance tests."""

    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for M4 incrementality tests")
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    schema = f"groundloop_m4_incrementality_{uuid.uuid4().hex}"
    with psycopg.connect(psycopg_url, autocommit=True) as connection:
        try:
            connection.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            connection.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(schema)
                )
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
