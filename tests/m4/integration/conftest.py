from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import Connection, sql

from groundloop.postgres import apply_m2_schema, temporary_m2_schema


@pytest.fixture
def m4_pipeline_connection() -> Iterator[Connection[tuple[object, ...]]]:
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for the M4 pipeline tests")
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(psycopg_url, autocommit=True) as connection:
        with temporary_m2_schema(connection):
            try:
                yield connection
            finally:
                with connection.transaction():
                    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


@pytest.fixture
def m4_committed_pipeline_connection() -> Iterator[Connection[tuple[object, ...]]]:
    """Use real commits so out-of-transaction worker boundaries are testable."""
    url = os.environ.get("GROUNDLOOP_TEST_DATABASE_URL") or os.environ.get(
        "GROUNDLOOP_DATABASE_URL"
    )
    if not url:
        pytest.skip("live PostgreSQL is required for the M4 pipeline tests")
    psycopg_url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    schema = f"groundloop_m4_committed_{uuid.uuid4().hex}"
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
