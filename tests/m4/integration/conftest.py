from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import Connection

from groundloop.postgres import temporary_m2_schema


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
